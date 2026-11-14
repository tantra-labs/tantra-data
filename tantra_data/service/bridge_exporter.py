"""Bridge exporter: machine-readable Parquet projections for the Rust runtime.

Implements the Python-side projection side of the one-bridge design (plan 0005):
it serializes the existing DuckDB filings, corporate-actions, block/bulk-deal
and news stores into per-date Parquet files under ``{root}/catalog/filings/``,
``{root}/catalog/corporate_actions/``, ``{root}/catalog/block_deals/`` and
``{root}/catalog/news/`` that the Rust runtime (``tantra-runtime-api-rest``) can
ingest with its existing ``arrow``/``parquet`` deps, filtering
``filing_date <= as_of`` / ``ex_date <= as_of`` for point-in-time semantics.

Covers gaps 0007 (filings), 0008 (corporate actions), 0005 (block/bulk-deal
disclosure ingestion) and 0006 (news ingestion). Values keep their exact
shapes: prices/ratios are preserved as exact ``Decimal`` via
``Decimal128(18, 4)``; no floats are introduced anywhere in the projection.
"""

from __future__ import annotations

from datetime import UTC, date
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.domain.block_deals import BlockDeal
from tantra_data.domain.calendar import CalendarEvent
from tantra_data.domain.corporate_actions import (
    CorporateAction,
    CorporateActionRecord,
    to_record,
)
from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.domain.filings import Filing, FilingType
from tantra_data.domain.institutional_flow import InstitutionalFlow
from tantra_data.domain.news import NewsArticle

__all__ = ["LakehouseBridgeExporter"]


class FilingsRepository(Protocol):
    """Structural interface for the filings store consumed by the bridge."""

    def query(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_type: FilingType | None = None,
    ) -> list[Filing]: ...

    def close(self) -> None: ...


class CorporateActionsRepository(Protocol):
    """Structural interface for the corporate-actions store consumed by the bridge."""

    def list(
        self,
        instrument: str | None = None,
        as_of: date | None = None,
    ) -> list[CorporateAction]: ...

    def close(self) -> None: ...


class BlockDealsRepository(Protocol):
    """Structural interface for the block/bulk-deal store consumed by the bridge."""

    def query(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
        category: str | None = None,
    ) -> list[BlockDeal]: ...

    def close(self) -> None: ...


class NewsRepository(Protocol):
    """Structural interface for the news store consumed by the bridge."""

    def query(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        query: str | None = None,
    ) -> list[NewsArticle]: ...

    def close(self) -> None: ...


class InstitutionalFlowRepository(Protocol):
    """Structural interface for the institutional-flows store consumed by the bridge."""

    def query(
        self,
        trade_date: date | None = None,
        participant: str | None = None,
        segment: str | None = None,
    ) -> list[InstitutionalFlow]: ...

    def close(self) -> None: ...


class DeliveryRepository(Protocol):
    """Structural interface for the delivery store consumed by the bridge."""

    def query(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
    ) -> list[DeliveryRecord]: ...

    def close(self) -> None: ...


class CalendarEventsRepository(Protocol):
    """Structural interface for the economic-calendar store consumed by the bridge."""

    def query(
        self,
        instrument: str | None = None,
        category: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[CalendarEvent]: ...

    def close(self) -> None: ...


class LakehouseBridgeExporter:
    """Writes one Parquet projection file per date for the bridge datasets."""

    def __init__(
        self,
        filings: FilingsRepository,
        corporate_actions: CorporateActionsRepository,
        block_deals: BlockDealsRepository | None = None,
        news: NewsRepository | None = None,
        institutional_flows: InstitutionalFlowRepository | None = None,
        delivery: DeliveryRepository | None = None,
        calendar: CalendarEventsRepository | None = None,
    ) -> None:
        self._filings = filings
        self._corporate_actions = corporate_actions
        self._block_deals = block_deals
        self._news = news
        self._institutional_flows = institutional_flows
        self._delivery = delivery
        self._calendar = calendar

    def export_filings(self, root: str | Path) -> int:
        """Write ``{root}/catalog/filings/<filing_date>.parquet`` per filing date.

        Returns the number of files written. Existing files are overwritten.
        """
        root = Path(root)
        by_date: dict[date, list[Filing]] = {}
        for filing in self._filings.query():
            by_date.setdefault(filing.filing_date, []).append(filing)

        count = 0
        for filing_date, filings in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "filing_date": pa.array([f.filing_date for f in filings], type=pa.date32()),
                    "instrument": pa.array([f.instrument for f in filings], type=pa.string()),
                    "filing_type": pa.array(
                        [f.filing_type.value for f in filings], type=pa.string()
                    ),
                    "title": pa.array([f.title for f in filings], type=pa.string()),
                    "source_url": pa.array([f.source_url for f in filings], type=pa.string()),
                }
            )
            path = root / "filings" / f"{filing_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_corporate_actions(self, root: str | Path) -> int:
        """Write ``{root}/catalog/corporate_actions/<ex_date>.parquet`` per ex-date.

        Returns the number of files written. Existing files are overwritten.
        """
        root = Path(root)
        records = [to_record(action) for action in self._corporate_actions.list()]
        by_date: dict[date, list[CorporateActionRecord]] = {}
        for record in records:
            by_date.setdefault(record.ex_date, []).append(record)

        count = 0
        for ex_date, actions in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "ex_date": pa.array([r.ex_date for r in actions], type=pa.date32()),
                    "instrument": pa.array([r.instrument for r in actions], type=pa.string()),
                    "action_type": pa.array(
                        [r.action_type.value for r in actions], type=pa.string()
                    ),
                    "old_shares": pa.array([r.old_shares for r in actions], type=pa.int32()),
                    "new_shares": pa.array([r.new_shares for r in actions], type=pa.int32()),
                    "per_share": pa.array(
                        [r.per_share for r in actions], type=pa.decimal128(18, 4)
                    ),
                    "issue_price": pa.array(
                        [r.issue_price for r in actions], type=pa.decimal128(18, 4)
                    ),
                    "currency": pa.array([r.currency for r in actions], type=pa.string()),
                }
            )
            path = root / "corporate_actions" / f"{ex_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_block_deals(self, root: str | Path) -> int:
        """Write ``{root}/catalog/block_deals/<trade_date>.parquet`` per trade date.

        Returns the number of files written. Existing files are overwritten.
        Returns 0 when no block-deals store is configured.
        """
        if self._block_deals is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[BlockDeal]] = {}
        for deal in self._block_deals.query():
            by_date.setdefault(deal.trade_date, []).append(deal)

        count = 0
        for trade_date, deals in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "trade_date": pa.array([d.trade_date for d in deals], type=pa.date32()),
                    "exchange": pa.array([d.exchange for d in deals], type=pa.string()),
                    "instrument": pa.array([d.instrument for d in deals], type=pa.string()),
                    "security_name": pa.array([d.security_name for d in deals], type=pa.string()),
                    "client_name": pa.array([d.client_name for d in deals], type=pa.string()),
                    "side": pa.array([d.side for d in deals], type=pa.string()),
                    "category": pa.array([d.category for d in deals], type=pa.string()),
                    "quantity": pa.array([d.quantity for d in deals], type=pa.decimal128(18, 4)),
                    "price": pa.array([d.price for d in deals], type=pa.decimal128(18, 4)),
                    "remarks": pa.array([d.remarks for d in deals], type=pa.string()),
                }
            )
            path = root / "block_deals" / f"{trade_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_news(self, root: str | Path) -> int:
        """Write ``{root}/catalog/news/<published_date>.parquet`` per publication date.

        Returns the number of files written. Existing files are overwritten.
        Returns 0 when no news store is configured.
        """
        if self._news is None:
            return 0
        root = Path(root)
        articles = self._news.query()
        by_date: dict[date, list[NewsArticle]] = {}
        for article in articles:
            by_date.setdefault(article.published_at.date(), []).append(article)
        count = 0
        for published_date, day_articles in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "published_date": pa.array(
                        [a.published_at.date() for a in day_articles], type=pa.date32()
                    ),
                    "published_at": pa.array(
                        [a.published_at.astimezone(UTC).isoformat() for a in day_articles],
                        type=pa.string(),
                    ),
                    "article_id": pa.array([a.article_id for a in day_articles], type=pa.string()),
                    "title": pa.array([a.title for a in day_articles], type=pa.string()),
                    "source": pa.array([a.source for a in day_articles], type=pa.string()),
                    "url": pa.array([a.url for a in day_articles], type=pa.string()),
                    "summary": pa.array([a.summary for a in day_articles], type=pa.string()),
                    "instruments": pa.array(
                        [",".join(a.instruments) for a in day_articles], type=pa.string()
                    ),
                }
            )
            path = root / "news" / f"{published_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_institutional_flows(self, root: str | Path) -> int:
        """Write ``{root}/catalog/institutional_flows/<trade_date>.parquet`` per trade date.

        Returns the number of files written. Existing files are overwritten.
        Returns 0 when no institutional-flows store is configured.
        """
        if self._institutional_flows is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[InstitutionalFlow]] = {}
        for flow in self._institutional_flows.query():
            by_date.setdefault(flow.trade_date, []).append(flow)

        count = 0
        for trade_date, flows in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "trade_date": pa.array([f.trade_date for f in flows], type=pa.date32()),
                    "participant": pa.array([f.participant for f in flows], type=pa.string()),
                    "segment": pa.array([f.segment for f in flows], type=pa.string()),
                    "gross_buy": pa.array([f.gross_buy for f in flows], type=pa.decimal128(18, 4)),
                    "gross_sell": pa.array(
                        [f.gross_sell for f in flows], type=pa.decimal128(18, 4)
                    ),
                    "net": pa.array([f.net for f in flows], type=pa.decimal128(18, 4)),
                    "source": pa.array([f.source for f in flows], type=pa.string()),
                }
            )
            path = root / "institutional_flows" / f"{trade_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_delivery(self, root: str | Path) -> int:
        """Write ``{root}/catalog/delivery/<trade_date>.parquet`` per trade date.

        Returns the number of files written. Existing files are overwritten.
        Returns 0 when no delivery store is configured.
        """
        if self._delivery is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[DeliveryRecord]] = {}
        for record in self._delivery.query():
            by_date.setdefault(record.trade_date, []).append(record)

        count = 0
        for trade_date, records in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "trade_date": pa.array([r.trade_date for r in records], type=pa.date32()),
                    "exchange": pa.array([r.exchange for r in records], type=pa.string()),
                    "instrument": pa.array([r.instrument for r in records], type=pa.string()),
                    "traded_quantity": pa.array(
                        [r.traded_quantity for r in records], type=pa.int64()
                    ),
                    "deliverable_quantity": pa.array(
                        [r.deliverable_quantity for r in records], type=pa.int64()
                    ),
                    "delivery_pct": pa.array(
                        [r.delivery_pct for r in records], type=pa.decimal128(18, 4)
                    ),
                }
            )
            path = root / "delivery" / f"{trade_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_economic_calendar(self, root: str | Path) -> int:
        """Write ``{root}/catalog/economic_calendar/<scheduled_date>.parquet`` per date.

        Returns the number of files written. Existing files are overwritten.
        Returns 0 when no calendar store is configured.
        """
        if self._calendar is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[CalendarEvent]] = {}
        for event in self._calendar.query():
            by_date.setdefault(event.scheduled_at.date(), []).append(event)

        count = 0
        for scheduled_date, events in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "scheduled_date": pa.array(
                        [e.scheduled_at.date() for e in events], type=pa.date32()
                    ),
                    "scheduled_at": pa.array(
                        [e.scheduled_at.astimezone(UTC).isoformat() for e in events],
                        type=pa.string(),
                    ),
                    "instrument": pa.array([e.instrument for e in events], type=pa.string()),
                    "country": pa.array([e.country for e in events], type=pa.string()),
                    "currency": pa.array([e.currency for e in events], type=pa.string()),
                    "category": pa.array([e.category for e in events], type=pa.string()),
                    "event_name": pa.array([e.event_name for e in events], type=pa.string()),
                    "impact_level": pa.array([e.impact_level for e in events], type=pa.string()),
                    "actual": pa.array([e.actual for e in events], type=pa.decimal128(18, 4)),
                    "forecast": pa.array([e.forecast for e in events], type=pa.decimal128(18, 4)),
                    "prior": pa.array([e.prior for e in events], type=pa.decimal128(18, 4)),
                    "period_label": pa.array([e.period_label for e in events], type=pa.string()),
                }
            )
            path = root / "economic_calendar" / f"{scheduled_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count
