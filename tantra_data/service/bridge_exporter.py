"""Bridge exporter: machine-readable Parquet projections for the Rust runtime.

Implements the Python-side projection side of the one-bridge design (plan 0005):
it serializes the existing DuckDB filings, corporate-actions, block/bulk-deal,
news, institutional-flow, delivery, economic-calendar, fundamentals,
mutual-fund and IPO stores into per-date Parquet files under
``{root}/catalog/filings/``, ``{root}/catalog/corporate_actions/``,
``{root}/catalog/block_deals/``, ``{root}/catalog/news/``,
``{root}/catalog/institutional_flows/``, ``{root}/catalog/delivery/``,
``{root}/catalog/economic_calendar/``, ``{root}/catalog/fundamentals/``,
``{root}/catalog/fundamental_ratios/``, ``{root}/catalog/mutual_funds/``,
``{root}/catalog/mf_nav/``, ``{root}/catalog/mf_returns/``,
``{root}/catalog/ipos/`` and ``{root}/catalog/ipo_subscriptions/``
that the Rust runtime (``tantra-runtime-api-rest``) can ingest with its
existing ``arrow``/``parquet`` deps, filtering
``filing_date <= as_of`` / ``ex_date <= as_of`` for point-in-time semantics.
Intraday option-chain snapshots (gap 0016) export per-expiry snapshot files,
a ``chain_as_of`` point-in-time projection, and a derived GEX series under
``{root}/option_chain/{expiry}/``.

Covers gaps 0007 (filings), 0008 (corporate actions), 0005 (block/bulk-deal
disclosure ingestion), 0006 (news ingestion), 0017/0018/0019 (institutional
flows / economic calendar / delivery bridges), 0020 (company fundamentals),
0021/0022 (mutual funds / IPO primary market) and 0015 (NSE trading holidays,
exported to ``catalog/trading_holidays/<year>.parquet`` for the
``tantra-runtime-backtest`` calendar consumer).
Values keep their exact shapes: prices/ratios are preserved as exact
``Decimal`` via ``Decimal128(18, 4)``; no floats are introduced anywhere in
the projection.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Protocol, TypedDict

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.adapters.cas_store import write_cas_object
from tantra_data.adapters.manifest_store import SCHEMA_VERSION, ManifestStore
from tantra_data.domain.block_deals import BlockDeal
from tantra_data.domain.calendar import CalendarEvent
from tantra_data.domain.corporate_actions import (
    CorporateAction,
    CorporateActionRecord,
    to_record,
)
from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.domain.filings import Filing, FilingType
from tantra_data.domain.fundamentals import (
    FinancialRatios,
    FinancialStatement,
    PeriodType,
    StatementType,
)
from tantra_data.domain.institutional_flow import InstitutionalFlow
from tantra_data.domain.ipos import IpoIssue, IpoSubscription
from tantra_data.domain.lakehouse import ManifestEntry
from tantra_data.domain.mutual_funds import MutualFundMeta, MutualFundNav, MutualFundReturn
from tantra_data.domain.news import NewsArticle
from tantra_data.domain.option_chain import (
    OptionChainSnapshot,
    StrikePositioning,
    compute_gamma_exposure,
)
from tantra_data.domain.shareholding import InsiderDisclosure, PledgeRecord, ShareholdingPattern
from tantra_data.domain.trading_holidays import TradingHoliday

__all__ = ["LakehouseBridgeExporter"]

#: v3 domain + primary date column per flat projection dataset (plan 0009 Phase B).
#: ``fundamentals`` and ``disclosures`` mirror ADR 0004 §2.2.
_V3_PROJECTIONS: dict[str, tuple[str, str]] = {
    "filings": ("fundamentals", "filing_date"),
    "corporate_actions": ("fundamentals", "ex_date"),
    "block_deals": ("disclosures", "trade_date"),
    "news": ("disclosures", "published_date"),
    "institutional_flows": ("disclosures", "trade_date"),
    "delivery": ("disclosures", "trade_date"),
    "economic_calendar": ("fundamentals", "scheduled_date"),
    "fundamentals": ("fundamentals", "filing_date"),
    "fundamental_ratios": ("fundamentals", "as_of_date"),
    "mutual_funds": ("fundamentals", "as_of_date"),
    "mf_nav": ("fundamentals", "nav_date"),
    "mf_returns": ("fundamentals", "as_of_date"),
    "ipos": ("disclosures", "as_of_date"),
    "ipo_subscriptions": ("disclosures", "as_of_date"),
}


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


class FinancialStatementsRepository(Protocol):
    """Structural interface for the financial-statement store consumed by the bridge."""

    def query(
        self,
        instrument: str | None = None,
        statement_type: StatementType | None = None,
        period_type: PeriodType | None = None,
        as_of: date | None = None,
        line_item: str | None = None,
    ) -> list[FinancialStatement]: ...

    def close(self) -> None: ...


class FundamentalRatiosRepository(Protocol):
    """Structural interface for the fundamental-ratios store consumed by the bridge."""

    def query(
        self,
        instrument: str | None = None,
        as_of: date | None = None,
    ) -> list[FinancialRatios]: ...

    def close(self) -> None: ...


class MutualFundsRepository(Protocol):
    """Structural interface for the mutual-fund scheme store consumed by the bridge."""

    def query(
        self,
        scheme_code: str | None = None,
        as_of: date | None = None,
        amc: str | None = None,
    ) -> list[MutualFundMeta]: ...

    def close(self) -> None: ...


class MutualFundNavRepository(Protocol):
    """Structural interface for the mutual-fund NAV store consumed by the bridge."""

    def query(
        self,
        scheme_code: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MutualFundNav]: ...

    def close(self) -> None: ...


class MutualFundReturnRepository(Protocol):
    """Structural interface for the mutual-fund return store consumed by the bridge."""

    def query(
        self,
        scheme_code: str | None = None,
        as_of: date | None = None,
    ) -> list[MutualFundReturn]: ...

    def close(self) -> None: ...


class IposRepository(Protocol):
    """Structural interface for the IPO issue store consumed by the bridge."""

    def query(
        self,
        symbol: str | None = None,
        status: str | None = None,
        as_of: date | None = None,
    ) -> list[IpoIssue]: ...

    def close(self) -> None: ...


class IpoSubscriptionsRepository(Protocol):
    """Structural interface for the IPO subscription store consumed by the bridge."""

    def query(
        self,
        symbol: str | None = None,
        as_of: date | None = None,
    ) -> list[IpoSubscription]: ...

    def close(self) -> None: ...


class OptionChainRepository(Protocol):
    """Structural interface for the option-chain store consumed by the bridge.

    ``snapshots_between`` is called with no arguments to enumerate every
    snapshot for the per-expiry Parquet projections (gap 0016).
    """

    def snapshots_between(
        self,
        underlying: str | None = None,
        expiry: date | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[OptionChainSnapshot]: ...

    def close(self) -> None: ...


class TradingHolidayRepository(Protocol):
    """Structural interface for the trading-holiday store consumed by the bridge.

    ``all_holidays`` is called with no arguments so the whole master can be
    projected into per-year Parquet files (gap 0015).
    """

    def all_holidays(self) -> list[TradingHoliday]: ...

    def close(self) -> None: ...


class ShareholdingRepository(Protocol):
    """Structural interface for the shareholding-pattern store consumed by the
    bridge (gap 0024). ``all_patterns`` is called with no arguments so the
    whole point-in-time series can be projected per filing date."""

    def all_patterns(self) -> list[ShareholdingPattern]: ...

    def close(self) -> None: ...


class PledgeRepository(Protocol):
    """Structural interface for the pledge store consumed by the bridge
    (gap 0024). ``all_pledges`` is called with no arguments so the whole
    snapshot series can be projected per quarter end."""

    def all_pledges(self) -> list[PledgeRecord]: ...

    def close(self) -> None: ...


class InsiderDisclosureRepository(Protocol):
    """Structural interface for the insider-disclosure store consumed by the
    bridge (gap 0024). ``all_disclosures`` is called with no arguments so the
    whole series can be projected per intimation date."""

    def all_disclosures(self) -> list[InsiderDisclosure]: ...

    def close(self) -> None: ...


class _GexSeriesRow(TypedDict):
    as_of: datetime
    expiry: date
    strike: Decimal
    gamma: Decimal
    open_interest: Decimal
    gex: Decimal
    net: Decimal
    flip: Decimal | None


class _ChainAsOfRow(TypedDict):
    as_of: datetime
    underlying: str
    expiry: date
    strike: Decimal
    option_type: str
    open_interest: Decimal
    oi_change: Decimal
    implied_volatility: Decimal
    delta: Decimal
    gamma: Decimal


def _as_decimal128(value: Decimal | None) -> Decimal | None:
    """Quantize a value to the 4-dp ``Decimal128(18, 4)`` projection scale.

    PyArrow refuses Decimals that would lose precision on rescale, so every
    money / ratio value is quantized exactly to paise precision first.
    """
    if value is None:
        return None
    return value.quantize(Decimal("0.0001"))


class LakehouseBridgeExporter:
    """Writes one Parquet projection file per date for the bridge datasets."""

    def __init__(
        self,
        filings: FilingsRepository | None = None,
        corporate_actions: CorporateActionsRepository | None = None,
        block_deals: BlockDealsRepository | None = None,
        news: NewsRepository | None = None,
        institutional_flows: InstitutionalFlowRepository | None = None,
        delivery: DeliveryRepository | None = None,
        calendar: CalendarEventsRepository | None = None,
        fundamentals: FinancialStatementsRepository | None = None,
        fundamental_ratios: FundamentalRatiosRepository | None = None,
        mutual_funds: MutualFundsRepository | None = None,
        mf_nav: MutualFundNavRepository | None = None,
        mf_returns: MutualFundReturnRepository | None = None,
        ipos: IposRepository | None = None,
        ipo_subscriptions: IpoSubscriptionsRepository | None = None,
        option_chain: OptionChainRepository | None = None,
        trading_holidays: TradingHolidayRepository | None = None,
        shareholding: ShareholdingRepository | None = None,
        pledge: PledgeRepository | None = None,
        insider: InsiderDisclosureRepository | None = None,
        manifest: ManifestStore | None = None,
    ) -> None:
        self._filings = filings
        self._corporate_actions = corporate_actions
        self._block_deals = block_deals
        self._news = news
        self._institutional_flows = institutional_flows
        self._delivery = delivery
        self._calendar = calendar
        self._fundamentals = fundamentals
        self._fundamental_ratios = fundamental_ratios
        self._mutual_funds = mutual_funds
        self._mf_nav = mf_nav
        self._mf_returns = mf_returns
        self._ipos = ipos
        self._ipo_subscriptions = ipo_subscriptions
        self._option_chain = option_chain
        self._trading_holidays = trading_holidays
        self._shareholding = shareholding
        self._pledge = pledge
        self._insider = insider
        self._manifest = manifest

    def _project_v3(self, root: Path, dataset: str) -> None:
        """Mirror a flat projection into its v3 CAS partition (manifest mode).

        When a ``ManifestStore`` is attached, the per-date files written by an
        ``export_*`` are also projected under ``{root}/{domain}/{dataset}/``
        (v3 partition, ADR 0004 §2.2), merged into one content-addressed
        object, and recorded as a single ``ManifestEntry`` committed to the
        catalog. Without a manifest this is a no-op so the legacy flat layout
        stays byte-identical.
        """
        if self._manifest is None:
            return
        mapping = _V3_PROJECTIONS.get(dataset)
        if mapping is None:
            return
        domain, sort_col = mapping
        flat_dir = root / dataset
        files = sorted(flat_dir.glob("*.parquet")) if flat_dir.exists() else []
        if not files:
            return

        v3_dir = root / domain / dataset
        v3_dir.mkdir(parents=True, exist_ok=True)
        tables: list[pa.Table] = []
        for path in files:
            table = pq.read_table(path)
            pq.write_table(table, v3_dir / path.name, version="2.6")
            tables.append(table)
        combined = pa.concat_tables(tables)

        min_ts, max_ts, as_of_ist = self._date_bounds(combined, sort_col)
        sha = write_cas_object(combined, sort_key=sort_col, root=self._manifest.root)
        entry = ManifestEntry(
            domain=domain,
            dataset=dataset,
            exchange=None,
            asset_class=None,
            timeframe=None,
            time_bucket=f"year={as_of_ist.year}",
            object_sha=sha,
            min_ts=min_ts,
            max_ts=max_ts,
            row_count=combined.num_rows,
            sort_key=sort_col,
        )
        parent = self._manifest.resolve_ref("main")
        self._manifest.commit(
            summary=f"bridge export {domain}/{dataset}",
            as_of_ist=as_of_ist,
            schema_version=SCHEMA_VERSION,
            entries=[entry],
            parent=parent,
        )

    @staticmethod
    def _date_bounds(table: pa.Table, column: str) -> tuple[datetime, datetime, date]:
        """Compute (min_ts, max_ts, as_of_ist) from a projection date column.

        ``date32`` cells become UTC midnights so the manifest bounds are
        timezone-aware; the latest date is the commit's ``as_of_ist``.
        """
        values = table.column(column).to_pylist()
        dates: list[date] = []
        for value in values:
            if isinstance(value, datetime):
                dates.append(value.date())
            elif isinstance(value, date):
                dates.append(value)
            else:
                raise TypeError(f"column {column!r} is not a date/timestamp column: {value!r}")
        lo, hi = min(dates), max(dates)
        min_ts = datetime.combine(lo, time.min, tzinfo=UTC)
        max_ts = datetime.combine(hi, time.max, tzinfo=UTC)
        return min_ts, max_ts, hi

    def export_filings(self, root: str | Path) -> int:
        """Write ``{root}/catalog/filings/<filing_date>.parquet`` per filing date.

        Returns the number of files written. Existing files are overwritten.
        """
        if self._filings is None:
            return 0
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
        self._project_v3(root, "filings")
        return count

    def export_corporate_actions(self, root: str | Path) -> int:
        """Write ``{root}/catalog/corporate_actions/<ex_date>.parquet`` per ex-date.

        Returns the number of files written. Existing files are overwritten.
        """
        if self._corporate_actions is None:
            return 0
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
        self._project_v3(root, "corporate_actions")
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
        self._project_v3(root, "block_deals")
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
        self._project_v3(root, "news")
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
        self._project_v3(root, "institutional_flows")
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
        self._project_v3(root, "delivery")
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
        self._project_v3(root, "economic_calendar")
        return count

    def export_trading_holidays(self, root: str | Path) -> int:
        """Write ``{root}/catalog/trading_holidays/<year>.parquet`` per year.

        Each file carries every declared NSE trading holiday / Muhurat session
        for that calendar year with exact ``date32`` dates — the projection
        ``tantra-runtime-backtest::NseTradingCalendar`` reads when it is built
        from the data root (gap 0015). Returns the number of files written;
        returns 0 when no trading-holiday store is configured.
        """
        if self._trading_holidays is None:
            return 0
        root = Path(root)
        by_year: dict[int, list[TradingHoliday]] = {}
        for holiday in self._trading_holidays.all_holidays():
            by_year.setdefault(holiday.date.year, []).append(holiday)

        count = 0
        for year, holidays in sorted(by_year.items()):
            ordered = sorted(holidays, key=lambda holiday: holiday.date)
            table = pa.Table.from_pydict(
                {
                    "holiday_date": pa.array([h.date for h in ordered], type=pa.date32()),
                    "description": pa.array([h.description for h in ordered], type=pa.string()),
                    "kind": pa.array([h.kind for h in ordered], type=pa.string()),
                }
            )
            path = root / "trading_holidays" / f"{year}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_shareholding(self, root: str | Path) -> int:
        """Write ``{root}/catalog/shareholding/<filing_date>.parquet`` per date.

        Shareholding-pattern rows are grouped by the date the filing became
        public (``filing_date``), preserving point-in-time semantics: a
        consumer filtering ``filing_date <= as_of`` sees exactly the pattern
        known at that date (no look-ahead). Percentages are exact
        ``Decimal128(18, 4)``. Returns the number of files written; returns 0
        when no shareholding store is configured.
        """
        if self._shareholding is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[ShareholdingPattern]] = {}
        for pattern in self._shareholding.all_patterns():
            by_date.setdefault(pattern.filing_date, []).append(pattern)

        count = 0
        for filing_date, patterns in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "filing_date": pa.array([p.filing_date for p in patterns], type=pa.date32()),
                    "instrument": pa.array([p.instrument for p in patterns], type=pa.string()),
                    "company_name": pa.array([p.company_name for p in patterns], type=pa.string()),
                    "exchange": pa.array([p.exchange for p in patterns], type=pa.string()),
                    "quarter_end": pa.array([p.quarter_end for p in patterns], type=pa.date32()),
                    "category": pa.array([p.category for p in patterns], type=pa.string()),
                    "no_of_shares": pa.array([p.no_of_shares for p in patterns], type=pa.int64()),
                    "shareholding_pct": pa.array(
                        [_as_decimal128(p.shareholding_pct) for p in patterns],
                        type=pa.decimal128(18, 4),
                    ),
                    "source": pa.array([p.source for p in patterns], type=pa.string()),
                }
            )
            path = root / "shareholding" / f"{filing_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_pledge(self, root: str | Path) -> int:
        """Write ``{root}/catalog/pledge/<quarter_end>.parquet`` per quarter.

        Promoter-pledge snapshots are grouped by the shareholding-pattern
        quarter end they refer to (``quarter_end``); each file carries the
        ``broadcast_date`` so a consumer can apply its own point-in-time
        filter (``broadcast_date <= as_of``). Percentages are exact
        ``Decimal128(18, 4)``. Returns the number of files written; returns 0
        when no pledge store is configured.
        """
        if self._pledge is None:
            return 0
        root = Path(root)
        by_quarter: dict[date, list[PledgeRecord]] = {}
        for record in self._pledge.all_pledges():
            by_quarter.setdefault(record.quarter_end, []).append(record)

        count = 0
        for quarter_end, records in sorted(by_quarter.items()):
            table = pa.Table.from_pydict(
                {
                    "quarter_end": pa.array([r.quarter_end for r in records], type=pa.date32()),
                    "instrument": pa.array([r.instrument for r in records], type=pa.string()),
                    "company_name": pa.array([r.company_name for r in records], type=pa.string()),
                    "broadcast_date": pa.array(
                        [r.broadcast_date for r in records], type=pa.date32()
                    ),
                    "promoter_holding_pct": pa.array(
                        [_as_decimal128(r.promoter_holding_pct) for r in records],
                        type=pa.decimal128(18, 4),
                    ),
                    "pledged_shares": pa.array(
                        [r.pledged_shares for r in records], type=pa.int64()
                    ),
                    "pledged_pct": pa.array(
                        [_as_decimal128(r.pledged_pct) for r in records],
                        type=pa.decimal128(18, 4),
                    ),
                    "promoter_shares": pa.array(
                        [r.promoter_shares for r in records], type=pa.int64()
                    ),
                    "total_issued_shares": pa.array(
                        [r.total_issued_shares for r in records], type=pa.int64()
                    ),
                    "source": pa.array([r.source for r in records], type=pa.string()),
                }
            )
            path = root / "pledge" / f"{quarter_end.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_insider_disclosures(self, root: str | Path) -> int:
        """Write ``{root}/catalog/insider_disclosures/<intimation_date>.parquet`` per date.

        Insider-trading disclosures are grouped by the date they were
        intimated to the exchange (``intimation_date``), preserving
        point-in-time semantics: a consumer filtering
        ``intimation_date <= as_of`` sees exactly the disclosures known at
        that date (no look-ahead). Values are exact ``Decimal128(18, 4)``.
        Returns the number of files written; returns 0 when no insider store
        is configured.
        """
        if self._insider is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[InsiderDisclosure]] = {}
        for disclosure in self._insider.all_disclosures():
            by_date.setdefault(disclosure.intimation_date, []).append(disclosure)

        count = 0
        for intimation_date, disclosures in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "intimation_date": pa.array(
                        [d.intimation_date for d in disclosures], type=pa.date32()
                    ),
                    "instrument": pa.array([d.instrument for d in disclosures], type=pa.string()),
                    "company_name": pa.array(
                        [d.company_name for d in disclosures], type=pa.string()
                    ),
                    "disclosure_id": pa.array(
                        [d.disclosure_id for d in disclosures], type=pa.int64()
                    ),
                    "trade_date": pa.array([d.trade_date for d in disclosures], type=pa.date32()),
                    "name": pa.array([d.name for d in disclosures], type=pa.string()),
                    "person_category": pa.array(
                        [d.person_category for d in disclosures], type=pa.string()
                    ),
                    "transaction_type": pa.array(
                        [d.transaction_type for d in disclosures], type=pa.string()
                    ),
                    "acq_mode": pa.array([d.acq_mode for d in disclosures], type=pa.string()),
                    "acquired_shares": pa.array(
                        [d.acquired_shares for d in disclosures], type=pa.int64()
                    ),
                    "buy_quantity": pa.array(
                        [d.buy_quantity for d in disclosures], type=pa.int64()
                    ),
                    "sell_quantity": pa.array(
                        [d.sell_quantity for d in disclosures], type=pa.int64()
                    ),
                    "value": pa.array(
                        [_as_decimal128(d.value) for d in disclosures],
                        type=pa.decimal128(18, 4),
                    ),
                    "before_shares": pa.array(
                        [d.before_shares for d in disclosures], type=pa.int64()
                    ),
                    "before_pct": pa.array(
                        [_as_decimal128(d.before_pct) for d in disclosures],
                        type=pa.decimal128(18, 4),
                    ),
                    "after_shares": pa.array(
                        [d.after_shares for d in disclosures], type=pa.int64()
                    ),
                    "after_pct": pa.array(
                        [_as_decimal128(d.after_pct) for d in disclosures],
                        type=pa.decimal128(18, 4),
                    ),
                    "source": pa.array([d.source for d in disclosures], type=pa.string()),
                }
            )
            path = root / "insider_disclosures" / f"{intimation_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_fundamentals(self, root: str | Path) -> int:
        """Write ``{root}/catalog/fundamentals/<filing_date>.parquet`` per filing date.

        Financial-statement line items are grouped by the date the disclosure
        became public (``filing_date``), preserving point-in-time semantics:
        a consumer filtering ``filing_date <= as_of`` sees exactly the line
        items that existed as of that date. Money values are exact
        ``Decimal128(18, 4)``. Returns the number of files written; returns 0
        when no fundamentals store is configured.
        """
        if self._fundamentals is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[FinancialStatement]] = {}
        for statement in self._fundamentals.query():
            by_date.setdefault(statement.filing_date, []).append(statement)

        count = 0
        for filing_date, statements in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "filing_date": pa.array([s.filing_date for s in statements], type=pa.date32()),
                    "instrument": pa.array([s.instrument for s in statements], type=pa.string()),
                    "statement_type": pa.array(
                        [s.statement_type.value for s in statements], type=pa.string()
                    ),
                    "period_type": pa.array(
                        [s.period_type.value for s in statements], type=pa.string()
                    ),
                    "period_end_date": pa.array(
                        [s.period_end_date for s in statements], type=pa.date32()
                    ),
                    "line_item": pa.array([s.line_item for s in statements], type=pa.string()),
                    "value": pa.array(
                        [_as_decimal128(s.value) for s in statements],
                        type=pa.decimal128(18, 4),
                    ),
                    "currency": pa.array([s.currency for s in statements], type=pa.string()),
                }
            )
            path = root / "fundamentals" / f"{filing_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        self._project_v3(root, "fundamentals")
        return count

    def export_fundamental_ratios(self, root: str | Path) -> int:
        """Write ``{root}/catalog/fundamental_ratios/<as_of_date>.parquet`` per date.

        Point-in-time ratio snapshots are grouped by ``as_of_date``. Every
        ratio is an exact ``Decimal128(18, 4)`` (``None`` when the vendor did
        not report it). Returns the number of files written; returns 0 when no
        ratios store is configured.
        """
        if self._fundamental_ratios is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[FinancialRatios]] = {}
        for ratios in self._fundamental_ratios.query():
            by_date.setdefault(ratios.as_of_date, []).append(ratios)

        count = 0
        for as_of_date, ratios_list in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "as_of_date": pa.array([r.as_of_date for r in ratios_list], type=pa.date32()),
                    "instrument": pa.array([r.instrument for r in ratios_list], type=pa.string()),
                    "pe": pa.array(
                        [_as_decimal128(r.pe) for r in ratios_list], type=pa.decimal128(18, 4)
                    ),
                    "pb": pa.array(
                        [_as_decimal128(r.pb) for r in ratios_list], type=pa.decimal128(18, 4)
                    ),
                    "ev_to_ebitda": pa.array(
                        [_as_decimal128(r.ev_to_ebitda) for r in ratios_list],
                        type=pa.decimal128(18, 4),
                    ),
                    "dividend_yield": pa.array(
                        [_as_decimal128(r.dividend_yield) for r in ratios_list],
                        type=pa.decimal128(18, 4),
                    ),
                    "roce": pa.array(
                        [_as_decimal128(r.roce) for r in ratios_list], type=pa.decimal128(18, 4)
                    ),
                    "roe": pa.array(
                        [_as_decimal128(r.roe) for r in ratios_list], type=pa.decimal128(18, 4)
                    ),
                    "debt_to_equity": pa.array(
                        [_as_decimal128(r.debt_to_equity) for r in ratios_list],
                        type=pa.decimal128(18, 4),
                    ),
                    "interest_coverage": pa.array(
                        [_as_decimal128(r.interest_coverage) for r in ratios_list],
                        type=pa.decimal128(18, 4),
                    ),
                    "source": pa.array([r.source for r in ratios_list], type=pa.string()),
                }
            )
            path = root / "fundamental_ratios" / f"{as_of_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        self._project_v3(root, "fundamental_ratios")
        return count

    def export_mutual_funds(self, root: str | Path) -> int:
        """Write ``{root}/catalog/mutual_funds/<as_of_date>.parquet`` per date.

        Scheme metadata is grouped by the NAV-dump date it was observed in
        (``as_of_date``), preserving point-in-time semantics. Returns the
        number of files written; returns 0 when no mutual-fund store is
        configured.
        """
        if self._mutual_funds is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[MutualFundMeta]] = {}
        for meta in self._mutual_funds.query():
            by_date.setdefault(meta.as_of_date, []).append(meta)

        count = 0
        for as_of_date, metas in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "as_of_date": pa.array([m.as_of_date for m in metas], type=pa.date32()),
                    "scheme_code": pa.array([m.scheme_code for m in metas], type=pa.string()),
                    "scheme_name": pa.array([m.scheme_name for m in metas], type=pa.string()),
                    "amc": pa.array([m.amc for m in metas], type=pa.string()),
                    "category": pa.array([m.category for m in metas], type=pa.string()),
                    "isin_growth": pa.array([m.isin_growth for m in metas], type=pa.string()),
                    "isin_div_reinvestment": pa.array(
                        [m.isin_div_reinvestment for m in metas], type=pa.string()
                    ),
                    "plan": pa.array([m.plan for m in metas], type=pa.string()),
                    "option": pa.array([m.option for m in metas], type=pa.string()),
                    "source": pa.array([m.source for m in metas], type=pa.string()),
                }
            )
            path = root / "mutual_funds" / f"{as_of_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        self._project_v3(root, "mutual_funds")
        return count

    def export_mf_nav(self, root: str | Path) -> int:
        """Write ``{root}/catalog/mf_nav/<nav_date>.parquet`` per NAV date.

        Per-scheme NAV observations are grouped by their disclosure date
        (``nav_date``). NAV / repurchase / sale prices are exact
        ``Decimal128(18, 4)``. Returns the number of files written; returns 0
        when no NAV store is configured.
        """
        if self._mf_nav is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[MutualFundNav]] = {}
        for nav in self._mf_nav.query():
            by_date.setdefault(nav.nav_date, []).append(nav)

        count = 0
        for nav_date, navs in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "nav_date": pa.array([n.nav_date for n in navs], type=pa.date32()),
                    "scheme_code": pa.array([n.scheme_code for n in navs], type=pa.string()),
                    "nav": pa.array(
                        [_as_decimal128(n.nav) for n in navs], type=pa.decimal128(18, 4)
                    ),
                    "repurchase_price": pa.array(
                        [_as_decimal128(n.repurchase_price) for n in navs],
                        type=pa.decimal128(18, 4),
                    ),
                    "sale_price": pa.array(
                        [_as_decimal128(n.sale_price) for n in navs],
                        type=pa.decimal128(18, 4),
                    ),
                    "source": pa.array([n.source for n in navs], type=pa.string()),
                }
            )
            path = root / "mf_nav" / f"{nav_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        self._project_v3(root, "mf_nav")
        return count

    def export_mf_returns(self, root: str | Path) -> int:
        """Write ``{root}/catalog/mf_returns/<as_of_date>.parquet`` per date.

        Derived return metrics are grouped by ``as_of_date``. CAGR / expense
        ratio are exact ``Decimal128(18, 4)`` (``None`` when not derivable or
        not supplied by the vendor). Returns the number of files written;
        returns 0 when no return store is configured.
        """
        if self._mf_returns is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[MutualFundReturn]] = {}
        for returns in self._mf_returns.query():
            by_date.setdefault(returns.as_of_date, []).append(returns)

        count = 0
        for as_of_date, records in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "as_of_date": pa.array([r.as_of_date for r in records], type=pa.date32()),
                    "scheme_code": pa.array([r.scheme_code for r in records], type=pa.string()),
                    "cagr_1y": pa.array(
                        [_as_decimal128(r.cagr_1y) for r in records],
                        type=pa.decimal128(18, 4),
                    ),
                    "expense_ratio": pa.array(
                        [_as_decimal128(r.expense_ratio) for r in records],
                        type=pa.decimal128(18, 4),
                    ),
                    "source": pa.array([r.source for r in records], type=pa.string()),
                }
            )
            path = root / "mf_returns" / f"{as_of_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        self._project_v3(root, "mf_returns")
        return count

    def export_ipos(self, root: str | Path) -> int:
        """Write ``{root}/catalog/ipos/<as_of_date>.parquet`` per snapshot date.

        IPO issue metadata is grouped by the date it was observed
        (``as_of_date``), preserving point-in-time semantics. Price bands, lot
        sizes, and issue sizes are exact ``Decimal128(18, 4)``. Returns the
        number of files written; returns 0 when no IPO store is configured.
        """
        if self._ipos is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[IpoIssue]] = {}
        for issue in self._ipos.query():
            by_date.setdefault(issue.as_of_date, []).append(issue)

        count = 0
        for as_of_date, issues in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "as_of_date": pa.array([i.as_of_date for i in issues], type=pa.date32()),
                    "symbol": pa.array([i.symbol for i in issues], type=pa.string()),
                    "company_name": pa.array([i.company_name for i in issues], type=pa.string()),
                    "board": pa.array([i.board for i in issues], type=pa.string()),
                    "status": pa.array([i.status for i in issues], type=pa.string()),
                    "open_date": pa.array([i.open_date for i in issues], type=pa.date32()),
                    "close_date": pa.array([i.close_date for i in issues], type=pa.date32()),
                    "listing_date": pa.array([i.listing_date for i in issues], type=pa.date32()),
                    "price_band_low": pa.array(
                        [_as_decimal128(i.price_band_low) for i in issues],
                        type=pa.decimal128(18, 4),
                    ),
                    "price_band_high": pa.array(
                        [_as_decimal128(i.price_band_high) for i in issues],
                        type=pa.decimal128(18, 4),
                    ),
                    "issue_price": pa.array(
                        [_as_decimal128(i.issue_price) for i in issues],
                        type=pa.decimal128(18, 4),
                    ),
                    "lot_size": pa.array(
                        [_as_decimal128(i.lot_size) for i in issues],
                        type=pa.decimal128(18, 4),
                    ),
                    "issue_size_cr": pa.array(
                        [_as_decimal128(i.issue_size_cr) for i in issues],
                        type=pa.decimal128(18, 4),
                    ),
                    "source": pa.array([i.source for i in issues], type=pa.string()),
                }
            )
            path = root / "ipos" / f"{as_of_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        self._project_v3(root, "ipos")
        return count

    def export_ipo_subscriptions(self, root: str | Path) -> int:
        """Write ``{root}/catalog/ipo_subscriptions/<as_of_date>.parquet`` per date.

        Subscription multiples are grouped by snapshot date (``as_of_date``).
        Multiples are exact ``Decimal128(18, 4)`` (``None`` when the exchange
        had not reported the category). Returns the number of files written;
        returns 0 when no subscription store is configured.
        """
        if self._ipo_subscriptions is None:
            return 0
        root = Path(root)
        by_date: dict[date, list[IpoSubscription]] = {}
        for subscription in self._ipo_subscriptions.query():
            by_date.setdefault(subscription.as_of_date, []).append(subscription)

        count = 0
        for as_of_date, subscriptions in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "as_of_date": pa.array([s.as_of_date for s in subscriptions], type=pa.date32()),
                    "symbol": pa.array([s.symbol for s in subscriptions], type=pa.string()),
                    "qib": pa.array(
                        [_as_decimal128(s.qib) for s in subscriptions],
                        type=pa.decimal128(18, 4),
                    ),
                    "nii": pa.array(
                        [_as_decimal128(s.nii) for s in subscriptions],
                        type=pa.decimal128(18, 4),
                    ),
                    "bnii": pa.array(
                        [_as_decimal128(s.bnii) for s in subscriptions],
                        type=pa.decimal128(18, 4),
                    ),
                    "snii": pa.array(
                        [_as_decimal128(s.snii) for s in subscriptions],
                        type=pa.decimal128(18, 4),
                    ),
                    "rii": pa.array(
                        [_as_decimal128(s.rii) for s in subscriptions],
                        type=pa.decimal128(18, 4),
                    ),
                    "total": pa.array(
                        [_as_decimal128(s.total) for s in subscriptions],
                        type=pa.decimal128(18, 4),
                    ),
                    "source": pa.array([s.source for s in subscriptions], type=pa.string()),
                }
            )
            path = root / "ipo_subscriptions" / f"{as_of_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        self._project_v3(root, "ipo_subscriptions")
        return count

    def export_option_chain(self, root: str | Path) -> int:
        """Write intraday option-chain snapshots under ``{root}/option_chain/``.

        Per expiry, one ``YYYY-MM-DD-HHMM.parquet`` file per snapshot (keyed by
        the UTC ``as_of``), plus a ``chain_as_of.parquet`` point-in-time
        projection whose rows carry, for every ``as_of``, the latest-known
        value per ``(strike, option_type)`` — exactly the rows
        ``chain_as_of`` returns at that timestamp (no look-ahead). All
        financial columns are exact ``Decimal128(18, 4)``. Returns the number
        of snapshot files written; returns 0 when no option-chain store is
        configured.
        """
        if self._option_chain is None:
            return 0
        root = Path(root)
        by_expiry, underlyings = self._option_chain_snapshots()

        count = 0
        for expiry in sorted(by_expiry):
            by_as_of = by_expiry[expiry]
            underlying = underlyings[expiry]
            for as_of in sorted(by_as_of):
                strikes = by_as_of[as_of]
                table = self._option_chain_table(as_of, underlying, expiry, strikes)
                path = root / "option_chain" / expiry.isoformat() / f"{as_of:%Y-%m-%d-%H%M}.parquet"
                path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(table, path, version="2.6")
                count += 1

            projection = self._chain_as_of_projection(by_as_of, underlying, expiry)
            proj_path = root / "option_chain" / expiry.isoformat() / "chain_as_of.parquet"
            proj_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(projection, proj_path, version="2.6")
        return count

    def export_option_chain_gex(
        self,
        root: str | Path,
        lot_size: Decimal,
        spot: Decimal,
    ) -> int:
        """Write ``{root}/option_chain/{expiry}/gex_series.parquet`` per expiry.

        The derived GEX series has one row per ``(as_of, strike)`` with the
        per-strike GEX (``gamma * open_interest * lot_size * spot``, exact
        ``Decimal``) plus the per-snapshot ``net`` and ``flip`` computed via
        :func:`~tantra_data.domain.option_chain.compute_gamma_exposure`.
        Returns the number of expiry series files written; returns 0 when no
        option-chain store is configured.
        """
        if self._option_chain is None:
            return 0
        root = Path(root)
        by_expiry, underlyings = self._option_chain_snapshots()

        count = 0
        for expiry in sorted(by_expiry):
            rows: list[_GexSeriesRow] = []
            for as_of in sorted(by_expiry[expiry]):
                snapshot = OptionChainSnapshot(
                    as_of=as_of,
                    underlying=underlyings[expiry],
                    expiry=expiry,
                    strikes=by_expiry[expiry][as_of],
                )
                exposure = compute_gamma_exposure(snapshot, lot_size=lot_size, spot=spot)
                for per_strike in exposure.per_strike:
                    rows.append(
                        {
                            "as_of": as_of,
                            "expiry": expiry,
                            "strike": per_strike.strike,
                            "gamma": per_strike.gamma,
                            "open_interest": per_strike.open_interest,
                            "gex": per_strike.gex,
                            "net": exposure.net,
                            "flip": exposure.flip,
                        }
                    )
            table = pa.Table.from_pydict(
                {
                    "as_of": pa.array(
                        [row["as_of"] for row in rows], type=pa.timestamp("us", tz="UTC")
                    ),
                    "expiry": pa.array([row["expiry"] for row in rows], type=pa.date32()),
                    "strike": pa.array(
                        [_as_decimal128(row["strike"]) for row in rows], type=pa.decimal128(18, 4)
                    ),
                    "gamma": pa.array(
                        [_as_decimal128(row["gamma"]) for row in rows], type=pa.decimal128(18, 4)
                    ),
                    "open_interest": pa.array(
                        [_as_decimal128(row["open_interest"]) for row in rows],
                        type=pa.decimal128(18, 4),
                    ),
                    "gex": pa.array(
                        [_as_decimal128(row["gex"]) for row in rows], type=pa.decimal128(18, 4)
                    ),
                    "net": pa.array(
                        [_as_decimal128(row["net"]) for row in rows], type=pa.decimal128(18, 4)
                    ),
                    "flip": pa.array(
                        [_as_decimal128(row["flip"]) for row in rows], type=pa.decimal128(18, 4)
                    ),
                }
            )
            path = root / "option_chain" / expiry.isoformat() / "gex_series.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def _option_chain_snapshots(
        self,
    ) -> tuple[
        dict[date, dict[datetime, list[StrikePositioning]]],
        dict[date, str],
    ]:
        """Group every stored snapshot by ``(expiry, as_of)`` and record the
        underlying symbol per expiry."""
        by_expiry: dict[date, dict[datetime, list[StrikePositioning]]] = {}
        underlyings: dict[date, str] = {}
        for snapshot in self._option_chain.snapshots_between():  # type: ignore[union-attr]
            by_expiry.setdefault(snapshot.expiry, {}).setdefault(snapshot.as_of, []).extend(
                snapshot.strikes
            )
            underlyings.setdefault(snapshot.expiry, snapshot.underlying)
        return by_expiry, underlyings

    @staticmethod
    def _option_chain_table(
        as_of: datetime,
        underlying: str,
        expiry: date,
        strikes: list[StrikePositioning],
    ) -> pa.Table:
        """Build the per-snapshot Arrow table (exact ``Decimal128(18, 4)``)."""
        return pa.Table.from_pydict(
            {
                "as_of": pa.array([as_of] * len(strikes), type=pa.timestamp("us", tz="UTC")),
                "underlying": pa.array([underlying] * len(strikes), type=pa.string()),
                "expiry": pa.array([expiry] * len(strikes), type=pa.date32()),
                "strike": pa.array(
                    [_as_decimal128(s.strike) for s in strikes], type=pa.decimal128(18, 4)
                ),
                "option_type": pa.array([s.option_type for s in strikes], type=pa.string()),
                "open_interest": pa.array(
                    [_as_decimal128(s.open_interest) for s in strikes],
                    type=pa.decimal128(18, 4),
                ),
                "oi_change": pa.array(
                    [_as_decimal128(s.oi_change) for s in strikes], type=pa.decimal128(18, 4)
                ),
                "implied_volatility": pa.array(
                    [_as_decimal128(s.implied_volatility) for s in strikes],
                    type=pa.decimal128(18, 4),
                ),
                "delta": pa.array(
                    [_as_decimal128(s.delta) for s in strikes], type=pa.decimal128(18, 4)
                ),
                "gamma": pa.array(
                    [_as_decimal128(s.gamma) for s in strikes], type=pa.decimal128(18, 4)
                ),
            }
        )

    @staticmethod
    def _chain_as_of_projection(
        by_as_of: dict[datetime, list[StrikePositioning]],
        underlying: str,
        expiry: date,
    ) -> pa.Table:
        """Build the point-in-time projection table for one expiry.

        At every ``as_of`` boundary the projection carries the latest-known
        value per ``(strike, option_type)`` — the same rows a ``chain_as_of``
        read at that timestamp would return, with no look-ahead.
        """
        latest: dict[tuple[Decimal, str], StrikePositioning] = {}
        rows: list[_ChainAsOfRow] = []
        for as_of in sorted(by_as_of):
            for positioning in by_as_of[as_of]:
                latest[(positioning.strike, positioning.option_type)] = positioning
            for positioning in sorted(latest.values(), key=lambda s: (s.strike, s.option_type)):
                rows.append(
                    {
                        "as_of": as_of,
                        "underlying": underlying,
                        "expiry": expiry,
                        "strike": positioning.strike,
                        "option_type": positioning.option_type,
                        "open_interest": positioning.open_interest,
                        "oi_change": positioning.oi_change,
                        "implied_volatility": positioning.implied_volatility,
                        "delta": positioning.delta,
                        "gamma": positioning.gamma,
                    }
                )
        return pa.Table.from_pydict(
            {
                "as_of": pa.array(
                    [row["as_of"] for row in rows], type=pa.timestamp("us", tz="UTC")
                ),
                "underlying": pa.array([row["underlying"] for row in rows], type=pa.string()),
                "expiry": pa.array([row["expiry"] for row in rows], type=pa.date32()),
                "strike": pa.array(
                    [_as_decimal128(row["strike"]) for row in rows], type=pa.decimal128(18, 4)
                ),
                "option_type": pa.array([row["option_type"] for row in rows], type=pa.string()),
                "open_interest": pa.array(
                    [_as_decimal128(row["open_interest"]) for row in rows],
                    type=pa.decimal128(18, 4),
                ),
                "oi_change": pa.array(
                    [_as_decimal128(row["oi_change"]) for row in rows], type=pa.decimal128(18, 4)
                ),
                "implied_volatility": pa.array(
                    [_as_decimal128(row["implied_volatility"]) for row in rows],
                    type=pa.decimal128(18, 4),
                ),
                "delta": pa.array(
                    [_as_decimal128(row["delta"]) for row in rows], type=pa.decimal128(18, 4)
                ),
                "gamma": pa.array(
                    [_as_decimal128(row["gamma"]) for row in rows], type=pa.decimal128(18, 4)
                ),
            }
        )
