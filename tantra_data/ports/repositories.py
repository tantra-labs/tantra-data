"""Repository and service protocols (ports).

The service layer depends on these protocols, never on concrete storage
classes, so DuckDB, Parquet, or future providers are swappable without touching
domain or service code.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

import polars as pl

from tantra_data.domain.block_deals import BlockDeal
from tantra_data.domain.calendar import CalendarEvent
from tantra_data.domain.corporate_actions import CorporateAction
from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.domain.filings import Filing, FilingType
from tantra_data.domain.fundamentals import (
    FinancialRatios,
    FinancialStatement,
    FundamentalRecord,
    PeriodType,
    StatementType,
)
from tantra_data.domain.institutional_flow import InstitutionalFlow
from tantra_data.domain.ipos import IpoIssue, IpoSubscription
from tantra_data.domain.lakehouse import CommitRecord
from tantra_data.domain.macro import MacroObservation
from tantra_data.domain.mutual_funds import MutualFundMeta, MutualFundNav, MutualFundReturn
from tantra_data.domain.news import NewsArticle
from tantra_data.domain.profile import SymbolProfile

# Canonical bar schema for the Parquet catalog. All financial columns are
# fixed-point decimals; ``ts`` is microsecond-resolution UTC.
BAR_SCHEMA: dict[str, pl.DataType] = {
    "ts": pl.Datetime("us"),
    "open": pl.Decimal(18, 4),
    "high": pl.Decimal(18, 4),
    "low": pl.Decimal(18, 4),
    "close": pl.Decimal(18, 4),
    "volume": pl.Int64(),
}


@runtime_checkable
class CorporateActionsRepository(Protocol):
    """Storage for corporate action records."""

    def list(
        self, instrument: str | None = None, as_of: date | None = None
    ) -> list[CorporateAction]:
        """Return actions, optionally filtered by instrument and ex-date window."""
        ...

    def add(self, action: CorporateAction) -> None:
        """Persist an action idempotently (same instrument+ex_date+type)."""
        ...


@runtime_checkable
class FundamentalsRepository(Protocol):
    """Storage for point-in-time fundamental records."""

    def upsert(self, record: FundamentalRecord) -> None:
        """Insert or replace a fundamental record (idempotent on full key)."""
        ...

    def query(
        self,
        instrument: str,
        as_of: date,
        metric: str | None = None,
    ) -> list[FundamentalRecord]:
        """Return records with ``filing_date <= as_of`` (no look-ahead)."""
        ...


@runtime_checkable
class FinancialStatementRepository(Protocol):
    """Storage for financial-statement line items (gap 0020)."""

    def upsert(self, statement: FinancialStatement) -> None:
        """Insert or replace a line item (idempotent on the full key)."""
        ...

    def query(
        self,
        instrument: str | None = None,
        statement_type: StatementType | None = None,
        period_type: PeriodType | None = None,
        as_of: date | None = None,
        line_item: str | None = None,
    ) -> list[FinancialStatement]:
        """Return line items, optionally filtered; ``as_of`` filters on
        ``filing_date <= as_of`` (no look-ahead)."""
        ...


@runtime_checkable
class FundamentalRatiosRepository(Protocol):
    """Storage for point-in-time fundamental-ratio snapshots (gap 0020)."""

    def upsert(self, ratios: FinancialRatios) -> None:
        """Insert or replace a ratio snapshot (idempotent on instrument + as_of)."""
        ...

    def query(
        self,
        instrument: str | None = None,
        as_of: date | None = None,
    ) -> list[FinancialRatios]:
        """Return ratio snapshots, optionally filtered by instrument / as-of date."""
        ...


@runtime_checkable
class CatalogRepository(Protocol):
    """Parquet catalog of OHLCV bars keyed by instrument (Lakehouse v3).

    Plan 0009 Phase A / ADR 0004 extend the v1/v2 surface with a git-style
    manifest and ref layer: ``write_bars``/``read_bars`` gain an ``as_of_ref``
    keyword (default ``"main"`` = the live head) and ``resolve_ref`` /
    ``commit`` / ``tag`` manage commit history, refs, and tags. Every new
    keyword carries a default so pre-existing implementations and call-sites
    keep working unchanged.
    """

    def write_bars(self, instrument: str, bars: pl.DataFrame, *, as_of_ref: str = "main") -> None:
        """Persist bars, validating the canonical decimal schema.

        ``as_of_ref`` names the ref the write is staged under (default
        ``"main"``); implementations predating the v3 ref layer may ignore it.
        """
        ...

    def read_bars(
        self,
        instrument: str,
        start: date | None = None,
        end: date | None = None,
        interval: str = "1d",
        *,
        as_of_ref: str = "main",
    ) -> pl.DataFrame:
        """Read bars sorted by timestamp, optionally filtered to a window.

        ``as_of_ref`` selects the commit to read from (``"main"`` is the live
        head); implementations predating the v3 ref layer may ignore it.
        """
        ...

    def resolve_ref(self, ref: str) -> CommitRecord | None:
        """Resolve ``ref`` (branch or tag name) to its pinned commit record,
        or ``None`` when the ref is unknown."""
        ...

    def commit(self, summary: str, *, as_of_ist: date, schema_version: int) -> CommitRecord:
        """Create a commit from the staged tree and return its record.

        ``as_of_ist`` is the IST session boundary the commit is dated to and
        ``schema_version`` the manifest/tree schema version being written.
        """
        ...

    def tag(self, tag: str, ref: str = "main") -> None:
        """Pin ``tag`` to the commit ``ref`` currently resolves to."""
        ...


@runtime_checkable
class MacroSeriesRepository(Protocol):
    """Storage for macroeconomic series observations."""

    def upsert(self, observation: MacroObservation) -> None:
        """Persist an observation idempotently (same series_id+as_of_date)."""
        ...

    def query(
        self,
        series_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MacroObservation]:
        """Return observations for ``series_id`` within ``[start_date, end_date]``."""
        ...


@runtime_checkable
class FilingsRepository(Protocol):
    """Storage for regulatory filings records."""

    def upsert(self, filing: Filing) -> None:
        """Persist a filing idempotently (same instrument+filing_date+title)."""
        ...

    def query(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_type: FilingType | None = None,
    ) -> list[Filing]:
        """Return filings, optionally filtered by instrument/date/type."""
        ...


@runtime_checkable
class CalendarRepository(Protocol):
    """Storage for scheduled calendar events (economic / earnings / dividends /
    splits / IPO)."""

    def upsert(self, event: CalendarEvent) -> None:
        """Persist a calendar event idempotently (same instrument+scheduled_at+event_name)."""
        ...

    def query(
        self,
        instrument: str | None = None,
        category: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[CalendarEvent]:
        """Return calendar events, optionally filtered by instrument/category/date window."""
        ...


@runtime_checkable
class ProfileRepository(Protocol):
    """Storage for company symbol profiles."""

    def upsert(self, profile: SymbolProfile) -> None:
        """Persist a symbol profile idempotently (same symbol)."""
        ...

    def query(
        self,
        symbol: str | None = None,
        sector: str | None = None,
        industry: str | None = None,
    ) -> list[SymbolProfile]:
        """Return symbol profiles, optionally filtered by symbol/sector/industry."""
        ...


@runtime_checkable
class BlockDealsRepository(Protocol):
    """Storage for NSE block/bulk deal disclosures."""

    def upsert(self, deal: BlockDeal) -> None:
        """Persist a deal idempotently (same trade_date+instrument+client_name+side)."""
        ...

    def query(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
        category: str | None = None,
    ) -> list[BlockDeal]:
        """Return deals, optionally filtered by date/instrument/category (block|bulk)."""
        ...


@runtime_checkable
class NewsRepository(Protocol):
    """Storage for financial news articles."""

    def upsert(self, article: NewsArticle) -> None:
        """Persist an article idempotently (same url)."""
        ...

    def query(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        query: str | None = None,
    ) -> list[NewsArticle]:
        """Return articles, optionally filtered by instrument/date window/text query."""
        ...


@runtime_checkable
class InstitutionalFlowRepository(Protocol):
    """Storage for daily FII/DII institutional net-flow records."""

    def upsert(self, flow: InstitutionalFlow) -> None:
        """Persist a flow record idempotently (same trade_date+participant+segment)."""
        ...

    def query(
        self,
        trade_date: date | None = None,
        participant: str | None = None,
        segment: str | None = None,
    ) -> list[InstitutionalFlow]:
        """Return flow records, optionally filtered by date/participant/segment."""
        ...


@runtime_checkable
class DeliveryRepository(Protocol):
    """Storage for per-symbol daily delivery records."""

    def upsert(self, record: DeliveryRecord) -> None:
        """Persist a delivery record idempotently (same trade_date+instrument)."""
        ...

    def query(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
    ) -> list[DeliveryRecord]:
        """Return delivery records, optionally filtered by date/instrument."""
        ...


@runtime_checkable
class MutualFundMetaRepository(Protocol):
    """Storage for mutual-fund scheme metadata (gap 0021)."""

    def upsert(self, meta: MutualFundMeta) -> None:
        """Insert or replace scheme metadata (idempotent on scheme_code + as_of_date)."""
        ...

    def query(
        self,
        scheme_code: str | None = None,
        as_of: date | None = None,
        amc: str | None = None,
    ) -> list[MutualFundMeta]:
        """Return scheme metadata, optionally filtered; ``as_of`` filters on
        ``as_of_date <= as_of`` (no look-ahead)."""
        ...


@runtime_checkable
class MutualFundNavRepository(Protocol):
    """Storage for per-scheme daily NAV history (gap 0021)."""

    def upsert(self, nav: MutualFundNav) -> None:
        """Insert or replace a NAV observation (idempotent on scheme_code + nav_date)."""
        ...

    def query(
        self,
        scheme_code: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MutualFundNav]:
        """Return NAV observations, optionally filtered by scheme / date window."""
        ...


@runtime_checkable
class MutualFundReturnRepository(Protocol):
    """Storage for derived mutual-fund return metrics (gap 0021)."""

    def upsert(self, returns: MutualFundReturn) -> None:
        """Insert or replace a return record (idempotent on scheme_code + as_of_date)."""
        ...

    def query(
        self,
        scheme_code: str | None = None,
        as_of: date | None = None,
    ) -> list[MutualFundReturn]:
        """Return derived-return records, optionally filtered by scheme / as-of date."""
        ...


@runtime_checkable
class IpoRepository(Protocol):
    """Storage for IPO issue metadata (gap 0022)."""

    def upsert(self, issue: IpoIssue) -> None:
        """Insert or replace an issue (idempotent on symbol + as_of_date)."""
        ...

    def query(
        self,
        symbol: str | None = None,
        status: str | None = None,
        as_of: date | None = None,
    ) -> list[IpoIssue]:
        """Return issues, optionally filtered; ``as_of`` filters on
        ``as_of_date <= as_of`` (no look-ahead)."""
        ...


@runtime_checkable
class IpoSubscriptionRepository(Protocol):
    """Storage for IPO subscription snapshots (gap 0022)."""

    def upsert(self, subscription: IpoSubscription) -> None:
        """Insert or replace a snapshot (idempotent on symbol + as_of_date)."""
        ...

    def query(
        self,
        symbol: str | None = None,
        as_of: date | None = None,
    ) -> list[IpoSubscription]:
        """Return subscription snapshots, optionally filtered by symbol / as-of date."""
        ...


@runtime_checkable
class DataService(Protocol):
    """Unified, look-ahead-bias-free data facade."""

    def adjusted_history(
        self,
        instrument: str,
        start: date | None = None,
        end: date | None = None,
    ) -> pl.DataFrame:
        """Raw history restated to the current corporate-action basis."""
        ...


__all__ = [
    "BlockDealsRepository",
    "CalendarRepository",
    "CatalogRepository",
    "CorporateActionsRepository",
    "DataService",
    "DeliveryRepository",
    "FilingsRepository",
    "FinancialStatementRepository",
    "FundamentalRatiosRepository",
    "FundamentalsRepository",
    "InstitutionalFlowRepository",
    "IpoRepository",
    "IpoSubscriptionRepository",
    "MacroSeriesRepository",
    "MutualFundMetaRepository",
    "MutualFundNavRepository",
    "MutualFundReturnRepository",
    "NewsRepository",
    "ProfileRepository",
]
