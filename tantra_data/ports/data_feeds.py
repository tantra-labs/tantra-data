"""Read ports and screener domain types for the analytical data lakehouse.

Defines the consumer-facing read protocols (``MarketReader``, ``ScreenerReader``,
``MacroReader``, ``ReferenceExporter``) and the screener query domain types
(``ScreenQuery``, ``ScreenCondition``, ``ComparisonOp``). The service layer
depends on these protocols, never on concrete storage classes, so DuckDB, Parquet,
or future providers are swappable without touching domain or service code.

Zero-floats note: every price/quantity stored or returned by the catalog is an
exact ``decimal.Decimal`` (``DECIMAL128(18,4)``). Analytical derivatives such as
returns and indicators are computed in Polars (native floating point) purely for
presentation to downstream quantitative engines; no stored financial column ever
uses ``float``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable

import polars as pl

from tantra_data.domain.block_deals import BlockDeal
from tantra_data.domain.calendar import CalendarEvent
from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.domain.institutional_flow import InstitutionalFlow
from tantra_data.domain.news import NewsArticle
from tantra_data.domain.profile import SymbolProfile
from tantra_data.ports.repositories import BAR_SCHEMA, CatalogRepository


class ComparisonOp(StrEnum):
    """Binary comparison operators supported by the screener engine."""

    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"
    EQ = "eq"
    CROSSES_ABOVE = "crosses_above"
    CROSSES_BELOW = "crosses_below"


@dataclass(frozen=True, slots=True)
class ScreenValue:
    """A screener condition value: either a literal Decimal or a field reference."""

    literal: Decimal | None = None
    field: str | None = None

    def __post_init__(self) -> None:
        if self.literal is None and self.field is None:
            raise ValueError("screen value must provide a literal or a field")
        if self.literal is not None and self.field is not None:
            raise ValueError("screen value must provide exactly one of literal/field")
        if self.literal is not None:
            object.__setattr__(self, "literal", Decimal(str(self.literal)))


@dataclass(frozen=True, slots=True)
class ScreenCondition:
    """A single screening predicate over one instrument/field."""

    field: str
    operator: ComparisonOp
    value: ScreenValue


@dataclass(frozen=True, slots=True)
class ScreenQuery:
    """A vectorized screening request over a universe of instruments."""

    universe: list[str]
    conditions: list[ScreenCondition] = field(default_factory=list)
    order_by: str | None = None
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class ScreenResult:
    """Outcome of :meth:`ScreenerReader.execute_screen`."""

    instruments: list[str]
    rows: int
    reference: str | None = None  # "$tantra/ref/<uuid>" when results are exported


@runtime_checkable
class MarketReader(Protocol):
    """Read market data (quotes, candles, returns, indicators) from the lakehouse."""

    def get_quote(self, instrument: str) -> dict[str, object]:
        """Latest quote for ``instrument`` (price + ts)."""
        ...

    def get_candles(
        self,
        instrument: str,
        start: date | None = None,
        end: date | None = None,
        interval: str = "1d",
    ) -> pl.DataFrame:
        """OHLCV bars for ``instrument`` over ``[start, end]``."""
        ...

    def get_returns(
        self,
        instrument: str,
        start: date | None = None,
        end: date | None = None,
        interval: str = "1d",
    ) -> pl.DataFrame:
        """Simple period returns for ``instrument`` over ``[start, end]``."""
        ...

    def get_indicators(
        self,
        instrument: str,
        indicator: str,
        params: dict[str, object] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> pl.DataFrame:
        """Technical indicator values (e.g. SMA, EMA) for ``instrument``."""
        ...


@runtime_checkable
class ScreenerReader(Protocol):
    """Vectorized multi-field screener over a universe of instruments."""

    def execute_screen(self, query: ScreenQuery) -> ScreenResult:
        """Evaluate ``query`` conditions across ``query.universe``."""
        ...


@runtime_checkable
class MacroReader(Protocol):
    """Read macroeconomic series (RBI, WPI, yields, etc.) from the lakehouse."""

    def get_series(
        self,
        series_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Macro series ``series_id`` over ``[start_date, end_date]``."""
        ...


@runtime_checkable
class FilingsReader(Protocol):
    """Read regulatory filings (SEBI/BSE/NSE equivalent of EDGAR) from the lakehouse."""

    def list_filings(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_type: str | None = None,
    ) -> list[object]:
        """Return filings, optionally filtered by instrument/date/type."""
        ...


@runtime_checkable
class ReferenceExporter(Protocol):
    """Ephemeral in-memory reference store emitting ``$tantra/ref/<uuid>`` URIs."""

    def export(self, reference_id: str, frame: pl.DataFrame) -> str:
        """Persist ``frame`` under ``reference_id`` and return its ref URI."""
        ...

    def resolve(self, reference_uri: str) -> pl.DataFrame:
        """Rehydrate a previously exported frame by its ref URI."""
        ...


@runtime_checkable
class CalendarReader(Protocol):
    """Read scheduled calendar events (economic / earnings / dividends / splits /
    IPO) from the lakehouse."""

    def get_calendar(
        self,
        from_date: date | None = None,
        to_date: date | None = None,
        category: str | None = None,
    ) -> pl.DataFrame:
        """Calendar events over ``[from_date, to_date]``, optionally
        category-filtered (``Economic`` / ``Earnings`` / ``Dividends`` /
        ``Splits`` / ``IPO``). Returns an exact-``Decimal`` Polars frame."""
        ...


@runtime_checkable
class ProfileReader(Protocol):
    """Read company symbol profiles from the lakehouse."""

    def get_profile(self, symbol: str) -> SymbolProfile | None:
        """Symbol profile for ``symbol`` (``None`` when unknown)."""
        ...

    def get_profiles(
        self,
        symbols: list[str] | None = None,
        sector: str | None = None,
        industry: str | None = None,
    ) -> list[SymbolProfile]:
        """Symbol profiles, optionally filtered by symbol list / sector /
        industry."""
        ...


@runtime_checkable
class BlockDealsReader(Protocol):
    """Read NSE block/bulk deal disclosures from the lakehouse."""

    def list_deals(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
        category: str | None = None,
    ) -> pl.DataFrame:
        """Deals over ``[from_date, to_date]``, optionally filtered by
        instrument or category (``block`` | ``bulk``). Returns an exact-
        ``Decimal`` Polars frame."""
        ...


@runtime_checkable
class NewsReader(Protocol):
    """Read financial news articles from the lakehouse."""

    def search_news(
        self,
        query: str | None = None,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Articles, optionally filtered by text query / instrument / date
        window. Returns a Polars frame (id, title, source, url,
        published_at, summary, instrument mentions)."""
        ...


@runtime_checkable
class InstitutionalFlowReader(Protocol):
    """Read daily FII/DII institutional net-flow records from the lakehouse."""

    def get_flows(
        self,
        trade_date: date | None = None,
        participant: str | None = None,
        segment: str | None = None,
    ) -> pl.DataFrame:
        """Flow records, optionally filtered by date/participant/segment.
        Returns an exact-``Decimal`` Polars frame (trade_date, participant,
        segment, gross_buy, gross_sell, net)."""
        ...


@runtime_checkable
class DeliveryReader(Protocol):
    """Read per-symbol daily delivery records from the lakehouse."""

    def get_delivery(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
    ) -> pl.DataFrame:
        """Delivery records, optionally filtered by date/instrument. Returns
        an exact-``Decimal`` Polars frame (trade_date, instrument,
        traded_quantity, deliverable_quantity, delivery_pct)."""
        ...


@runtime_checkable
class FundamentalsReader(Protocol):
    """Read financial statements and point-in-time ratios from the lakehouse."""

    def get_statements(
        self,
        instrument: str | None = None,
        period_type: str | None = None,
        as_of: date | None = None,
    ) -> pl.DataFrame:
        """Financial-statement line items, optionally filtered by
        instrument/period_type, with ``filing_date <= as_of``. Returns an
        exact-``Decimal`` Polars frame (instrument, statement_type,
        period_type, period_end_date, filing_date, line_item, value,
        currency)."""
        ...

    def get_ratios(
        self,
        instrument: str | None = None,
        as_of: date | None = None,
    ) -> pl.DataFrame:
        """Point-in-time ratio snapshots, optionally filtered by
        instrument/as_of. Returns an exact-``Decimal`` Polars frame
        (instrument, as_of_date, pe, pb, ev_to_ebitda, dividend_yield, roce,
        roe, debt_to_equity, interest_coverage, source)."""
        ...


__all__ = [
    "BAR_SCHEMA",
    "BlockDeal",
    "BlockDealsReader",
    "CalendarEvent",
    "CalendarReader",
    "CatalogRepository",
    "ComparisonOp",
    "DeliveryReader",
    "DeliveryRecord",
    "FilingsReader",
    "FundamentalsReader",
    "InstitutionalFlow",
    "InstitutionalFlowReader",
    "MacroReader",
    "MarketReader",
    "NewsArticle",
    "NewsReader",
    "ProfileReader",
    "ReferenceExporter",
    "ScreenCondition",
    "ScreenQuery",
    "ScreenResult",
    "ScreenValue",
    "ScreenerReader",
    "SymbolProfile",
]
