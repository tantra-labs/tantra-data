"""Point-in-time fundamental records.

A ``FundamentalRecord`` pairs the *period* being reported (``period_end_date``)
with the *filing* date on which the disclosure became public. As-of queries must
filter on ``filing_date`` so a backtest on date ``D`` can never observe a
financial fact that was not yet filed — this is the core anti-look-ahead-bias
invariant of the lakehouse.

Gap 0020 adds the financial-statement shape (:class:`FinancialStatement`, one
exact-``Decimal`` line item per income / balance-sheet / cash-flow statement,
annual and quarterly) and a point-in-time ratios snapshot
(:class:`FinancialRatios`, P/E, P/B, EV/EBITDA, dividend yield, ROCE, ROE,
debt/equity, interest coverage). Every value is an exact ``decimal.Decimal``;
value objects are frozen, self-validating dataclasses that compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum

from tantra_data._compat import to_decimal

_RATIO_FIELDS = (
    "pe",
    "pb",
    "ev_to_ebitda",
    "dividend_yield",
    "roce",
    "roe",
    "debt_to_equity",
    "interest_coverage",
)


def _decimal_or_none(value: Decimal | int | str | None) -> Decimal | None:
    """Coerce an int/str/Decimal into an exact ``Decimal``, or ``None``."""
    if value is None:
        return None
    return to_decimal(value)


@dataclass(frozen=True, slots=True)
class FundamentalRecord:
    """A single disclosed fundamental metric for one period."""

    instrument: str
    period_end_date: date
    filing_date: date
    metric: str
    value: Decimal

    def __post_init__(self) -> None:
        if self.filing_date < self.period_end_date:
            raise ValueError(
                "filing_date must be >= period_end_date "
                "(a metric cannot be filed before its period ends; "
                "this guard prevents look-ahead bias)"
            )
        if not self.metric:
            raise ValueError("metric must be non-empty")
        object.__setattr__(self, "value", to_decimal(self.value))


class StatementType(StrEnum):
    """Financial-statement family of a line item."""

    INCOME_STATEMENT = "income_statement"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW = "cash_flow"


class PeriodType(StrEnum):
    """Reporting cadence of a financial statement."""

    ANNUAL = "annual"
    QUARTERLY = "quarterly"


@dataclass(frozen=True, slots=True)
class FinancialStatement:
    """One disclosed line item of a company's financial statements.

    The ``statement_type`` selects the family (income / balance-sheet /
    cash-flow), ``period_type`` the cadence (annual / quarterly), and
    ``line_item`` the canonical metric name (e.g. ``"total_revenue"``,
    ``"net_income"``, ``"total_assets"``). ``filing_date`` is the date the
    disclosure became public (the fetch date for vendor snapshots); as-of
    queries must filter on it so a backtest on date ``D`` can never observe a
    fact that was not yet filed.
    """

    instrument: str
    statement_type: StatementType
    period_type: PeriodType
    period_end_date: date
    filing_date: date
    line_item: str
    value: Decimal
    currency: str = "INR"

    def __post_init__(self) -> None:
        if self.filing_date < self.period_end_date:
            raise ValueError(
                "filing_date must be >= period_end_date "
                "(a line item cannot be filed before its period ends; "
                "this guard prevents look-ahead bias)"
            )
        if not self.instrument:
            raise ValueError("instrument must be non-empty")
        if not self.line_item:
            raise ValueError("line_item must be non-empty")
        if not self.currency:
            raise ValueError("currency must be non-empty")
        object.__setattr__(self, "value", to_decimal(self.value))


@dataclass(frozen=True, slots=True)
class FinancialRatios:
    """A point-in-time snapshot of valuation / profitability ratios.

    One snapshot per ``(instrument, as_of_date)``. Every ratio is the value
    disclosed or derivable as of that date, stored as an exact ``Decimal``
    (or ``None`` when the vendor did not report it — ratios are never
    fabricated).
    """

    instrument: str
    as_of_date: date
    pe: Decimal | None
    pb: Decimal | None
    ev_to_ebitda: Decimal | None
    dividend_yield: Decimal | None
    roce: Decimal | None
    roe: Decimal | None
    debt_to_equity: Decimal | None
    interest_coverage: Decimal | None
    source: str

    def __post_init__(self) -> None:
        if not self.instrument:
            raise ValueError("instrument must be non-empty")
        if not self.source:
            raise ValueError("source must be non-empty")
        for name in _RATIO_FIELDS:
            object.__setattr__(self, name, _decimal_or_none(getattr(self, name)))


__all__ = [
    "FinancialRatios",
    "FinancialStatement",
    "FundamentalRecord",
    "PeriodType",
    "StatementType",
]
