"""Yahoo Finance (yfinance) company-fundamentals provider (gap 0020).

Fetches financial statements (income / balance-sheet / cash-flow, annual and
quarterly) and a point-in-time valuation-ratios snapshot from
``yfinance.Ticker("<SYMBOL>.NS")`` and maps them onto canonical
:class:`~tantra_data.domain.fundamentals.FinancialStatement` /
:class:`~tantra_data.domain.fundamentals.FinancialRatios` rows.

Verified live against ``RELIANCE.NS`` (2026-11-14): ``Ticker.info`` returns
valuation ratios (``trailingPE`` 22.08, ``priceToBook`` 1.92,
``enterpriseToEbitda`` 12.00, ``dividendYield`` 0.45, ``returnOnEquity``
0.08759, ``debtToEquity`` 44.325) and ``Ticker.financials`` /
``.balance_sheet`` / ``.cashflow`` return statement DataFrames indexed by line
item with period-end date columns (e.g. FY2026 ``Total Revenue`` =
10,572,190,000,000 INR).

``yfinance`` is an optional dependency: if it is not installed this module
degrades gracefully (``yfinance_available()`` is ``False`` and
``fetch_fundamentals`` raises a clear :class:`ImportError`). Every monetary
value and ratio is converted to an exact ``decimal.Decimal`` at the parse
boundary (``Decimal(str(value))``) — never ``float``.

Line items are canonicalized to snake_case metric names (``total_revenue``,
``net_income``, ``ebit``, ``interest_expense``, ``total_assets``,
``total_debt``, ``stockholders_equity``, ``current_liabilities``,
``operating_cash_flow``, ...). Four ratios come straight from the vendor's
``info`` (P/E, P/B, EV/EBITDA, dividend yield); the other four (ROCE, ROE,
debt/equity, interest coverage) are derived with exact ``Decimal`` arithmetic
from the latest annual statement, falling back to vendor-reported values when
a derivation is not computable — no ratio is ever fabricated.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, cast

from tantra_data.domain.fundamentals import (
    FinancialRatios,
    FinancialStatement,
    PeriodType,
    StatementType,
)

__all__ = [
    "ParsedFundamentals",
    "RatioRow",
    "StatementRow",
    "fetch_fundamentals",
    "ingest_fundamentals",
    "parse_fundamentals",
    "rows_to_ratios",
    "rows_to_statements",
    "yfinance_available",
]

try:  # yfinance is optional; degrade gracefully when unavailable
    import yfinance as yf

    _YFINANCE_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without yfinance installed
    yf = None
    _YFINANCE_AVAILABLE = False

SOURCE = "yfinance"
_DEFAULT_CURRENCY = "INR"
_NSE_SUFFIX = ".NS"
_QUANT = Decimal("0.0001")
_HUNDRED = Decimal("100")

_DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y")

_LINE_ITEM_ALIASES = {
    "Total Revenue": "total_revenue",
    "Operating Revenue": "total_revenue",
    "Net Income": "net_income",
    "Net Income Common Stockholders": "net_income",
    "Net Income Continuous Operations": "net_income",
    "Net Income From Continuing And Discontinued Operation": "net_income",
    "EBIT": "ebit",
    "EBITDA": "ebitda",
    "Operating Income": "operating_income",
    "Interest Expense": "interest_expense",
    "Interest Expense Non Operating": "interest_expense",
    "Total Assets": "total_assets",
    "Total Debt": "total_debt",
    "Stockholders Equity": "stockholders_equity",
    "Total Equity Gross Minority Interest": "stockholders_equity",
    "Common Stock Equity": "stockholders_equity",
    "Current Liabilities": "current_liabilities",
    "Cash And Cash Equivalents": "cash_and_cash_equivalents",
    "Operating Cash Flow": "operating_cash_flow",
    "Capital Expenditure": "capital_expenditure",
    "Free Cash Flow": "free_cash_flow",
}


@dataclass(frozen=True)
class StatementRow:
    """One parsed financial-statement line item."""

    instrument: str
    statement_type: StatementType
    period_type: PeriodType
    period_end_date: date
    filing_date: date
    line_item: str
    value: Decimal
    currency: str = _DEFAULT_CURRENCY


@dataclass(frozen=True)
class RatioRow:
    """One parsed point-in-time ratio snapshot."""

    symbol: str
    as_of_date: date
    pe: Decimal | None
    pb: Decimal | None
    ev_to_ebitda: Decimal | None
    dividend_yield: Decimal | None
    roce: Decimal | None
    roe: Decimal | None
    debt_to_equity: Decimal | None
    interest_coverage: Decimal | None
    source: str = SOURCE


@dataclass(frozen=True)
class ParsedFundamentals:
    """Canonical parsed rows from one yfinance-style fundamentals payload."""

    symbol: str
    as_of_date: date
    statements: list[StatementRow]
    ratios: RatioRow | None


def yfinance_available() -> bool:
    """Return ``True`` when the ``yfinance`` package is importable."""
    return _YFINANCE_AVAILABLE


def _parse_date(value: object) -> date:
    """Parse a period-end / as-of date from a string, date, or datetime."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unsupported fundamentals date: {text!r}")


def _to_decimal(value: object) -> Decimal | None:
    """Convert a vendor value to an exact ``Decimal``, or ``None``."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _snake_case(name: str) -> str:
    """Normalize an arbitrary line-item name to ``snake_case``."""
    words = [chunk.lower() for chunk in re.split(r"[^0-9A-Za-z]+", name) if chunk]
    return "_".join(words)


def _canonical_line_item(name: str) -> str:
    """Map a vendor line-item name onto a canonical snake_case metric name."""
    key = name.strip()
    canonical = _LINE_ITEM_ALIASES.get(key)
    if canonical is None:
        canonical = _snake_case(key)
    return canonical


def parse_fundamentals(payload: object) -> ParsedFundamentals:
    """Deterministically parse a yfinance-style fundamentals payload.

    The documented payload shape is::

        {
          "symbol": "RELIANCE",
          "as_of_date": "2026-11-14",
          "info": {"trailingPE": 22.08, "priceToBook": 1.92, ...},
          "statements": {
            "annual": {
              "income_statement": {"2026-03-31": {"Total Revenue": 10572190000000.0, ...}},
              "balance_sheet": {...},
              "cash_flow": {...},
            },
            "quarterly": {...},
          },
        }

    Every numeric value is converted to an exact ``Decimal`` at the parse
    boundary; non-numeric values are skipped (never fabricated).
    """
    if not isinstance(payload, dict):
        raise ValueError("fundamentals payload must be a dict")
    symbol = payload.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("fundamentals payload is missing a symbol")
    as_of = _parse_date(payload.get("as_of_date"))
    instrument = f"NSE:{symbol}-EQ"

    statements: list[StatementRow] = []
    statements_block = payload.get("statements")
    if isinstance(statements_block, dict):
        for period_key, period_type in (
            ("annual", PeriodType.ANNUAL),
            ("quarterly", PeriodType.QUARTERLY),
        ):
            period_block = statements_block.get(period_key)
            if not isinstance(period_block, dict):
                continue
            for stmt_key, stmt_type in (
                ("income_statement", StatementType.INCOME_STATEMENT),
                ("balance_sheet", StatementType.BALANCE_SHEET),
                ("cash_flow", StatementType.CASH_FLOW),
            ):
                periods = period_block.get(stmt_key)
                if not isinstance(periods, dict):
                    continue
                for period_text, items in periods.items():
                    period_end = _parse_date(period_text)
                    if not isinstance(items, dict):
                        continue
                    for line_item, value in items.items():
                        decimal_value = _to_decimal(value)
                        if decimal_value is None:
                            continue
                        statements.append(
                            StatementRow(
                                instrument=instrument,
                                statement_type=stmt_type,
                                period_type=period_type,
                                period_end_date=period_end,
                                filing_date=as_of,
                                line_item=_canonical_line_item(str(line_item)),
                                value=decimal_value,
                            )
                        )

    ratios = _parse_ratios(payload, symbol, as_of, statements)
    return ParsedFundamentals(
        symbol=symbol,
        as_of_date=as_of,
        statements=statements,
        ratios=ratios,
    )


def _ratio(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    """Compute ``numerator / denominator`` exactly, or ``None`` when undefined."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return (numerator / denominator).quantize(_QUANT)


def _latest_annual(statements: list[StatementRow]) -> dict[str, Decimal]:
    """Latest annual-period line items as ``{canonical_line_item: Decimal}``.

    When multiple vendor line items collapse onto one canonical metric the
    first occurrence wins (it is the primary vendor row); later aliases never
    overwrite the earlier one.
    """
    annual = [row for row in statements if row.period_type is PeriodType.ANNUAL]
    if not annual:
        return {}
    latest_end = max(row.period_end_date for row in annual)
    latest: dict[str, Decimal] = {}
    for row in annual:
        if row.period_end_date != latest_end:
            continue
        latest.setdefault(row.line_item, row.value)
    return latest


def _parse_ratios(
    payload: dict[object, object],
    symbol: str,
    as_of: date,
    statements: list[StatementRow],
) -> RatioRow | None:
    """Build a ratio snapshot from ``info`` + the latest annual statement."""
    info = payload.get("info")
    if not isinstance(info, dict):
        info = {}

    latest = _latest_annual(statements)
    ebit = latest.get("ebit")
    interest_expense = latest.get("interest_expense")
    net_income = latest.get("net_income")
    total_assets = latest.get("total_assets")
    current_liabilities = latest.get("current_liabilities")
    total_debt = latest.get("total_debt")
    equity = latest.get("stockholders_equity")

    capital_employed = total_assets
    if total_assets is not None and current_liabilities is not None:
        capital_employed = total_assets - current_liabilities

    roce = _ratio(ebit, capital_employed)
    roe = _ratio(net_income, equity)
    debt_to_equity = None
    if total_debt is not None and equity is not None and equity != 0:
        # One-step quantize keeps the percentage exact (avoids a double-round).
        debt_to_equity = (total_debt / equity * _HUNDRED).quantize(_QUANT)
    interest_coverage = _ratio(ebit, interest_expense)

    if roce is None:
        roce = _to_decimal(info.get("returnOnCapital"))
    if roe is None:
        roe = _to_decimal(info.get("returnOnEquity"))
    if debt_to_equity is None:
        debt_to_equity = _to_decimal(info.get("debtToEquity"))

    return RatioRow(
        symbol=symbol,
        as_of_date=as_of,
        pe=_to_decimal(info.get("trailingPE")),
        pb=_to_decimal(info.get("priceToBook")),
        ev_to_ebitda=_to_decimal(info.get("enterpriseToEbitda")),
        dividend_yield=_to_decimal(info.get("dividendYield")),
        roce=roce,
        roe=roe,
        debt_to_equity=debt_to_equity,
        interest_coverage=interest_coverage,
    )


def rows_to_statements(rows: list[StatementRow]) -> list[FinancialStatement]:
    """Map parsed statement rows onto canonical domain records."""
    return [
        FinancialStatement(
            instrument=row.instrument,
            statement_type=row.statement_type,
            period_type=row.period_type,
            period_end_date=row.period_end_date,
            filing_date=row.filing_date,
            line_item=row.line_item,
            value=row.value,
            currency=row.currency,
        )
        for row in rows
    ]


def rows_to_ratios(row: RatioRow | None) -> FinancialRatios | None:
    """Map a parsed ratio row onto the canonical domain snapshot."""
    if row is None:
        return None
    return FinancialRatios(
        instrument=f"NSE:{row.symbol}-EQ",
        as_of_date=row.as_of_date,
        pe=row.pe,
        pb=row.pb,
        ev_to_ebitda=row.ev_to_ebitda,
        dividend_yield=row.dividend_yield,
        roce=row.roce,
        roe=row.roe,
        debt_to_equity=row.debt_to_equity,
        interest_coverage=row.interest_coverage,
        source=row.source,
    )


def _period_key(column: object) -> str:
    """Stringify a statement column (a period-end timestamp/date) as ISO."""
    if isinstance(column, datetime):
        return column.date().isoformat()
    if isinstance(column, date):
        return column.isoformat()
    return str(column)


def _frame_to_periods(frame: object) -> dict[str, dict[str, object]]:
    """Convert a yfinance statement frame to ``{period: {line_item: value}}``.

    yfinance statements are pandas-like DataFrames indexed by line item with
    period-end date columns; plain dict payloads are also accepted.
    """
    result: dict[str, dict[str, object]] = {}
    if frame is None:
        return result
    if isinstance(frame, dict):
        for period, items in frame.items():
            if isinstance(items, dict):
                result[str(period)] = {str(k): v for k, v in items.items()}
        return result
    columns = getattr(frame, "columns", None)
    if columns is None:
        return result
    frame_any = cast(Any, frame)
    for column in columns:
        period = _period_key(column)
        line_items: dict[str, object] = {}
        for line_item, value in frame_any[column].items():
            if value is None:
                continue
            if isinstance(value, (str, int, float, Decimal)):
                line_items[str(line_item)] = value
        if line_items:
            result[period] = line_items
    return result


def _statement_blocks(
    financials: object,
    balance_sheet: object,
    cashflow: object,
) -> dict[str, dict[str, dict[str, object]]]:
    """Build the ``statements`` block for a period cadence."""
    return {
        "income_statement": _frame_to_periods(financials),
        "balance_sheet": _frame_to_periods(balance_sheet),
        "cash_flow": _frame_to_periods(cashflow),
    }


def fetch_fundamentals(symbol: str, as_of: date | None = None) -> ParsedFundamentals:
    """Fetch live fundamentals for ``symbol`` from yfinance.

    Symbols are joined with ``.NS`` for the NSE listing. ``as_of`` defaults to
    today (the fetch date) and becomes the ``filing_date`` of every statement
    row and the ``as_of_date`` of the ratios snapshot. Raises
    :class:`ImportError` when ``yfinance`` is not installed.
    """
    if not _YFINANCE_AVAILABLE or yf is None:
        raise ImportError(
            "yfinance is not installed; install it with `pip install yfinance` "
            "to fetch company fundamentals from Yahoo Finance"
        )
    if as_of is None:
        as_of = date.today()
    ticker = yf.Ticker(f"{symbol}{_NSE_SUFFIX}")
    info = ticker.info or {}
    payload = {
        "symbol": symbol,
        "as_of_date": as_of.isoformat(),
        "info": info,
        "statements": {
            "annual": _statement_blocks(ticker.financials, ticker.balance_sheet, ticker.cashflow),
            "quarterly": _statement_blocks(
                ticker.quarterly_financials,
                ticker.quarterly_balance_sheet,
                ticker.quarterly_cashflow,
            ),
        },
    }
    return parse_fundamentals(payload)


class _StatementStore(Protocol):
    """Minimal structural contract for a financial-statement store."""

    def upsert(self, statement: FinancialStatement) -> None: ...


class _RatiosStore(Protocol):
    """Minimal structural contract for a fundamental-ratio store."""

    def upsert(self, ratios: FinancialRatios) -> None: ...


def ingest_fundamentals(
    statement_store: _StatementStore,
    ratios_store: _RatiosStore,
    symbol: str,
    as_of: date | None = None,
) -> tuple[int, int]:
    """Fetch live fundamentals for ``symbol`` and upsert them into stores.

    Returns ``(statement_row_count, ratio_snapshot_count)``.
    """
    parsed = fetch_fundamentals(symbol, as_of)
    statements = rows_to_statements(parsed.statements)
    for statement in statements:
        statement_store.upsert(statement)
    ratios = rows_to_ratios(parsed.ratios)
    if ratios is not None:
        ratios_store.upsert(ratios)
    return len(statements), 1 if ratios is not None else 0
