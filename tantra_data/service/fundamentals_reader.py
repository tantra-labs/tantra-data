"""Polars readers for financial statements and ratio snapshots (gap 0020)."""

from __future__ import annotations

from datetime import date

import polars as pl

from tantra_data.domain.fundamentals import PeriodType
from tantra_data.ports.data_feeds import FundamentalsReader
from tantra_data.ports.repositories import (
    FinancialStatementRepository,
    FundamentalRatiosRepository,
)

__all__ = ["PythonFundamentalsReader"]

STATEMENT_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "instrument": pl.String,
    "statement_type": pl.String,
    "period_type": pl.String,
    "period_end_date": pl.Date,
    "filing_date": pl.Date,
    "line_item": pl.String,
    "value": pl.Decimal(18, 4),
    "currency": pl.String,
}

RATIOS_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "instrument": pl.String,
    "as_of_date": pl.Date,
    "pe": pl.Decimal(18, 4),
    "pb": pl.Decimal(18, 4),
    "ev_to_ebitda": pl.Decimal(18, 4),
    "dividend_yield": pl.Decimal(18, 4),
    "roce": pl.Decimal(18, 4),
    "roe": pl.Decimal(18, 4),
    "debt_to_equity": pl.Decimal(18, 4),
    "interest_coverage": pl.Decimal(18, 4),
    "source": pl.String,
}


class PythonFundamentalsReader(FundamentalsReader):
    """In-process reader over statement and ratio repositories."""

    def __init__(
        self,
        statements: FinancialStatementRepository,
        ratios: FundamentalRatiosRepository,
    ) -> None:
        """Create a reader over the given statement and ratio repositories."""
        self._statements = statements
        self._ratios = ratios

    def get_statements(
        self,
        instrument: str | None = None,
        period_type: PeriodType | str | None = None,
        as_of: date | None = None,
    ) -> pl.DataFrame:
        """Return financial-statement line items as a canonical Polars frame."""
        period = PeriodType(period_type) if isinstance(period_type, str) else period_type
        records = self._statements.query(
            instrument=instrument,
            period_type=period,
            as_of=as_of,
        )
        if not records:
            return pl.DataFrame(schema=STATEMENT_SCHEMA)
        return pl.DataFrame(
            {
                "instrument": [record.instrument for record in records],
                "statement_type": [record.statement_type.value for record in records],
                "period_type": [record.period_type.value for record in records],
                "period_end_date": [record.period_end_date for record in records],
                "filing_date": [record.filing_date for record in records],
                "line_item": [record.line_item for record in records],
                "value": [record.value for record in records],
                "currency": [record.currency for record in records],
            },
            schema=STATEMENT_SCHEMA,
        )

    def get_ratios(
        self,
        instrument: str | None = None,
        as_of: date | None = None,
    ) -> pl.DataFrame:
        """Return point-in-time ratio snapshots as a canonical Polars frame."""
        records = self._ratios.query(instrument=instrument, as_of=as_of)
        if not records:
            return pl.DataFrame(schema=RATIOS_SCHEMA)
        return pl.DataFrame(
            {
                "instrument": [record.instrument for record in records],
                "as_of_date": [record.as_of_date for record in records],
                "pe": [record.pe for record in records],
                "pb": [record.pb for record in records],
                "ev_to_ebitda": [record.ev_to_ebitda for record in records],
                "dividend_yield": [record.dividend_yield for record in records],
                "roce": [record.roce for record in records],
                "roe": [record.roe for record in records],
                "debt_to_equity": [record.debt_to_equity for record in records],
                "interest_coverage": [record.interest_coverage for record in records],
                "source": [record.source for record in records],
            },
            schema=RATIOS_SCHEMA,
        )
