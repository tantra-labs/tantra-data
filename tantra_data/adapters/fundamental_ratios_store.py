"""DuckDB-backed repository for point-in-time ratio snapshots (gap 0020)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.fundamentals import FinancialRatios
from tantra_data.ports.repositories import FundamentalRatiosRepository

__all__ = ["DuckDbFundamentalRatiosStore"]

_RATIO_COLUMNS = (
    "pe, pb, ev_to_ebitda, dividend_yield, roce, roe, debt_to_equity, interest_coverage"
)

_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS fundamental_ratios (
    instrument VARCHAR,
    as_of_date DATE,
    {_RATIO_COLUMNS.replace(", ", " DECIMAL(38, 10), ") + " DECIMAL(38, 10)"},
    source VARCHAR
)
"""

_COLUMNS = f"instrument, as_of_date, {_RATIO_COLUMNS}, source"


def _str_or_none(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


class DuckDbFundamentalRatiosStore(FundamentalRatiosRepository):
    """DuckDB-backed point-in-time ratio-snapshot store.

    ``upsert`` DELETE-then-INSERTs per instrument + as_of_date; ``query``
    returns typed :class:`FinancialRatios` rows, optionally filtered by
    instrument / as-of date.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, ratios: FinancialRatios) -> None:
        """Insert or replace the snapshot for its instrument and as-of date."""
        self._connection.execute(
            "DELETE FROM fundamental_ratios WHERE instrument = ? AND as_of_date = ?",
            [ratios.instrument, ratios.as_of_date],
        )
        self._connection.execute(
            f"INSERT INTO fundamental_ratios ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ratios.instrument,
                ratios.as_of_date,
                _str_or_none(ratios.pe),
                _str_or_none(ratios.pb),
                _str_or_none(ratios.ev_to_ebitda),
                _str_or_none(ratios.dividend_yield),
                _str_or_none(ratios.roce),
                _str_or_none(ratios.roe),
                _str_or_none(ratios.debt_to_equity),
                _str_or_none(ratios.interest_coverage),
                ratios.source,
            ],
        )

    def query(
        self,
        instrument: str | None = None,
        as_of: date | None = None,
    ) -> list[FinancialRatios]:
        """Query ratio snapshots, optionally filtered by instrument / as-of date."""
        clauses: list[str] = []
        params: list[object] = []
        if instrument is not None:
            clauses.append("instrument = ?")
            params.append(instrument)
        if as_of is not None:
            clauses.append("as_of_date <= ?")
            params.append(as_of)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM fundamental_ratios{where_clause}"
            " ORDER BY as_of_date, instrument",
            params,
        ).fetchall()
        return [
            FinancialRatios(
                instrument=row[0],
                as_of_date=row[1],
                pe=_decimal_or_none(row[2]),
                pb=_decimal_or_none(row[3]),
                ev_to_ebitda=_decimal_or_none(row[4]),
                dividend_yield=_decimal_or_none(row[5]),
                roce=_decimal_or_none(row[6]),
                roe=_decimal_or_none(row[7]),
                debt_to_equity=_decimal_or_none(row[8]),
                interest_coverage=_decimal_or_none(row[9]),
                source=row[10],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
