"""DuckDB-backed repository for derived mutual-fund returns (gap 0021)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.mutual_funds import MutualFundReturn
from tantra_data.ports.repositories import MutualFundReturnRepository

__all__ = ["DuckDbMutualFundReturnStore"]

_COLUMNS = "scheme_code, as_of_date, cagr_1y, expense_ratio, source"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mutual_fund_return (
    scheme_code VARCHAR,
    as_of_date DATE,
    cagr_1y DECIMAL(38, 10),
    expense_ratio DECIMAL(38, 10),
    source VARCHAR
)
"""


def _str_or_none(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


class DuckDbMutualFundReturnStore(MutualFundReturnRepository):
    """DuckDB-backed derived-return store.

    ``upsert`` DELETE-then-INSERTs per scheme_code + as_of_date; ``query``
    returns typed :class:`MutualFundReturn` rows, optionally filtered by
    scheme code / as-of date.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, returns: MutualFundReturn) -> None:
        """Insert or replace the return record for its scheme and as-of date."""
        self._connection.execute(
            "DELETE FROM mutual_fund_return WHERE scheme_code = ? AND as_of_date = ?",
            [returns.scheme_code, returns.as_of_date],
        )
        self._connection.execute(
            f"INSERT INTO mutual_fund_return ({_COLUMNS}) VALUES (?, ?, ?, ?, ?)",
            [
                returns.scheme_code,
                returns.as_of_date,
                _str_or_none(returns.cagr_1y),
                _str_or_none(returns.expense_ratio),
                returns.source,
            ],
        )

    def query(
        self,
        scheme_code: str | None = None,
        as_of: date | None = None,
    ) -> list[MutualFundReturn]:
        """Query derived returns, optionally filtered by scheme / as-of date."""
        clauses: list[str] = []
        params: list[object] = []
        if scheme_code is not None:
            clauses.append("scheme_code = ?")
            params.append(scheme_code)
        if as_of is not None:
            clauses.append("as_of_date <= ?")
            params.append(as_of)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM mutual_fund_return{where_clause}"
            " ORDER BY as_of_date, scheme_code",
            params,
        ).fetchall()
        return [
            MutualFundReturn(
                scheme_code=row[0],
                as_of_date=row[1],
                cagr_1y=_decimal_or_none(row[2]),
                expense_ratio=_decimal_or_none(row[3]),
                source=row[4],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
