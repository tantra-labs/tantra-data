"""DuckDB-backed repository for IPO issue metadata (gap 0022)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.ipos import IpoIssue
from tantra_data.ports.repositories import IpoRepository

__all__ = ["DuckDbIpoStore"]

_COLUMNS = (
    "symbol, company_name, board, status, open_date, close_date, listing_date,"
    " price_band_low, price_band_high, issue_price, lot_size, issue_size_cr,"
    " as_of_date, source"
)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ipo_issues (
    symbol VARCHAR,
    company_name VARCHAR,
    board VARCHAR,
    status VARCHAR,
    open_date DATE,
    close_date DATE,
    listing_date DATE,
    price_band_low DECIMAL(38, 10),
    price_band_high DECIMAL(38, 10),
    issue_price DECIMAL(38, 10),
    lot_size DECIMAL(38, 10),
    issue_size_cr DECIMAL(38, 10),
    as_of_date DATE,
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


class DuckDbIpoStore(IpoRepository):
    """DuckDB-backed IPO issue store.

    ``upsert`` DELETE-then-INSERTs per symbol + as_of_date; ``query`` returns
    typed :class:`IpoIssue` rows, optionally filtered by symbol / status, with
    ``as_of_date <= as_of`` point-in-time semantics.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, issue: IpoIssue) -> None:
        """Insert or replace the issue for its symbol and as-of date."""
        self._connection.execute(
            "DELETE FROM ipo_issues WHERE symbol = ? AND as_of_date = ?",
            [issue.symbol, issue.as_of_date],
        )
        self._connection.execute(
            "INSERT INTO ipo_issues"
            f" ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                issue.symbol,
                issue.company_name,
                issue.board,
                issue.status,
                issue.open_date,
                issue.close_date,
                issue.listing_date,
                str(issue.price_band_low),
                str(issue.price_band_high),
                _str_or_none(issue.issue_price),
                _str_or_none(issue.lot_size),
                _str_or_none(issue.issue_size_cr),
                issue.as_of_date,
                issue.source,
            ],
        )

    def query(
        self,
        symbol: str | None = None,
        status: str | None = None,
        as_of: date | None = None,
    ) -> list[IpoIssue]:
        """Query issues, optionally filtered; ``as_of`` is ``as_of_date <=``."""
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if as_of is not None:
            clauses.append("as_of_date <= ?")
            params.append(as_of)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM ipo_issues{where_clause} ORDER BY as_of_date, symbol",
            params,
        ).fetchall()
        return [
            IpoIssue(
                symbol=row[0],
                company_name=row[1],
                board=row[2],
                status=row[3],
                open_date=row[4],
                close_date=row[5],
                listing_date=row[6],
                price_band_low=Decimal(str(row[7])),
                price_band_high=Decimal(str(row[8])),
                issue_price=_decimal_or_none(row[9]),
                lot_size=_decimal_or_none(row[10]),
                issue_size_cr=_decimal_or_none(row[11]),
                as_of_date=row[12],
                source=row[13],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
