"""DuckDB-backed repository for mutual-fund NAV history (gap 0021)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.mutual_funds import MutualFundNav
from tantra_data.ports.repositories import MutualFundNavRepository

__all__ = ["DuckDbMutualFundNavStore"]

_COLUMNS = "scheme_code, nav_date, nav, repurchase_price, sale_price, source"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mutual_fund_nav (
    scheme_code VARCHAR,
    nav_date DATE,
    nav DECIMAL(38, 10),
    repurchase_price DECIMAL(38, 10),
    sale_price DECIMAL(38, 10),
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


class DuckDbMutualFundNavStore(MutualFundNavRepository):
    """DuckDB-backed per-scheme daily NAV-history store.

    ``upsert`` DELETE-then-INSERTs per scheme_code + nav_date; ``query``
    returns typed :class:`MutualFundNav` rows, optionally filtered by scheme
    code / date window.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, nav: MutualFundNav) -> None:
        """Insert or replace the NAV observation for its scheme and date."""
        self._connection.execute(
            "DELETE FROM mutual_fund_nav WHERE scheme_code = ? AND nav_date = ?",
            [nav.scheme_code, nav.nav_date],
        )
        self._connection.execute(
            f"INSERT INTO mutual_fund_nav ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?)",
            [
                nav.scheme_code,
                nav.nav_date,
                str(nav.nav),
                _str_or_none(nav.repurchase_price),
                _str_or_none(nav.sale_price),
                nav.source,
            ],
        )

    def query(
        self,
        scheme_code: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MutualFundNav]:
        """Query NAV observations, optionally filtered by scheme / date window."""
        clauses: list[str] = []
        params: list[object] = []
        if scheme_code is not None:
            clauses.append("scheme_code = ?")
            params.append(scheme_code)
        if start_date is not None:
            clauses.append("nav_date >= ?")
            params.append(start_date)
        if end_date is not None:
            clauses.append("nav_date <= ?")
            params.append(end_date)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM mutual_fund_nav{where_clause} ORDER BY nav_date, scheme_code",
            params,
        ).fetchall()
        return [
            MutualFundNav(
                scheme_code=row[0],
                nav_date=row[1],
                nav=Decimal(str(row[2])),
                repurchase_price=_decimal_or_none(row[3]),
                sale_price=_decimal_or_none(row[4]),
                source=row[5],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
