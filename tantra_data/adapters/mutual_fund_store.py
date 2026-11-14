"""DuckDB-backed repository for mutual-fund scheme metadata (gap 0021)."""

from __future__ import annotations

from datetime import date

import duckdb

from tantra_data.domain.mutual_funds import MutualFundMeta
from tantra_data.ports.repositories import MutualFundMetaRepository

__all__ = ["DuckDbMutualFundStore"]

_COLUMNS = (
    "scheme_code, scheme_name, amc, category, isin_growth,"
    " isin_div_reinvestment, plan, option, as_of_date, source"
)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mutual_fund_meta (
    scheme_code VARCHAR,
    scheme_name VARCHAR,
    amc VARCHAR,
    category VARCHAR,
    isin_growth VARCHAR,
    isin_div_reinvestment VARCHAR,
    plan VARCHAR,
    option VARCHAR,
    as_of_date DATE,
    source VARCHAR
)
"""


class DuckDbMutualFundStore(MutualFundMetaRepository):
    """DuckDB-backed scheme-metadata store.

    ``upsert`` DELETE-then-INSERTs per scheme_code + as_of_date; ``query``
    returns typed :class:`MutualFundMeta` rows, optionally filtered by scheme
    code / AMC, with ``as_of_date <= as_of`` point-in-time semantics.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, meta: MutualFundMeta) -> None:
        """Insert or replace the scheme for its code and as-of date."""
        self._connection.execute(
            "DELETE FROM mutual_fund_meta WHERE scheme_code = ? AND as_of_date = ?",
            [meta.scheme_code, meta.as_of_date],
        )
        self._connection.execute(
            f"INSERT INTO mutual_fund_meta ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                meta.scheme_code,
                meta.scheme_name,
                meta.amc,
                meta.category,
                meta.isin_growth,
                meta.isin_div_reinvestment,
                meta.plan,
                meta.option,
                meta.as_of_date,
                meta.source,
            ],
        )

    def query(
        self,
        scheme_code: str | None = None,
        as_of: date | None = None,
        amc: str | None = None,
    ) -> list[MutualFundMeta]:
        """Query scheme metadata, optionally filtered; ``as_of`` is ``as_of_date <=``."""
        clauses: list[str] = []
        params: list[object] = []
        if scheme_code is not None:
            clauses.append("scheme_code = ?")
            params.append(scheme_code)
        if as_of is not None:
            clauses.append("as_of_date <= ?")
            params.append(as_of)
        if amc is not None:
            clauses.append("amc = ?")
            params.append(amc)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM mutual_fund_meta{where_clause}"
            " ORDER BY as_of_date, scheme_code",
            params,
        ).fetchall()
        return [
            MutualFundMeta(
                scheme_code=row[0],
                scheme_name=row[1],
                amc=row[2],
                category=row[3],
                isin_growth=row[4],
                isin_div_reinvestment=row[5],
                plan=row[6],
                option=row[7],
                as_of_date=row[8],
                source=row[9],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
