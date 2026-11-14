"""DuckDB-backed repository for IPO subscription snapshots (gap 0022)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.ipos import IpoSubscription
from tantra_data.ports.repositories import IpoSubscriptionRepository

__all__ = ["DuckDbIpoSubscriptionStore"]

_COLUMNS = "symbol, as_of_date, qib, nii, bnii, snii, rii, total, source"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ipo_subscriptions (
    symbol VARCHAR,
    as_of_date DATE,
    qib DECIMAL(38, 10),
    nii DECIMAL(38, 10),
    bnii DECIMAL(38, 10),
    snii DECIMAL(38, 10),
    rii DECIMAL(38, 10),
    total DECIMAL(38, 10),
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


class DuckDbIpoSubscriptionStore(IpoSubscriptionRepository):
    """DuckDB-backed IPO subscription-snapshot store.

    ``upsert`` DELETE-then-INSERTs per symbol + as_of_date; ``query`` returns
    typed :class:`IpoSubscription` rows, optionally filtered by symbol, with
    ``as_of_date <= as_of`` point-in-time semantics.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, subscription: IpoSubscription) -> None:
        """Insert or replace the snapshot for its symbol and as-of date."""
        self._connection.execute(
            "DELETE FROM ipo_subscriptions WHERE symbol = ? AND as_of_date = ?",
            [subscription.symbol, subscription.as_of_date],
        )
        self._connection.execute(
            f"INSERT INTO ipo_subscriptions ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                subscription.symbol,
                subscription.as_of_date,
                _str_or_none(subscription.qib),
                _str_or_none(subscription.nii),
                _str_or_none(subscription.bnii),
                _str_or_none(subscription.snii),
                _str_or_none(subscription.rii),
                _str_or_none(subscription.total),
                subscription.source,
            ],
        )

    def query(
        self,
        symbol: str | None = None,
        as_of: date | None = None,
    ) -> list[IpoSubscription]:
        """Query subscription snapshots, optionally filtered by symbol / as-of date."""
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        if as_of is not None:
            clauses.append("as_of_date <= ?")
            params.append(as_of)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM ipo_subscriptions{where_clause} ORDER BY as_of_date, symbol",
            params,
        ).fetchall()
        return [
            IpoSubscription(
                symbol=row[0],
                as_of_date=row[1],
                qib=_decimal_or_none(row[2]),
                nii=_decimal_or_none(row[3]),
                bnii=_decimal_or_none(row[4]),
                snii=_decimal_or_none(row[5]),
                rii=_decimal_or_none(row[6]),
                total=_decimal_or_none(row[7]),
                source=row[8],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
