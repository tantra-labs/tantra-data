"""DuckDB-backed NSE block/bulk deal disclosure store.

``DuckDbBlockDealsStore`` persists disclosed block and bulk deals with exact
fixed-point ``DECIMAL`` prices and quantities — never floating point. Upserts
are idempotent on the disclosure key ``(trade_date, instrument, client_name,
side)``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.block_deals import BlockDeal

_BLOCK_DEALS_DDL = """
CREATE TABLE IF NOT EXISTS block_deals (
    trade_date    DATE,
    exchange      VARCHAR,
    instrument    VARCHAR,
    security_name VARCHAR,
    client_name   VARCHAR,
    side          VARCHAR,
    category      VARCHAR,
    quantity      DECIMAL(38, 10),
    price         DECIMAL(38, 10),
    remarks       VARCHAR
)
"""


class DuckDbBlockDealsStore:
    """Block/bulk deal disclosure store on DuckDB."""

    def __init__(self, path: str = ":memory:") -> None:
        self._con = duckdb.connect(path)
        self._con.execute(_BLOCK_DEALS_DDL)

    def upsert(self, deal: BlockDeal) -> None:
        key = (deal.trade_date, deal.instrument, deal.client_name, deal.side)
        self._con.execute(
            "DELETE FROM block_deals WHERE trade_date = ? AND instrument = ?"
            " AND client_name = ? AND side = ?",
            key,
        )
        self._con.execute(
            "INSERT INTO block_deals (trade_date, exchange, instrument, security_name,"
            " client_name, side, category, quantity, price, remarks)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                deal.trade_date,
                deal.exchange,
                deal.instrument,
                deal.security_name,
                deal.client_name,
                deal.side,
                deal.category,
                deal.quantity,
                deal.price,
                deal.remarks,
            ),
        )

    def query(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
        category: str | None = None,
    ) -> list[BlockDeal]:
        sql = (
            "SELECT trade_date, exchange, instrument, security_name, client_name, side,"
            " category, quantity, price, remarks FROM block_deals"
        )
        params: list[object] = []
        clauses: list[str] = []
        if trade_date is not None:
            clauses.append("trade_date = ?")
            params.append(trade_date)
        if instrument is not None:
            clauses.append("instrument = ?")
            params.append(instrument)
        if category is not None:
            clauses.append("category = ?")
            params.append(category.lower())
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY trade_date, instrument"
        rows = self._con.execute(sql, params).fetchall()
        return [
            BlockDeal(
                trade_date=row[0],
                exchange=row[1],
                instrument=row[2],
                security_name=row[3],
                client_name=row[4],
                side=row[5],
                category=row[6],
                quantity=Decimal(str(row[7])),
                price=Decimal(str(row[8])),
                remarks=row[9],
            )
            for row in rows
        ]

    def close(self) -> None:
        self._con.close()


__all__ = ["DuckDbBlockDealsStore"]
