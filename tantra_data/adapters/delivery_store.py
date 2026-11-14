"""DuckDB-backed delivery repository for per-symbol delivery records."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.ports.repositories import DeliveryRepository

__all__ = ["DuckDbDeliveryStore"]

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS delivery_records (
    trade_date DATE,
    instrument VARCHAR,
    exchange VARCHAR,
    traded_quantity INTEGER,
    deliverable_quantity INTEGER,
    delivery_pct DECIMAL(18, 4)
)
"""


class DuckDbDeliveryStore(DeliveryRepository):
    """Store delivery records in a DuckDB database, keyed by date and instrument."""

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, record: DeliveryRecord) -> None:
        """Insert or replace the record for its trade date and instrument."""
        self._connection.execute(
            "DELETE FROM delivery_records WHERE trade_date = ? AND instrument = ?",
            [record.trade_date, record.instrument],
        )
        self._connection.execute(
            """
            INSERT INTO delivery_records (
                trade_date, instrument, exchange, traded_quantity,
                deliverable_quantity, delivery_pct
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                record.trade_date,
                record.instrument,
                record.exchange,
                record.traded_quantity,
                record.deliverable_quantity,
                record.delivery_pct,
            ],
        )

    def query(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
    ) -> list[DeliveryRecord]:
        """Query delivery records, optionally filtered by date and instrument."""
        conditions: list[str] = []
        params: list[object] = []
        if trade_date is not None:
            conditions.append("trade_date = ?")
            params.append(trade_date)
        if instrument is not None:
            conditions.append("instrument = ?")
            params.append(instrument)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self._connection.execute(
            f"""
            SELECT trade_date, instrument, exchange, traded_quantity,
                   deliverable_quantity, delivery_pct
            FROM delivery_records
            {where}
            ORDER BY trade_date, instrument
            """,
            params,
        ).fetchall()
        return [
            DeliveryRecord(
                trade_date=row[0],
                instrument=row[1],
                exchange=row[2],
                traded_quantity=row[3],
                deliverable_quantity=row[4],
                delivery_pct=Decimal(str(row[5])),
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
