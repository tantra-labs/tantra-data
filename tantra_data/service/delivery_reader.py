"""Polars delivery data reader backed by a delivery repository."""

from __future__ import annotations

from datetime import date

import polars as pl

from tantra_data.ports.data_feeds import DeliveryReader
from tantra_data.ports.repositories import DeliveryRepository

__all__ = ["PythonDeliveryReader"]

_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "trade_date": pl.Date,
    "instrument": pl.String,
    "exchange": pl.String,
    "traded_quantity": pl.Int64,
    "deliverable_quantity": pl.Int64,
    "delivery_pct": pl.Decimal(18, 4),
}


class PythonDeliveryReader(DeliveryReader):
    """Read delivery records from a repository as canonical Polars frames."""

    def __init__(self, repository: DeliveryRepository) -> None:
        """Create a reader over the given delivery repository."""
        self._repository = repository

    def get_delivery(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
    ) -> pl.DataFrame:
        """Return delivery records as a DataFrame, or an empty canonical frame."""
        records = self._repository.query(trade_date=trade_date, instrument=instrument)
        if not records:
            return pl.DataFrame(schema=_SCHEMA)
        return pl.DataFrame(
            {
                "trade_date": [record.trade_date for record in records],
                "instrument": [record.instrument for record in records],
                "exchange": [record.exchange for record in records],
                "traded_quantity": [record.traded_quantity for record in records],
                "deliverable_quantity": [record.deliverable_quantity for record in records],
                "delivery_pct": [record.delivery_pct for record in records],
            },
            schema=_SCHEMA,
        )
