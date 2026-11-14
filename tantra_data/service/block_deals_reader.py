"""NSE block/bulk deal disclosure reader.

Reads disclosed block/bulk deals from a
:class:`~tantra_data.ports.repositories.BlockDealsRepository` and returns them
as Polars frames with exact ``Decimal(18,4)`` price and quantity columns —
never floating point.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from tantra_data.ports.repositories import BlockDealsRepository


class PythonBlockDealsReader:
    """Block/bulk deal reader backed by a block-deals repository."""

    def __init__(self, repository: BlockDealsRepository) -> None:
        self.repository = repository

    def list_deals(
        self,
        trade_date: date | None = None,
        instrument: str | None = None,
        category: str | None = None,
    ) -> pl.DataFrame:
        """Deals, optionally filtered by date/instrument/category.

        Returns an empty canonical frame when no deals match. Prices and
        quantities are exact ``Decimal(18,4)`` (zero floats).
        """
        deals = self.repository.query(trade_date, instrument, category)
        if not deals:
            return pl.DataFrame(
                {
                    "trade_date": [],
                    "exchange": [],
                    "instrument": [],
                    "security_name": [],
                    "client_name": [],
                    "side": [],
                    "category": [],
                    "quantity": [],
                    "price": [],
                    "remarks": [],
                },
                schema={
                    "trade_date": pl.Date,
                    "exchange": pl.String,
                    "instrument": pl.String,
                    "security_name": pl.String,
                    "client_name": pl.String,
                    "side": pl.String,
                    "category": pl.String,
                    "quantity": pl.Decimal(18, 4),
                    "price": pl.Decimal(18, 4),
                    "remarks": pl.String,
                },
            )
        return pl.DataFrame(
            {
                "trade_date": [d.trade_date for d in deals],
                "exchange": [d.exchange for d in deals],
                "instrument": [d.instrument for d in deals],
                "security_name": [d.security_name for d in deals],
                "client_name": [d.client_name for d in deals],
                "side": [d.side for d in deals],
                "category": [d.category for d in deals],
                "quantity": [d.quantity for d in deals],
                "price": [d.price for d in deals],
                "remarks": [d.remarks for d in deals],
            },
            schema={
                "trade_date": pl.Date,
                "exchange": pl.String,
                "instrument": pl.String,
                "security_name": pl.String,
                "client_name": pl.String,
                "side": pl.String,
                "category": pl.String,
                "quantity": pl.Decimal(18, 4),
                "price": pl.Decimal(18, 4),
                "remarks": pl.String,
            },
        )


__all__ = ["PythonBlockDealsReader"]
