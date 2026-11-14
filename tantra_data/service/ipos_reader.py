"""Polars readers for IPO issues and subscription snapshots (gap 0022)."""

from __future__ import annotations

from datetime import date

import polars as pl

from tantra_data.ports.data_feeds import IpoReader
from tantra_data.ports.repositories import IpoRepository, IpoSubscriptionRepository

__all__ = ["PythonIpoReader"]

IPO_SCHEMA: dict[str, pl.DataType] = {
    "symbol": pl.String(),
    "company_name": pl.String(),
    "board": pl.String(),
    "status": pl.String(),
    "open_date": pl.Date(),
    "close_date": pl.Date(),
    "listing_date": pl.Date(),
    "price_band_low": pl.Decimal(18, 4),
    "price_band_high": pl.Decimal(18, 4),
    "issue_price": pl.Decimal(18, 4),
    "lot_size": pl.Decimal(18, 4),
    "issue_size_cr": pl.Decimal(18, 4),
    "as_of_date": pl.Date(),
    "source": pl.String(),
}

IPO_SUBSCRIPTION_SCHEMA: dict[str, pl.DataType] = {
    "symbol": pl.String(),
    "as_of_date": pl.Date(),
    "qib": pl.Decimal(18, 4),
    "nii": pl.Decimal(18, 4),
    "bnii": pl.Decimal(18, 4),
    "snii": pl.Decimal(18, 4),
    "rii": pl.Decimal(18, 4),
    "total": pl.Decimal(18, 4),
    "source": pl.String(),
}


class PythonIpoReader(IpoReader):
    """In-process :class:`IpoReader` backed by repository stores."""

    def __init__(
        self,
        issues: IpoRepository,
        subscriptions: IpoSubscriptionRepository,
    ) -> None:
        """Create a reader over the given IPO repositories."""
        self._issues = issues
        self._subscriptions = subscriptions

    def get_issues(
        self,
        status: str | None = None,
        as_of: date | None = None,
    ) -> pl.DataFrame:
        """Return IPO issues as a canonical Polars frame."""
        records = self._issues.query(status=status, as_of=as_of)
        if not records:
            return pl.DataFrame(schema=IPO_SCHEMA)
        return pl.DataFrame(
            {
                "symbol": [record.symbol for record in records],
                "company_name": [record.company_name for record in records],
                "board": [record.board for record in records],
                "status": [record.status for record in records],
                "open_date": [record.open_date for record in records],
                "close_date": [record.close_date for record in records],
                "listing_date": [record.listing_date for record in records],
                "price_band_low": [record.price_band_low for record in records],
                "price_band_high": [record.price_band_high for record in records],
                "issue_price": [record.issue_price for record in records],
                "lot_size": [record.lot_size for record in records],
                "issue_size_cr": [record.issue_size_cr for record in records],
                "as_of_date": [record.as_of_date for record in records],
                "source": [record.source for record in records],
            },
            schema=IPO_SCHEMA,
        )

    def get_subscriptions(
        self,
        symbol: str | None = None,
        as_of: date | None = None,
    ) -> pl.DataFrame:
        """Return IPO subscription snapshots as a canonical Polars frame."""
        records = self._subscriptions.query(symbol=symbol, as_of=as_of)
        if not records:
            return pl.DataFrame(schema=IPO_SUBSCRIPTION_SCHEMA)
        return pl.DataFrame(
            {
                "symbol": [record.symbol for record in records],
                "as_of_date": [record.as_of_date for record in records],
                "qib": [record.qib for record in records],
                "nii": [record.nii for record in records],
                "bnii": [record.bnii for record in records],
                "snii": [record.snii for record in records],
                "rii": [record.rii for record in records],
                "total": [record.total for record in records],
                "source": [record.source for record in records],
            },
            schema=IPO_SUBSCRIPTION_SCHEMA,
        )
