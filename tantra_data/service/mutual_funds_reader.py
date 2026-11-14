"""Polars readers for mutual-fund schemes, NAV history, and returns (gap 0021)."""

from __future__ import annotations

from datetime import date

import polars as pl

from tantra_data.ports.data_feeds import MutualFundReader
from tantra_data.ports.repositories import (
    MutualFundMetaRepository,
    MutualFundNavRepository,
    MutualFundReturnRepository,
)

__all__ = ["PythonMutualFundReader"]

MUTUAL_FUND_SCHEMA: dict[str, pl.DataType] = {
    "scheme_code": pl.String(),
    "scheme_name": pl.String(),
    "amc": pl.String(),
    "category": pl.String(),
    "isin_growth": pl.String(),
    "isin_div_reinvestment": pl.String(),
    "plan": pl.String(),
    "option": pl.String(),
    "as_of_date": pl.Date(),
    "source": pl.String(),
}

MF_NAV_SCHEMA: dict[str, pl.DataType] = {
    "scheme_code": pl.String(),
    "nav_date": pl.Date(),
    "nav": pl.Decimal(18, 4),
    "repurchase_price": pl.Decimal(18, 4),
    "sale_price": pl.Decimal(18, 4),
    "source": pl.String(),
}

MF_RETURN_SCHEMA: dict[str, pl.DataType] = {
    "scheme_code": pl.String(),
    "as_of_date": pl.Date(),
    "cagr_1y": pl.Decimal(18, 4),
    "expense_ratio": pl.Decimal(18, 4),
    "source": pl.String(),
}


class PythonMutualFundReader(MutualFundReader):
    """In-process :class:`MutualFundReader` backed by repository stores."""

    def __init__(
        self,
        metas: MutualFundMetaRepository,
        navs: MutualFundNavRepository,
        returns: MutualFundReturnRepository,
    ) -> None:
        """Create a reader over the given mutual-fund repositories."""
        self._metas = metas
        self._navs = navs
        self._returns = returns

    def get_schemes(self, amc: str | None = None) -> pl.DataFrame:
        """Return scheme metadata as a canonical Polars frame."""
        records = self._metas.query(amc=amc)
        if not records:
            return pl.DataFrame(schema=MUTUAL_FUND_SCHEMA)
        return pl.DataFrame(
            {
                "scheme_code": [record.scheme_code for record in records],
                "scheme_name": [record.scheme_name for record in records],
                "amc": [record.amc for record in records],
                "category": [record.category for record in records],
                "isin_growth": [record.isin_growth for record in records],
                "isin_div_reinvestment": [record.isin_div_reinvestment for record in records],
                "plan": [record.plan for record in records],
                "option": [record.option for record in records],
                "as_of_date": [record.as_of_date for record in records],
                "source": [record.source for record in records],
            },
            schema=MUTUAL_FUND_SCHEMA,
        )

    def get_nav_history(
        self,
        scheme_code: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Return NAV history as a canonical Polars frame."""
        records = self._navs.query(
            scheme_code=scheme_code,
            start_date=start_date,
            end_date=end_date,
        )
        if not records:
            return pl.DataFrame(schema=MF_NAV_SCHEMA)
        return pl.DataFrame(
            {
                "scheme_code": [record.scheme_code for record in records],
                "nav_date": [record.nav_date for record in records],
                "nav": [record.nav for record in records],
                "repurchase_price": [record.repurchase_price for record in records],
                "sale_price": [record.sale_price for record in records],
                "source": [record.source for record in records],
            },
            schema=MF_NAV_SCHEMA,
        )

    def get_returns(
        self,
        scheme_code: str | None = None,
        as_of: date | None = None,
    ) -> pl.DataFrame:
        """Return derived return metrics as a canonical Polars frame."""
        records = self._returns.query(scheme_code=scheme_code, as_of=as_of)
        if not records:
            return pl.DataFrame(schema=MF_RETURN_SCHEMA)
        return pl.DataFrame(
            {
                "scheme_code": [record.scheme_code for record in records],
                "as_of_date": [record.as_of_date for record in records],
                "cagr_1y": [record.cagr_1y for record in records],
                "expense_ratio": [record.expense_ratio for record in records],
                "source": [record.source for record in records],
            },
            schema=MF_RETURN_SCHEMA,
        )
