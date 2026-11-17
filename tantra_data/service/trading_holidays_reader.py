"""Python trading-holiday reader over the DuckDB store (gap 0015)."""

from __future__ import annotations

from tantra_data.domain.trading_holidays import TradingHoliday
from tantra_data.store.trading_holidays import DuckDbTradingHolidayStore


class PythonTradingHolidayReader:
    """Reader backed by a ``DuckDbTradingHolidayStore`` (satisfies the
    ``TradingHolidayReader`` port)."""

    def __init__(self, store: DuckDbTradingHolidayStore) -> None:
        self.store = store

    def holidays_for_year(self, year: int) -> list[TradingHoliday]:
        """Return the stored holiday master for ``year``."""
        return self.store.holidays_for_year(year)


__all__ = ["PythonTradingHolidayReader"]
