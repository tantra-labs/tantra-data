"""NSE trading-holiday read port (gap 0015).

Consumers (``tantra-runtime-backtest::NseTradingCalendar`` via the Parquet
bridge, and any Python consumer) depend on this protocol, never on a concrete
store, so DuckDB, Parquet, or future providers are swappable without touching
consumer code. The year-scoped read returns the holiday master for a calendar
year as it stands in the store (point-in-time after ingestion).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from tantra_data.domain.trading_holidays import TradingHoliday

__all__ = ["TradingHolidayReader"]


@runtime_checkable
class TradingHolidayReader(Protocol):
    """Read the NSE trading-holiday master for a calendar year."""

    def holidays_for_year(self, year: int) -> list[TradingHoliday]:
        """Return every declared holiday / Muhurat session in ``year``."""
        ...
