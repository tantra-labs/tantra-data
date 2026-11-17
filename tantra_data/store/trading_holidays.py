"""DuckDB-backed NSE trading-holiday store (gap 0015).

The store lives in a single DuckDB database at ``{root}/trading_holidays.duckdb``.
Ingestion is idempotent per calendar year: ``put`` replaces every stored row for
the years present in the incoming list (a year's holiday master is announced in
December of the prior year and occasionally amended mid-year, so re-ingesting a
year is the natural refresh unit). Reads are exact ``date`` rows with no
floats anywhere.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from tantra_data.domain.trading_holidays import TradingHoliday

__all__ = ["DuckDbTradingHolidayStore"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS nse_trading_holidays (
    holiday_date DATE,
    description VARCHAR,
    kind VARCHAR
)
"""

_COLUMNS = "holiday_date, description, kind"


class DuckDbTradingHolidayStore:
    """DuckDB store for the NSE trading-holiday master.

    Mirrors the ``store/option_chain.py`` pattern: the table is created in
    ``__init__``, ``put`` DELETE-then-INSERTs per covered calendar year, and
    the read helpers return typed domain rows.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self._root / "trading_holidays.duckdb"))
        self._connection.execute(CREATE_TABLE_SQL)

    def put(self, holidays: list[TradingHoliday]) -> None:
        """Replace the stored rows for every calendar year in ``holidays``.

        Re-ingesting the same year never duplicates rows and never mixes stale
        entries with the refreshed master.
        """
        years = sorted({holiday.date.year for holiday in holidays})
        for year in years:
            self._connection.execute(
                "DELETE FROM nse_trading_holidays WHERE YEAR(holiday_date) = ?", [year]
            )
        for holiday in holidays:
            self._connection.execute(
                f"INSERT INTO nse_trading_holidays ({_COLUMNS}) VALUES (?, ?, ?)",
                [holiday.date, holiday.description, holiday.kind],
            )

    def holidays_for_year(self, year: int) -> list[TradingHoliday]:
        """Return every stored holiday for ``year``, ordered by date."""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM nse_trading_holidays "
            "WHERE YEAR(holiday_date) = ? ORDER BY holiday_date, kind",
            [year],
        ).fetchall()
        return [self._to_holiday(row) for row in rows]

    def all_holidays(self) -> list[TradingHoliday]:
        """Return every stored holiday across all years, ordered by date."""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM nse_trading_holidays ORDER BY holiday_date, kind"
        ).fetchall()
        return [self._to_holiday(row) for row in rows]

    def years(self) -> list[int]:
        """Return the distinct calendar years present in the store."""
        rows = self._connection.execute(
            "SELECT DISTINCT YEAR(holiday_date) FROM nse_trading_holidays ORDER BY 1"
        ).fetchall()
        return [int(row[0]) for row in rows]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    @staticmethod
    def _to_holiday(row: tuple[object, ...]) -> TradingHoliday:
        holiday_date = row[0]
        if not isinstance(holiday_date, date):
            raise ValueError(f"expected a DATE holiday_date, got {holiday_date!r}")
        return TradingHoliday(date=holiday_date, description=str(row[1]), kind=str(row[2]))
