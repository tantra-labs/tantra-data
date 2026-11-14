"""Storage adapters: DuckDB point-in-time stores and the Parquet catalog."""

from tantra_data.adapters.duckdb_store import (
    DuckDbCalendarStore,
    DuckDbCorporateActionsStore,
    DuckDbFundamentalsStore,
    DuckDbProfileStore,
)
from tantra_data.adapters.filings_store import DuckDbFilingsStore
from tantra_data.adapters.fundamental_ratios_store import DuckDbFundamentalRatiosStore
from tantra_data.adapters.fundamental_statement_store import DuckDbFinancialStatementStore
from tantra_data.adapters.macro_store import DuckDbMacroStore
from tantra_data.adapters.parquet_catalog import ParquetCatalog

__all__ = [
    "DuckDbCalendarStore",
    "DuckDbCorporateActionsStore",
    "DuckDbFilingsStore",
    "DuckDbFinancialStatementStore",
    "DuckDbFundamentalRatiosStore",
    "DuckDbFundamentalsStore",
    "DuckDbMacroStore",
    "DuckDbProfileStore",
    "ParquetCatalog",
]
