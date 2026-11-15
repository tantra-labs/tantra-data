"""tantra-data — Point-in-Time Financial Lakehouse & Corporate Actions.

Tier 6A bounded context. Domain value objects are pure and self-validating;
ports are protocols; adapters implement DuckDB/Parquet storage; providers ingest
official NSE files; the service facade composes everything into a
look-ahead-bias-free data service.
"""

from __future__ import annotations

from tantra_data._compat import VYAAPAAR_AVAILABLE
from tantra_data.adapters import (
    DuckDbCalendarStore,
    DuckDbCorporateActionsStore,
    DuckDbFilingsStore,
    DuckDbFinancialStatementStore,
    DuckDbFundamentalRatiosStore,
    DuckDbFundamentalsStore,
    DuckDbMacroStore,
    DuckDbProfileStore,
    ParquetCatalog,
)
from tantra_data.domain.block_deals import BlockDeal
from tantra_data.domain.calendar import CalendarEvent
from tantra_data.domain.corporate_actions import (
    Bonus,
    CorporateAction,
    CorporateActionRecord,
    CorporateActionType,
    Dividend,
    Rights,
    Split,
    from_record,
    to_record,
)
from tantra_data.domain.coverage import (
    CoverageEntry,
    CoverageLedger,
    CoverageReport,
    InstrumentCoverage,
)
from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.domain.filings import Filing, FilingType
from tantra_data.domain.fundamentals import (
    FinancialRatios,
    FinancialStatement,
    FundamentalRecord,
    PeriodType,
    StatementType,
)
from tantra_data.domain.institutional_flow import InstitutionalFlow
from tantra_data.domain.macro import MacroObservation
from tantra_data.domain.news import NewsArticle
from tantra_data.domain.profile import SymbolProfile
from tantra_data.domain.quantity import Quantity, QuantityError, parse_quantity
from tantra_data.ports.data_feeds import (
    BlockDealsReader,
    CalendarReader,
    ComparisonOp,
    DeliveryReader,
    FilingsReader,
    FundamentalsReader,
    InstitutionalFlowReader,
    MacroReader,
    MarketReader,
    NewsReader,
    ProfileReader,
    ReferenceExporter,
    ScreenCondition,
    ScreenerReader,
    ScreenQuery,
    ScreenResult,
    ScreenValue,
)
from tantra_data.ports.repositories import (
    BlockDealsRepository,
    CalendarRepository,
    DeliveryRepository,
    FilingsRepository,
    FinancialStatementRepository,
    FundamentalRatiosRepository,
    InstitutionalFlowRepository,
    MacroSeriesRepository,
    NewsRepository,
    ProfileRepository,
)
from tantra_data.providers.bhavcopy import (
    BhavcopyRow,
    fetch_bhavcopy,
    parse_bhavcopy,
    read_bhavcopy,
    rows_to_bars,
)
from tantra_data.providers.block_deals import (
    parse_block_deals,
    parse_bulk_deals,
    read_block_deals,
    read_bulk_deals,
    rows_to_deals,
)
from tantra_data.providers.calendar import (
    CalendarRow,
    parse_calendar,
    read_calendar,
    rows_to_events,
    stubbed_calendar_rows,
)
from tantra_data.providers.corporate_actions import (
    parse_corporate_actions,
    rows_to_actions,
    subject_to_action,
)
from tantra_data.providers.delivery import (
    parse_delivery,
    read_delivery,
    rows_to_records,
)
from tantra_data.providers.filings import (
    FilingRow,
    parse_filings,
    read_filings,
    rows_to_filings,
)
from tantra_data.providers.forex_factory import (
    events_to_rows,
    fetch_forex_factory,
    parse_forex_factory,
)
from tantra_data.providers.fundamentals import (
    fetch_fundamentals,
    ingest_fundamentals,
    parse_fundamentals,
    rows_to_ratios,
    rows_to_statements,
)
from tantra_data.providers.institutional_flow import (
    parse_institutional_flows,
    read_institutional_flows,
    rows_to_flows,
)
from tantra_data.providers.macro import (
    MacroRow,
    parse_macro,
    read_macro,
    rows_to_observations,
)
from tantra_data.providers.news import (
    parse_news,
    read_news,
    rows_to_articles,
)
from tantra_data.providers.profile import (
    ProfileRow,
    fetch_symbol_profiles,
    rows_to_profiles,
    yfinance_available,
)
from tantra_data.service.block_deals_reader import PythonBlockDealsReader
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter
from tantra_data.service.calendar_reader import LakehouseCalendarReader
from tantra_data.service.data_service import TantraDataService
from tantra_data.service.delivery_reader import PythonDeliveryReader
from tantra_data.service.filings_reader import LakehouseFilingsReader
from tantra_data.service.fundamentals_reader import PythonFundamentalsReader
from tantra_data.service.institutional_flow_reader import PythonInstitutionalFlowReader
from tantra_data.service.macro_reader import PythonMacroReader
from tantra_data.service.market_reader import LakehouseMarketReader
from tantra_data.service.news_reader import PythonNewsReader
from tantra_data.service.profile_reader import LakehouseProfileReader
from tantra_data.service.reference_exporter import InMemoryReferenceExporter
from tantra_data.service.screener import VectorizedScreener

__version__ = "0.1.0"

__all__ = [
    "VYAAPAAR_AVAILABLE",
    "BhavcopyRow",
    "BlockDeal",
    "BlockDealsReader",
    "BlockDealsRepository",
    "Bonus",
    "CalendarEvent",
    "CalendarReader",
    "CalendarRepository",
    "CalendarRow",
    "ComparisonOp",
    "CorporateAction",
    "CorporateActionRecord",
    "CorporateActionType",
    "CoverageEntry",
    "CoverageLedger",
    "CoverageReport",
    "DeliveryReader",
    "DeliveryRecord",
    "DeliveryRepository",
    "Dividend",
    "DuckDbCalendarStore",
    "DuckDbCorporateActionsStore",
    "DuckDbFilingsStore",
    "DuckDbFinancialStatementStore",
    "DuckDbFundamentalRatiosStore",
    "DuckDbFundamentalsStore",
    "DuckDbMacroStore",
    "DuckDbProfileStore",
    "Filing",
    "FilingRow",
    "FilingType",
    "FilingsReader",
    "FilingsRepository",
    "FinancialRatios",
    "FinancialStatement",
    "FinancialStatementRepository",
    "FundamentalRatiosRepository",
    "FundamentalRecord",
    "FundamentalsReader",
    "InMemoryReferenceExporter",
    "InstitutionalFlow",
    "InstitutionalFlowReader",
    "InstitutionalFlowRepository",
    "InstrumentCoverage",
    "LakehouseBridgeExporter",
    "LakehouseCalendarReader",
    "LakehouseFilingsReader",
    "LakehouseMarketReader",
    "LakehouseProfileReader",
    "MacroObservation",
    "MacroReader",
    "MacroRow",
    "MacroSeriesRepository",
    "MarketReader",
    "NewsArticle",
    "NewsReader",
    "NewsRepository",
    "ParquetCatalog",
    "PeriodType",
    "ProfileReader",
    "ProfileRepository",
    "ProfileRow",
    "PythonBlockDealsReader",
    "PythonDeliveryReader",
    "PythonFundamentalsReader",
    "PythonInstitutionalFlowReader",
    "PythonMacroReader",
    "PythonNewsReader",
    "Quantity",
    "QuantityError",
    "ReferenceExporter",
    "Rights",
    "ScreenCondition",
    "ScreenQuery",
    "ScreenResult",
    "ScreenValue",
    "ScreenerReader",
    "Split",
    "StatementType",
    "SymbolProfile",
    "TantraDataService",
    "VectorizedScreener",
    "__version__",
    "events_to_rows",
    "fetch_bhavcopy",
    "fetch_forex_factory",
    "fetch_fundamentals",
    "fetch_symbol_profiles",
    "from_record",
    "ingest_fundamentals",
    "parse_bhavcopy",
    "parse_block_deals",
    "parse_bulk_deals",
    "parse_calendar",
    "parse_corporate_actions",
    "parse_delivery",
    "parse_filings",
    "parse_forex_factory",
    "parse_fundamentals",
    "parse_institutional_flows",
    "parse_macro",
    "parse_news",
    "parse_quantity",
    "read_bhavcopy",
    "read_block_deals",
    "read_bulk_deals",
    "read_calendar",
    "read_delivery",
    "read_filings",
    "read_institutional_flows",
    "read_macro",
    "read_news",
    "rows_to_actions",
    "rows_to_articles",
    "rows_to_bars",
    "rows_to_deals",
    "rows_to_events",
    "rows_to_filings",
    "rows_to_flows",
    "rows_to_observations",
    "rows_to_profiles",
    "rows_to_ratios",
    "rows_to_records",
    "rows_to_statements",
    "stubbed_calendar_rows",
    "subject_to_action",
    "to_record",
    "yfinance_available",
]
