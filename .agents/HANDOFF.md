# `tantra-data` — Inter-Agent Handoff

## Phase / Slice Status
* **Tier 6A (COMPLETE)**: Implemented Point-in-Time Financial Lakehouse bounded context.
* **Docs overhaul (2026-11-12)**: the historical `docs/roadmap/` was dismantled into
  `docs/design/`, `docs/gaps/`, `docs/plans/` and the whole set rewritten to match the source.
  - Design: `architecture.md` now inventories the actual 7-domain / 7-reader / 7-store /
    5-provider / 8-service layout (calendar, filings, macro, profile included).
  - Gaps: **numbering collision fixed** — open gaps 0001/0002/0003 (calendar,
    fundamentals, calendar-stub) collided with closed gaps 0001/0002/0003; renumbered to
    **0010/0011/0012**. **0011 (symbol profile) closed** (yfinance implementation,
    `261d646`); 0010 (calendar real source) and 0012 (calendar stub) stay open.
  - Plans: new active plans **0003–0006** group the open gaps (calendar/reference,
    content/disclosure, Rust bridge, lakehouse ingestion).
  - Cross-repo: `tantra-runtime` plan 0003's gap citations updated to the new numbers.
* Package `tantra_data/` with hexagonal layout:
  * `domain/`: `Split`, `Bonus`, `Dividend`, `Rights` (frozen, self-validating, `Decimal`-exact),
    `FundamentalRecord` (filing/period dates with look-ahead guard), `CoverageLedger` (gap discovery),
    `InstrumentCoverage`, `CoverageReport`.
  * `ports/`: `CorporateActionsRepository`, `FundamentalsRepository`, `CatalogRepository`,
    `DataService` protocols + canonical `BAR_SCHEMA`.
  * `adapters/`: `DuckDbFundamentalsStore`, `DuckDbCorporateActionsStore`,
    `ParquetCatalog` (supporting both Hive layout and runtime `{root}/catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`
    layouts with exact `DECIMAL128(18,4)` prices).
  * `providers/`: NSE `cm` bhavcopy parser + `rows_to_bars` mapper.
  * `service/`: `TantraDataService.adjusted_history()`, `coverage()`, `report_coverage()`.
  * `cli/`: `python -m tantra_data.cli` coverage reporting tool (`tantra-coverage`).
* Zone 3 configs: `configs/catalog.toml`. CI: `.github/workflows/ci.yml`.
- `docs/migration-guide.md` decimated: historical migration map moved to
  `docs/design/code-migration-map.md`; verification commands moved to
  `docs/roadmap/migration-verification.md`; original `migration-guide.md` removed.
* **Gap 0001 (CLOSED)**: Ran coverage-ledger reporting for `tantra-ui-web` instruments. Confirmed
  equity coverage (2014–2026, >3,150 bars each) in the deployed lakehouse catalog (`RELIANCE`,
  `TCS`, `HDFCBANK`, `INFY`, `ICICIBANK`), unblocking gap 0011 sparklines. Identified that benchmark
  indices (`NIFTY50`) are missing from cash bhavcopies, requiring index ingestion before plan 0007
  option (a) can use real benchmark bars.

## Decisions & Discoveries
* `pyproject.toml`: Made `tantra>=0.1.0` optional under `[project.optional-dependencies].native`
  (consistent with `_compat.py` runtime fallback); added console script `tantra-coverage`.
* `ParquetCatalog`: Seamlessly reads both Hive layout (`{root}/<instrument>/year=YYYY/*.parquet`) and
  the runtime catalog layout (`{root}/catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`), normalizing
  both nanosecond `int64` and temporal timestamps into microsecond UTC datetimes, and float/double
  prices into canonical `DECIMAL(18, 4)`.
* NSE cash equity bhavcopies (`sec_bhavdata_full_*.csv`) do not contain benchmark index levels
  (`NIFTY 50`), which live in separate index bhavcopies (`ind_close_all_*.csv`).
* **Gap 0001 (calendar) implemented with stubbed data** — `CalendarEvent` domain,
  `CalendarRepository`/`CalendarReader` ports, `DuckDbCalendarStore` adapter,
  `LakehouseCalendarReader` service, stubbed CSV provider (`stubbed_calendar_rows`).
  Stubbed data is a stopgap pending an external vendor decision — tracked by
  `docs/gaps/0012-calendar-stubbed-data.md`. Tests in `tests/test_calendar.py`.
* **Gap 0002 (symbol profile) implemented with yfinance** — `SymbolProfile` domain,
  `ProfileRepository`/`ProfileReader` ports, `DuckDbProfileStore` adapter,
  `LakehouseProfileReader` service, yfinance provider (`fetch_symbol_profiles`,
  degrades gracefully when yfinance is absent). Tests in `tests/test_profile.py`.

## What was implemented (gaps 0001 & 0002)
- **Calendar ingestion** (gap 0001): `CalendarEvent` domain value object
  (`tantra_data/domain/calendar.py`); `CalendarRow`/`parse_calendar` CSV provider
  (`tantra_data/providers/calendar.py`); `CalendarRepository` port +
  `DuckDbCalendarStore` adapter (`tantra_data/adapters/duckdb_store.py`);
  `CalendarReader` port + `LakehouseCalendarReader` service
  (`tantra_data/service/calendar_reader.py`). Data is stubbed/hardcoded pending
  an external vendor decision (see docs/gaps/0012-calendar-stubbed-data.md).
- **Symbol profile ingestion** (gap 0002): `SymbolProfile` domain value object
  (`tantra_data/domain/profile.py`); `ProfileRow`/`fetch_symbol_profiles` yfinance
  provider (`tantra_data/providers/profile.py`); `ProfileRepository` port +
  `DuckDbProfileStore` adapter (`tantra_data/adapters/duckdb_store.py`);
  `ProfileReader` port + `LakehouseProfileReader` service
  (`tantra_data/service/profile_reader.py`). yfinance is optional; degrades
  gracefully when absent.
- **Tests**: 18 new tests (`tests/test_calendar.py` 10, `tests/test_profile.py` 8).
- Full suite `pytest -q`: **100 passed, 0 failed**; `ruff check` + `ruff format --check`
  clean; `mypy` clean on new files.

## What was implemented (plan 0001)
- **Analytical data-feed ports** (`tantra_data/ports/data_feeds.py`): `MarketReader`
  (`get_quote`, `get_candles`, `get_returns`, `get_indicators`), `ScreenerReader`
  (`execute_screen`), `MacroReader` (`get_series`), `ReferenceExporter` (`export`/`resolve`
  emitting `$tantra/ref/<uuid>` URIs), plus screener domain types
  (`ScreenQuery`, `ScreenCondition`, `ComparisonOp`).
- **Implementations** (`tantra_data/service/`): `LakehouseMarketReader` backed by the
  Parquet catalog + `TantraDataService`; `VectorizedScreener` (gt/lt/gte/lte/eq and
  crosses_above/crosses_below vs literal or field, order_by + limit);
  `InMemoryReferenceExporter`; `PythonMacroReader` (empty canonical frame — stub,
  see open gap 0002).
- **Tests**: 16 new tests in `tests/test_data_feeds.py`; full suite `pytest -q`: 45 passed,
  0 failed; `ruff check` + `ruff format --check` clean.

## What was implemented (plan 0002 / gaps 0002 & 0003)
- **Macro series ingestion** (gap 0002 closed): `MacroObservation` domain value object
  (`tantra_data/domain/macro.py`); `MacroRow`/`parse_macro` CSV provider
  (`tantra_data/providers/macro.py`); `MacroSeriesRepository` port +
  `DuckDbMacroStore` adapter (`tantra_data/adapters/macro_store.py`);
  `PythonMacroReader.get_series` now reads from the repository with as-of semantics and
  returns exact `Decimal(18,4)` Polars frames (`tantra_data/service/macro_reader.py`).
- **Filings ingestion** (gap 0003 closed): `Filing`/`FilingType` domain value objects
  (`tantra_data/domain/filings.py`); `FilingRow`/`parse_filings` CSV provider
  (`tantra_data/providers/filings.py`); `FilingsRepository` port +
  `DuckDbFilingsStore` adapter (`tantra_data/adapters/filings_store.py`);
  `FilingsReader` port + `LakehouseFilingsReader` service
  (`tantra_data/service/filings_reader.py`).
- **Tests**: 18 new tests (`tests/test_macro.py` 9, `tests/test_filings.py` 9).
- Full suite `pytest -q`: **63 passed, 0 failed**; `ruff check` + `ruff format --check`
  clean. Both gap docs moved to Closed Gaps; plan 0002 moved to Implemented Plans.

## Immediate Next Step
* `tantra-ui-web`: Proceed with gap 0011 (wire sparklines to `GET /bars/{id}`) against verified
  catalog data; for plan 0007 / gap 0024 (Risk tab), either use option (b) local equity curve or
  schedule index ingestion in `tantra-data`.
* Tier 6B: `tantra-adapters` (broker ACL SDK with entry points and paper/mock harness).

## Operating Invariants
* Zero floats: `decimal.Decimal` + `DECIMAL128(18,4)` everywhere in financial columns.
* Tests live in `tests/` only (parallel test directory rule).
