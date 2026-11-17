# `tantra-data` — Inter-Agent Handoff

## Phase / Slice Status
* **Gap 0024 closed (2026-11-17, T17.5)**: shipped shareholding / promoter-pledge / SEBI-PIT
  insider-disclosure ingestion — three **verified-live** sources (recorded before any provider code):
  BSE `Corp_Shareholding_ng/w` + iXBRL (HTTP 200, 107 quarterly filings / 36 summary categories for
  scrip 500325), NSE `corporate-pledgedata` (HTTP 200, 1,555 company pledge snapshots), NSE
  `corporates-pit?symbol=RELIANCE` (HTTP 200, 20 structured PIT disclosures). `domain/shareholding.py`
  (`ShareholdingPattern`/`PledgeRecord`/`InsiderDisclosure`, exact `Decimal` percentages + int share
  counts, `pledged_pct_of_promoter`), `providers/bse_shareholding.py` / `nse_pledge.py` / `nse_pit.py`
  (live-with-fixture-fallback, `source: fixture` labels, `on_live_error`, `LIVE_VERIFICATION` pinned),
  `store/shareholding.py` `DuckDbShareholdingStore` (`{root}/shareholding.duckdb`, idempotent,
  point-in-time no-look-ahead `patterns_as_of`/`pledges_as_of`), `ports/shareholding.py` +
  `service/shareholding_reader.py`, `cli/ingest_shareholding.py`, bridge
  `export_shareholding`/`export_pledge`/`export_insider_disclosures` →
  `catalog/shareholding|pledge|insider_disclosures/*.parquet` (Decimal128(18,4)). **Stays open &
  documented (never fabricated)**: SAST Reg 29/31 (NSE sast endpoints HTTP 404; announcements feed is
  attachment-only), analyst estimates (no licensable free source), and numeric pledge rows from BSE
  iXBRL (taxonomy exposes only the boolean encumbrance flag — NSE pledgedata covers the numbers). 37
  tests in `tests/test_shareholding.py`; full `pytest` green (372); ruff check + format clean; mypy:
  pre-existing errors only (option_chain.py + option-chain sections of bridge_exporter.py at HEAD), zero
  new on the new modules.
* **Gap 0023 closed (2026-11-17, T17.4)**: shipped `tantra_data/store/mutual_funds.py`
  (`DuckDbMutualFundsStore` at `{root}/mutual_funds.duckdb`: `mf_schemes`, `mf_fund_stats`,
  `mf_holdings`, `mf_nav`, `mf_returns`; exact `DECIMAL` columns). Lookthrough
  (`mf_holdings.isin` ↔ `companies.isin_code`) → weighted portfolio ROCE / P/E / D/E, large/mid/small
  market-cap breakdown (fixed `LARGE_CAP_MIN_CR`/`MID_CAP_MIN_CR` bands), `coverage_pct`; NAV/AUM/TER/
  category/plan/option records; portfolio-overlap queries; disclosure-lag no-look-ahead window; honest
  missing-data degradation (`MutualFundsStoreError` with the `[mutual-funds]` extra / `TANTRA_DATA_DIR`
  provision hint — never fabricated). Connections open/close per op so the consumer's `read_only`
  connection to the same file coexists. 17 tests in `tests/test_mutual_funds_store.py`; full `pytest`
  green; ruff check + format clean; mypy: 21 pre-existing errors at HEAD, zero new. Consumer
  un-skipped: `tantra-examples` `test_mutual_fund_screener_execution` passes against the provisioned
  dummy `mutual_funds.duckdb` (dummy `build_mutual_funds` writes through the store).
* **Gap 0016 option-chain live verification follow-on (T12.3, 2026-11-17)**: retried the live NSE
  feed — still NOT verified from the sandbox. Discovered (from the live page JS) that NSE removed
  the legacy `option-chain-indices` path (now HTTP 404 "Resource not found"); the current frontend
  endpoint is `/api/option-chain-v3?type=Indices&symbol=` which returns `200 {}` (empty) from the
  sandbox — Akamai JS-challenge-gated. Provider hardened in the same change: targets the current
  endpoint, browser-like headers (UA/Accept-Language/Accept/Referer), structured
  `OptionChainLiveError` (status/url/symbol) surfaced via `fetch_option_chain(on_live_error=...)`,
  `LIVE_VERIFICATION` status pinned machine-readably; CLI prints a `WARNING` on fallback. 8 new
  live-vs-fallback contract tests in `tests/test_option_chain_live.py`; full `pytest` 247 green;
  ruff check + format clean; mypy: 24 pre-existing errors at HEAD, zero new.
* **nav_bars Decimal-quantize fix (2026-11-16)**: `loaders/amfi.py::nav_bars` now quantizes the
  NAV to 4dp (`Decimal("0.0001")`) — float binary artifacts previously carried 40+ scale digits
  into the parquet store's decimal128(38,10) columns and failed with "Rescaling Decimal value
  would cause data loss". Un-skipped `test_04_portfolio_optimization`.
* **ex 0003 loaders/quantity (2026-11-15)**: shipped the tantra-examples-gap-0003 loader
  surface: `loaders/amfi.py` (AMFI NAVAll.txt → `parse_navall`/`nav_bars`/`AmfiNavLoader`,
  wheel-optional lazy `Bar`/`InstrumentId`/`ParquetBarStore` imports with
  `tantra[native]` hint), `loaders/nse.py` (`parse_bhavcopy_csv` 3-dialect UDiFF/Sec-Bhavdata/
  legacy + `NseBhavcopyProvider` with `download_session_bhavcopy`), `loaders/yfinance.py`
  (`YFinanceProvider`, `yfinance_available()`-gated `RuntimeError`, Decimal bars),
  `domain/quantity.py` (`Quantity`/`QuantityError`/`parse_quantity` — verbatim Decimal
  crore/lakh/million/percent parser, exported from `tantra_data` `__init__`). 25 new tests in
  `tests/test_loaders.py` (4 wheel-gated skips run under a wheel-enabled venv); full `pytest`
  green (baseline 192 → +25, 4 skipped, exit 0); ruff check + format clean. `AmfiNavLoader`
  store-backed `load()` is exercised by tantra-examples `test_amfi_loader.py`.
* **Gaps 0021/0022 (Python side, 2026-11-14)**: mutual-fund (AMFI `NAVAll.txt`,
  live-verified 14,455 rows) + IPO (NSE `ipo-current-issue`/`all-upcoming-issues`/
  `public-past-issues`/`ipo-detail`, live-verified) ingestion landed:
  providers `providers/mutual_funds.py` + `providers/ipos.py` (parse/fetch/ingest,
  exact `Decimal` CAGR derivation for MFs), domains `domain/mutual_funds.py`
  (`MutualFundMeta`/`MutualFundNav`/`MutualFundReturn`) + `domain/ipos.py`
  (`IpoIssue`/`IpoSubscription`), 5 stores (`adapters/mutual_fund_store.py`,
  `mutual_fund_nav_store.py`, `mutual_fund_return_store.py`, `ipo_store.py`,
  `ipo_subscription_store.py`), readers `service/mutual_funds_reader.py` +
  `service/ipos_reader.py` (`Decimal(18,4)` frames), CLIs
  `cli/ingest_mutual_funds.py` + `cli/ingest_ipos.py`, bridge export
  `export_mutual_funds`/`export_mf_nav`/`export_mf_returns` →
  `catalog/mutual_funds|mf_nav|mf_returns/` + `export_ipos`/
  `export_ipo_subscriptions` → `catalog/ipos|ipo_subscriptions/` (exact
  `Decimal128(18,4)`). 31 new tests (17 MF + 14 IPO); full `pytest` 190 green;
  ruff + mypy clean. Gap docs stay OPEN: Rust projection readers are the
  orchestrator slice, and **MF holdings have no verified live source** (not
  fabricated).
* **Gap 0020 fundamentals (Python side, 2026-11-14)**: yfinance vendor
  `providers/fundamentals.py` (parse/rows/fetch/ingest; `FinancialStatement` +
  `FinancialRatios` domain; exact `Decimal`; live-verified `RELIANCE.NS`), stores
  `DuckDbFinancialStatementStore`/`DuckDbFundamentalRatiosStore`, reader
  `PythonFundamentalsReader` (`Decimal(18,4)` frames), `cli/ingest_fundamentals.py`
  (registered in `cli/__main__.py`), bridge export `export_fundamentals` →
  `catalog/fundamentals/<filing_date>.parquet` + `export_fundamental_ratios` →
  `catalog/fundamental_ratios/<as_of_date>.parquet` (exact `Decimal128(18,4)`),
  `--dataset fundamentals|fundamental-ratios`. 15 tests; full `pytest` 159 green;
  ruff + mypy clean. Gap doc stays OPEN until the Rust `tantra-market-store`
  projection reader + runtime wiring land (orchestrator slice).
* **Tier 6A (COMPLETE)**: Implemented Point-in-Time Financial Lakehouse bounded context.
* **Bridge exporter (T7.2, 2026-11-14)**: `LakehouseBridgeExporter.export_block_deals` writes
  `{root}/catalog/block_deals/<trade_date>.parquet` (decimal128(18,4)) from the
  `DuckDbBlockDealsStore`; CLI `--dataset block-deals|all`; gap 0005 closed upstream. The Rust
  bridge (`tantra-market-store::BlockDealsProjectionReader` + `tantra-runtime-api-rest`
  `/market-data/block-deals`) consumes it.
* **Bridge exporter (2026-11-14, gaps 0017/0018/0019 closed)**: `export_institutional_flows`,
  `export_delivery`, `export_economic_calendar` write
  `{root}/catalog/institutional_flows|delivery|economic_calendar/<date>.parquet` (exact
  `decimal128(18,4)` for money/percent columns, `int64` quantities, ISO-8601 UTC strings for
  timestamps); CLI `--dataset institutional-flows|delivery|economic-calendar`. Consumed by
  `tantra-market-store` `InstitutionalFlowsProjectionReader`/`DeliveryProjectionReader`/
  `EconomicCalendarProjectionReader` + `tantra-runtime-api-rest`
  `FeedMarketReader::with_*` (runtime gap 0007 items 3b/11/12). 14 exporter tests; full
  `pytest` 144 green; ruff clean (note: sandbox ruff 0.17.5 reformatted 13 files whitespace-only
  under the repo's `ruff>=0.4.0` pin).
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
* **NSE option-chain API moved (2026-11-17)**: the legacy `/api/option-chain-indices` endpoint is
  gone (404); the live frontend (`dist/js/sections/option-chain-v3.js`) calls
  `/api/option-chain-v3?type=Indices&symbol=`. The v3 response keeps the same `records`
  envelope (`expiryDates`/`data`/`timestamp`) so the existing parser applies unchanged. The v3 API
  is Akamai JS-sensor-gated (`_abck`/`bm_sz` challenge cookies) — `curl_cffi` gets the cookies but
  the API still returns `{}`, so live verification needs a host that can complete the JS challenge.
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
* Close gap 0024's Rust bridge: `tantra-market-store` projection readers over
  `catalog/shareholding/*.parquet` + `catalog/pledge/*.parquet` +
  `catalog/insider_disclosures/*.parquet` (per gaps 0017/0018/0019), then wire the
  Shareholding / Forecaster / Red-Flag screener categories and insider-deal alerts
  (root plan 0010 F9). SAST Reg 29/31 and analyst estimates stay open-and-documented
  (no structured/licensable free source verified).
* Option-chain live verification remains open: retry `option-chain-v3` from a host that can pass the
  Akamai JS challenge (real browser / residential proxy); the provider stays on the documented,
  never-fabricated `source: fixture` fallback until then (status pinned in `LIVE_VERIFICATION`).
* Close gaps 0020/0021/0022: Rust `tantra-market-store` projection readers over
  `catalog/fundamentals/*.parquet` + `catalog/fundamental_ratios/*.parquet`
  (fundamentals), `catalog/mutual_funds|mf_nav|mf_returns/*.parquet` (mutual
  funds), and `catalog/ipos|ipo_subscriptions/*.parquet` (IPO)
  (`FeedMarketReader::with_fundamentals` style, per gaps 0017/0018/0019), then
  wire `fundamental_ratios`/`financial_statements`, `fund_search` and
  `ipo_calendar`/`ipo_subscription` chat tools and close the gap docs. MF
  `fund_holdings` additionally needs a live-verified holdings source first.
* `tantra-ui-web`: Proceed with gap 0011 (wire sparklines to `GET /bars/{id}`) against verified
  catalog data; for plan 0007 / gap 0024 (Risk tab), either use option (b) local equity curve or
  schedule index ingestion in `tantra-data`.
* Tier 6B: `tantra-adapters` (broker ACL SDK with entry points and paper/mock harness).

## Operating Invariants
* Zero floats: `decimal.Decimal` + `DECIMAL128(18,4)` everywhere in financial columns.
* Tests live in `tests/` only (parallel test directory rule).
