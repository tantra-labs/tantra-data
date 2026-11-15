# Gaps index

Every time something in this repo is stubbed, mocked, or hardcoded because the real
dependency (a data source, a pipeline, a design decision) isn't wired up yet, it gets a
numbered entry here instead of being left silent. Per root `.agents/rules/gap-tracking.md`
§3, this index also carries gaps whose symptom shows up in a *different* repo but whose root
cause lives here.

Numbering is per-repo, sequential, never reused: closed gaps 0001–0003 (and 0011) keep
their numbers, so new gaps continue from 0004. The original open gaps 0001–0003 collided
with the closed 0001–0003 and were renumbered to 0010–0012 during the 2026-11-12 docs
overhaul.

## Entry template

```markdown
# XXXX — Short title

**Status**: open | closed (link the closing commit/PR)

## What's stubbed or hardcoded
...

## Why
...

## How to close it
...
```

## Open gaps

| [0015](0015-nse-trading-holiday-provider.md) | NSE trading-holiday provider — tantra-data owns the data (ledger plan 0005 T2.7 decision); runtime `NseTradingCalendar` still seeds its own holiday list | `tantra_data` (calendar) | — |
| [0016](0016-intraday-option-chain-snapshot-lakehouse.md) | Intraday option-chain snapshot ingestion (OI/ΔOI/IV/delta/gamma) + point-in-time GEX/flip series — no lakehouse store or as-of reader exists (Elephant Edge EE+) | `tantra_data` (derivatives) | — |
| [0023](0023-mutual-funds-store-surface.md) | `tantra_data.store.mutual_funds` (DuckDbMutualFundsStore) never shipped — the mutual-fund screener's lookthrough store surface is absent; consumer (`screen_mutual_funds`) degrades to its missing-extra error | `tantra_data` (funds) | — |
| [0024](0024-shareholding-sast-estimates.md) | Per-stock shareholding, pledge, SAST/insider disclosures and analyst estimates — no data source (blocks Shareholding/Forecaster/Red-flag screens) | `tantra_data` (reference) | — |

## Closed gaps

| # | Title | Area |
|---|-------|------|
| 0001 | Parquet catalog coverage verified for downstream consumers (`tantra-runtime`'s `/bars` endpoint, `tantra-ui-web`'s Risk tab/sparklines) | `tantra_data` (catalog) |
| 0002 | Macro series ingestion stub (`PythonMacroReader` returns empty) | `tantra_data.service.macro_reader` | Closed in `c08710d` (macro series ingestion behind `PythonMacroReader`) |
| 0003 | Filings ingestion & expanded economic series breadth | `tantra-data` (macro + filings) | Closed in `c08710d` (filings ingestion behind `FilingsReader`/`LakehouseFilingsReader`) |
| 0004 | `data/catalog/` NSE daily bars seeded via ad-hoc copy, not tantra-data's own pipeline | `data/catalog/1D`, `data/catalog/5M` | Closed in `3245b3b` (bhavcopy backfill `cli/ingest_catalog.py`; `providers/bhavcopy.py` tolerates the live NSE header; catalog seeded by tantra-data's own pipeline) |
| 0005 | Block & bulk deal disclosure ingestion | `tantra_data` (reference) | Closed in `3a6b247` (`BlockDeal` domain, `providers/block_deals.py`, `DuckDbBlockDealsStore`, `PythonBlockDealsReader`, `BlockDealsRepository`/`BlockDealsReader` ports) |
| 0006 | News article ingestion | `tantra_data` (reference) | Closed in `7af9a81` (`NewsArticle` domain, Moneycontrol RSS `providers/news.py`, `DuckDbNewsStore`, `PythonNewsReader`) |
| 0007 | Filings data is Python-side only — no Rust consumer bridge | `tantra_data` (filings) | Closed in `tantra-market` `b040f68` + `tantra-runtime` `db60aaa` (`FilingsProjectionReader`, `FeedContentReader::with_filings`) |
| 0008 | Corporate-actions data is Python-side only — no Rust consumer bridge | `tantra_data` (corporate actions) | Closed in `tantra-market` `b040f68` + `tantra-runtime` `db60aaa` (`CorporateActionsProjectionReader`, `FeedMarketReader::with_corporate_actions`) |
| 0009 | Corporate-actions DuckDB store has no automatic ingestion | `tantra_data` (corporate actions) | Closed in `cd9fea6` (NSE `corporates-corporateActions` provider → `Split`/`Bonus`/`Dividend`/`Rights` through `DuckDbCorporateActionsStore`) |
| 0010 | Economic/earnings/dividends/splits/IPO calendar ingestion (real vendor source) | `tantra_data` (calendar) | Closed in `cd9fea6` (vendor selected: Forex Factory weekly JSON feed — verified live; `providers/forex_factory.py` + `cli/ingest_calendar.py`) |
| 0011 | Company fundamentals / symbol profile ingestion (was `0002-company-fundamentals-symbol-profile.md`; renumbered to fix the 0001–0003 collision) | `tantra_data` (profile) | Closed in `261d646` (`SymbolProfile` domain, `ProfileRepository`/`ProfileReader` ports, `DuckDbProfileStore`, `LakehouseProfileReader`, yfinance provider `fetch_symbol_profiles`, tests in `tests/test_profile.py`) |
| 0012 | Calendar data stubbed pending external source decision | `tantra_data` (calendar) | Closed in `cd9fea6` (live Forex Factory ingestion path; `default_calendar_rows()` feeds from the live feed with stub fallback) |
| 0013 | FII/DII daily institutional net-flow ingestion | `tantra_data` (reference) | Closed in `3a6b247` (`InstitutionalFlow` domain, NSE `fiidiiTradeReact` `providers/institutional_flow.py`, `DuckDbInstitutionalFlowStore`, `PythonInstitutionalFlowReader`) |
| 0014 | Delivery percentage / delivery-adjusted volume ingestion | `tantra_data` (reference) | Closed in `7af9a81` (`DeliveryRecord` domain, `providers/delivery.py` over the bhavcopy delivery columns, `DuckDbDeliveryStore`, `PythonDeliveryReader`) |
| 0017 | Institutional-flow data is Python-side only — no Rust consumer bridge | `tantra_data` (bridge_exporter) + `tantra-market-store` projection reader | Closed 2026-11-14 (`export_institutional_flows` + `--dataset institutional-flows`, `catalog/institutional_flows/*.parquet`; `InstitutionalFlowsProjectionReader`; `FeedMarketReader::with_institutional_flows` — runtime gap 0007 item 11) |
| 0018 | Economic-calendar data is Python-side only — no Rust consumer bridge | `tantra_data` (bridge_exporter) + `tantra-market-store` projection reader | Closed 2026-11-14 (`export_economic_calendar` + `--dataset economic-calendar`, `catalog/economic_calendar/*.parquet`; `EconomicCalendarProjectionReader`; `FeedMarketReader::with_economic_calendar` → `macro_series` — runtime gap 0007 item 3b) |
| 0019 | Delivery % / delivery-adjusted volume is Python-side only — no Rust consumer bridge | `tantra_data` (bridge_exporter) + `tantra-market-store` projection reader | Closed 2026-11-14 (`export_delivery` + `--dataset delivery`, `catalog/delivery/*.parquet`; `DeliveryProjectionReader`; `FeedMarketReader::with_delivery` — runtime gap 0007 item 12) |
| 0020 | Company fundamentals / financial statements — no data source anywhere | `tantra_data` (fundamentals) | Closed 2026-11-14 (full chain): yfinance vendor verified live (`d94230c`, statements + point-in-time ratios, exact Decimal), exported to `catalog/fundamentals` + `catalog/fundamental_ratios`; `FundamentalsProjectionReader` (`a8b0c05`); `FeedFundamentalReader::with_fundamentals` (`37db5b8`) — runtime gap 0007 item 14 |
| 0021 | Mutual-fund NAV / holdings data — no data source anywhere | `tantra_data` (funds) | Closed 2026-11-14: AMFI daily NAV dump verified live (`17c5b94`, 14,214 schemes/NAV/return records) → `catalog/mutual_funds` + `mf_nav` + `mf_returns`; `FundsIposProjectionReader` (`9f8b04c`); `FeedFundReader::with_funds` (`f752334`) — runtime gap 0007 item 15. **Holdings**: no free machine-readable source verified; `fund_holdings` keeps its explicit gap citation (documented, never fabricated) |
| 0022 | IPO calendar & subscription data — no data source anywhere | `tantra_data` (primary market) | Closed 2026-11-14: NSE primary-market API verified live (`17c5b94`, 1,492 past issues + 31 subscriptions) → `catalog/ipos` + `catalog/ipo_subscriptions`; `FundsIposProjectionReader` (`9f8b04c`); `FeedIpoReader::with_ipos` (`f752334`) — runtime gap 0007 item 16 |
