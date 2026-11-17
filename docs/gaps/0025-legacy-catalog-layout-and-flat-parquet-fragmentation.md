# 0025 — Legacy Per-Symbol Catalog & Flat Parquet Layout Fragmentation

**Status**: open

## What's stubbed or hardcoded

`tantra-data/data/` and `tantra_data` adapters currently write and scan three incompatible legacy layouts instead of the unified Lakehouse v3 layout defined in root ADR [`docs/design/0004-data-catalog-lakehouse-layout.md`](../../../docs/design/0004-data-catalog-lakehouse-layout.md):

1. `tantra_data/adapters/parquet_catalog.py` (`_find_parquet_files`, `ParquetCatalog`) scans `<root>/<encoded-instrument>/year=*/*.parquet` and `<root>/catalog/{TF}/{EXCHANGE}/{SYMBOL}/{YYYY}.parquet` (per-symbol yearly files, leaving 120+ separate symbol trees on disk alongside root `<SYMBOL>.NSE.parquet` files).
2. `tantra_data/service/bridge_exporter.py` (`LakehouseBridgeExporter`) emits uncategorized per-date Parquet projections directly under `{root}/catalog/{filings,corporate_actions,block_deals,news,institutional_flows,delivery,economic_calendar,fundamentals,fundamental_ratios,mutual_funds,mf_nav,mf_returns,ipos,ipo_subscriptions}/` without `objects/sha256` immutability or `catalog.duckdb` commit manifests.
3. No `reference/aliases` table resolves historical ticker renames (e.g., `ZOMATO` → `ETERNAL`) into an immutable `instrument_uid`, and `CoverageLedger` (`coverage_ledger.json`) tracks files independently of snapshot commits.

## Why

- Per-instrument directory trees prevent scaling to full NSE/NFO/MCX universes (small-file explosion, slow cross-sectional screening, derivatives directory churn).
- Unversioned mutable `catalog/` directories give backtests no snapshot pinning across corporate-action restatements or nightly compaction.

## How to close it

Implemented via [`tantra-data` plan 0009](../plans/0009-unified-lakehouse-v3-and-cas-manifest.md):

1. Migrate `ParquetCatalog` and `LakehouseBridgeExporter` to write sorted (`(instrument_uid, ts)`), right-sized v3 partitions (`market/`, `derivatives/`, `reference/`, `fundamentals/`, `disclosures/`) into CAS `objects/sha256/` + `catalog.duckdb` manifests.
2. Add `reference/aliases` symbology lookup and commit-scoped coverage queries.
3. Run `tantra-catalog migrate-v3` against existing `data/` fixtures/datasets, verify 100% bar and Decimal parity, and deprecate legacy `*.NSE.parquet` and per-symbol `catalog/{TF}/{EX}/{SYM}/` scan paths.
