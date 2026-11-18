# 0009 — Unified Lakehouse v3 Taxonomy, Symbology Aliases & Git-Style CAS Manifest

**Status**: **Implemented 2026-11-17** (Phases A–C) — `ee802e9` (Phase A domain types + `CatalogRepository` ref surface), `398eeb7` (Phase B CAS object store + `catalog.duckdb` manifest, dual-read), `d64c877` (Phase C `tantra-catalog` CLI: status/log/tag/verify/compact/gc/materialize-views/migrate-v3). 359 tests green (5 env skips). Gap `0025` closed. Cross-repo Rust consumers remain tracked by `tantra-market` gap `0008` + `tantra-runtime` gap `0011` (root plan `0011` Phase 3).

## 0. Executive Summary & Objective

Implement the Python lakehouse storage engine and migration pipeline specified in root ADR [`docs/design/0004-data-catalog-lakehouse-layout.md`](../../../docs/design/0004-data-catalog-lakehouse-layout.md) and root plan [`0011`](../../../docs/plans/0011-unified-data-catalog-lakehouse-v3.md). Unify `ParquetCatalog`, `LakehouseBridgeExporter`, `CoverageLedger`, and ingestion CLIs around:

1. Canonical semantic categorization (`domain/dataset/exchange/asset_class/timeframe/time_bucket`) with right-sized IST partitions and `(instrument_uid, ts)` row-group sorting.
2. Content-addressed immutable Parquet blobs (`objects/sha256/<2-char>/<hash>.parquet`), atomic commits (`commits/<sha>.json`), refs/tags (`refs/{heads,tags}/*`), and an indexed `catalog.duckdb` manifest + symbology alias table.
3. Compaction, GC, optional `views/` Hive hardlink materialization, and one-shot migration of legacy `data/catalog/` and `*.NSE.parquet` files.

## 1. Context & Gaps Covered

- Closes [`tantra-data` gap 0025](../gaps/0025-legacy-catalog-layout-and-flat-parquet-fragmentation.md) (retires per-symbol `catalog/{TF}/{EX}/{SYM}/{YYYY}.parquet`, flat `*.NSE.parquet`, and unpartitioned `bridge_exporter` output).
- Supplies the aligned partition convention (`date=YYYY-MM-DD` sorted by `(expiry, strike, option_type, as_of_ts)`) for [`tantra-data` gap 0016](../gaps/0016-intraday-option-chain-snapshot-lakehouse.md).
- Integrates with [`tantra-data` plan 0007](0007-instrument-master-index-membership.md) so `reference/instruments`, `reference/aliases`, `reference/classification`, and `reference/index_membership` live inside the same versioned manifest and provide multi-axis categorization without rigid directories.
- Feeds [`tantra-market` gap 0008](../../../tantra-market/docs/gaps/0008-lakehouse-v3-manifest-and-snapshot-reader.md) and [`tantra-runtime` gap 0011](../../../tantra-runtime/docs/gaps/0011-backtest-catalog-manifest-pinning.md).

## 2. Component Specifications & Phases

### Phase A — Domain & Ports (`tantra_data/domain/`, `tantra_data/ports/`)
- `CatalogRef`, `CommitRecord` (`commit_sha`, `parent_sha`, `tree_hash`, `created_at_utc`, `as_of_ist`, `schema_version`, `summary`), `ManifestEntry` (`domain`, `dataset`, `exchange`, `asset_class`, `timeframe`, `time_bucket`, `object_sha`, `min_ts`, `max_ts`, `row_count`, `sort_key`), and `InstrumentAlias` (`exchange`, `symbol`, `series`, `valid_from`, `valid_to`, `instrument_uid`).
- Extend `CatalogRepository` (`write_bars`, `read_bars`, `resolve_ref`, `commit`, `tag`) with `as_of_ref: str = "main"` while keeping existing call-sites backward-compatible.

### Phase B — CAS Object Store & `catalog.duckdb` Manifest Adapter (`tantra_data/adapters/`)
- **CAS writer (`cas_store.py`):** Write `Zstd` Parquet segments with 100K-row groups, sorted per dataset key, `Decimal128(18, 4)` everywhere, written to `.tmp` sibling, hashed (`sha256`), and atomically promoted to `objects/sha256/<ab>/<hash>.parquet`.
- **Manifest & ref manager (`manifest_store.py`):** Parameterized DuckDB SQL tables (`commits`, `manifest_entries`, `refs`, `instrument_aliases`); fast-forward `refs/heads/main` only after schema + monotonicity + Zero-Floats verification passes on `refs/heads/staging/<pipeline>`.
- **Dual-read `ParquetCatalog` & `LakehouseBridgeExporter`:** Resolve via `catalog.duckdb` manifest first, falling back to legacy v1/v2 paths only until migration completes; migrate `LakehouseBridgeExporter` projections (`filings`, `corporate_actions`, `block_deals`, `news`, `institutional_flows`, `delivery`, `economic_calendar`, `fundamentals`, `fundamental_ratios`, `mutual_funds`, `mf_nav`, `mf_returns`, `ipos`, `ipo_subscriptions`) into `fundamentals/` and `disclosures/` v3 partitions committed atomically.

### Phase C — CLI, Compaction & Migration (`tantra_data/cli/`)
- `tantra-catalog` CLI (`cli/catalog.py`):
  - `status`, `log`, `tag <name> [commit]`, `verify [--ref main]`
  - `compact`: merge intraday `part-*` flush segments into sorted monthly/yearly objects, emit a new commit without touching blobs pinned by existing `refs/tags/*`.
  - `gc --dry-run`: prune unreferenced `objects/sha256/*` not reachable from any `refs/{heads,tags}/*`.
  - `materialize-views`: create read-only hardlinks/symlinks under `views/` for external SQL/Parquet inspection tools.
  - `migrate-v3`: ingest legacy `data/*.NSE.parquet`, `data/catalog/{1D,5M}/...`, and flat bridge projections into v3 CAS and verify bar counts + OHLCV decimal sums match 100%.

## 3. Test Plan (TDD)

In `tests/` only (no network):
- `tests/test_cas_manifest.py`: content deduplication, atomic staging commit rollback on failed invariant check, ref/tag pinning (`as_of` time-travel), GC reachability safety.
- `tests/test_row_group_pruning.py`: verify `(instrument_uid, ts)` sort order writes distinct min/max row-group bounds and prunes single-symbol scans across multi-symbol yearly/monthly files.
- `tests/test_alias_continuity.py`: verify rename alias resolution (`ZOMATO` → `ETERNAL`) across ex-date boundaries without data duplication.
- `tests/test_migrate_v3.py`: migrate fixture `catalog/1D` + `5M` + `*.NSE.parquet` + bridge projections and assert exact `Decimal` equality (`open`/`high`/`low`/`close`/`volume`) before vs. after.

## 4. Dependencies & Tier Notes

L6 Python (`tantra_data`: PyArrow, Polars, DuckDB, stdlib). No dependency on Rust crates required (`tantra-market-store` reads the shared on-disk CAS + commit JSON / DuckDB contract independently per [`tantra-market` gap 0008](../../../tantra-market/docs/gaps/0008-lakehouse-v3-manifest-and-snapshot-reader.md)).
