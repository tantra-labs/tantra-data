# 0011 — tantra-data design review (2026-11): lakehouse commit/ref/GC integrity, ingestion robustness & provenance

**Status**: Proposed (2026-11-23)
**Scope**: `tantra-data` · **Umbrella**: root `docs/plans/0021-workspace-design-review-2026-11.md`

## 0. Executive Summary & Objective

The review found that the v3 manifest is not yet a safe snapshot store. `ManifestStore.commit` writes only the entries it is given, so one bridge export hides every other dataset from `main` (gap 0027, critical); refs have no compare-and-swap or lock (0028); and `gc` walks only ref tips with no grace period (0029), which turns 0027 into real data loss. Beyond the commit path, ingestion has no shared HTTP layer, retry or provenance (0030, 0031), CAS reads are unverified (0032), `as_of_ist` uses host-local time (0033), and the AMFI NAV path uses floats (0034).

This plan fixes commit/ref/gc integrity first, then hardens ingestion and provenance, then prepares deployment (object store, backups, scheduling).

Non-goals: the dynamic-SQL allow-list is already owned by root `docs/plans/0019-codebase-audit-refactoring-unification-and-wiring.md` P1.6 (R1 only adds one site); Protocol ports (0019 P2.3) and reader wiring (0019 P5.2) stay there.

## 1. Context & Gaps Covered

| Gap | Title | Kind | Severity |
|---|---|---|---|
| [0027](../gaps/0027-bridge-export-drops-datasets-from-main.md) | Bridge export commits a one-entry tree: `main` loses every other dataset | design-flaw | critical |
| [0028](../gaps/0028-ref-cas-and-catalog-locking.md) | No compare-and-swap or cross-process lock on refs / `catalog.duckdb` | design-flaw | high |
| [0029](../gaps/0029-gc-reachability-and-grace.md) | `gc` considers only ref tips, no grace period, races in-flight writes | design-flaw | high |
| [0030](../gaps/0030-ingestion-robustness.md) | Ingestion HTTP layer: no retry/backoff, non-atomic cache, 8 NSE bootstraps | design-flaw | high |
| [0031](../gaps/0031-provenance-scheduling-observability.md) | No provenance/lineage, ingest orchestration, alerting or health surface | deployment | high |
| [0032](../gaps/0032-cas-read-verification.md) | CAS reads do not verify content hash; tree may reference missing objects | design-flaw | medium |
| [0033](../gaps/0033-as-of-ist-clock.md) | `date.today()` stamps `as_of_ist` from host-local time | design-flaw | medium |
| [0034](../gaps/0034-nav-and-catalog-float-leaks.md) | Float leaks at AMFI NAV loader and Parquet normalisation | design-flaw | medium |

Plan-only items (no standalone gap doc):

- R1 — Dynamic SQL allow-list — `tantra_data/store/mutual_funds.py:636` interpolates a column list derived from `DESCRIBE`; same root cause as the known `:746` site. Other `adapters/*_store.py` and `store/shareholding.py:216-320` f-strings use fixed fragments with bound values (not an injection today).
- R2 — Per-dataset DuckDB store boilerplate — 11 `tantra_data/adapters/*_store.py` plus `tantra_data/store/{option_chain,shareholding,fno_spreads,trading_holidays,mutual_funds}.py` each open their own `.duckdb` with their own DDL, `_COLUMNS`, upsert and `where_clause`; `tantra_data/adapters/duckdb_store.py:1-418` repeats the same pattern; connections are plain instance attributes (`tantra_data/adapters/manifest_store.py:166`), not thread-safe.
- R3 — Object-storage readiness — all paths are local `Path` with POSIX rename (`tantra_data/adapters/cas_store.py:69-71`, `:96-117`; `tantra_data/adapters/manifest_store.py:224-237`, `:293-305`); `catalog.duckdb` must be a local file (`:164-170`); each export is written three times (`tantra_data/service/bridge_exporter.py:431-444`) plus `views/` copies (`tantra_data/cli/catalog.py:374-380`); no backup or rebuild command.
- R4 — Cross-repo duplication — screen DSL (`tantra_data/service/screener.py:1-169` vs `tantra-core/src/screener.rs`), holidays (`tantra_data/domain/trading_holidays.py`, `tantra_data/store/trading_holidays.py`, `tantra_data/providers/trading_holidays.py` vs `tantra-core/src/calendar.rs:39`, `tantra-market-finance/src/calendar.rs`, `tantra-ffi/python/tantra/calendar*`), corporate actions (`tantra_data/domain/corporate_actions.py` vs `tantra-ledger-portfolio`, `tantra-market-normalizer`), plus the economic calendar `tantra_data/domain/calendar.py:30`.

## 2. Component Specifications & Phases

### Phase A — Stop the data loss (do first)

1. **Gap 0027** (first, before anything else): snapshot-merge `ManifestStore.commit`; `entries_replace_only` switch for `compact`; repair command for already-damaged roots; regression test. Until this lands, no `tantra-catalog gc` run on exported data roots.
2. **Gap 0029**: parent-chain/retention reachability, 24h grace, dry-run default.
3. **Gap 0028**: `expect_head` / `RefConflict`, `{root}/.lock`, fsync ordering, immutable tags. 0029 reuses the lock.
4. **Gap 0032**: existence check in `commit`, verified reads, fsync before promote.
5. **Gap 0033** and **Gap 0034**: `ist_today(clock)` + `Clock` port; Decimal-only NAV and normalisation.

**Gate**: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/test_cas_manifest.py tests/test_catalog_cli.py tests/test_lakehouse_domain.py`, then `pytest -q`.

### Phase B — Ingestion robustness & de-duplication

1. **Gap 0030**: `providers/http.py` (`HttpTransport`, `NseSession`), atomic cache writes, one bhavcopy downloader, `docs/research/data-source-licensing.md`.
2. **R1**: add `tantra_data/store/mutual_funds.py:636` to the shared allow-list helper delivered by root 0019 P1.6; add a lint rule banning f-string `execute(`.
3. **R2**: generic `DuckDbTable[T]` plus one `DuckDbSession` (cursor-per-call, `threading.Lock`); contract test run against every store through its Protocol port; consider a single `reference.duckdb`.
4. **R4**: designate `tantra-core` types as the contract; generate Python models from the `schemars` schema; golden vectors shared by both languages; delete the Python screener once the wheel exposes `tantra.screener`.

**Gate**: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/`.

### Phase C — Deployment readiness

1. **Gap 0031**: provenance in commit payload and `runs` table; `tantra-ingest <dataset>` entry point with structured logs, exit codes, metrics textfile; scheduling via `tantra-runtime-jobs` / CronJob; single settings object.
2. **R3**: `ObjectStore` Protocol (`put_if_absent`, `get`, `list`, `cas_ref`) with `LocalFs` and `fsspec`/S3 implementations (S3 refs via conditional PUT); `catalog.duckdb` as a derived cache with `tantra-catalog rebuild` and `snapshot`; drop duplicate projection copies once Rust readers use the manifest; retention rule for `objects/` + `commits/`.

**Gate**: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/`, rebuild-from-JSON equality test, fake object-store test.

## 3. Relationship to Other Plans

- Root plan 0019: P1.6 (SQL allow-list), P2.3 (ports), P5.2 (reader wiring) are not duplicated here; R1 extends P1.6, gap 0034 touches P5.2.
- Root plan 0022 (deployment architecture): R3, gap 0031. Root plan 0021: umbrella.
- Root gaps 0018 (manifest/ref contract; gaps 0027-0029, 0032), 0020 (time; gap 0033), 0019 and 0015 (R4).
- `tantra-market/docs/gaps/0014-lakehouse-writer-ref-cas.md`: the Rust writer must follow the same snapshot and lock protocol (0027, 0028).
- `tantra-runtime/docs/plans/0012-api-design-review-2026-11.md` R6: scheduling side of gap 0031.
- Earlier plans 0007 (instrument master) and 0008 (screen DSL) overlap R4.

## 4. Exit Criteria

1. Two sequential exports of different datasets both resolve at `main`; a repair command exists for damaged roots (0027).
2. Two concurrent writers produce exactly one `RefConflict`; a crash-injection test passes (0028).
3. `gc` preserves history-pinned commits and fresh orphans, and deletes nothing by default (0029).
4. Every provider uses `HttpTransport`; retry, truncated-download and licensing-note tests exist (0030).
5. Commits carry provenance and ingest CLIs share one entry point and exit-code table (0031).
6. Corrupt objects are detected on verified reads (0032); no `date.today()` remains (0033); no `float` on NAV or money paths (0034).
7. R1-R4 are implemented or tracked in their upstream gaps, and this plan is removed and indexed per `.agents/rules/gap-tracking.md` §4.
