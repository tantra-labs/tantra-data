# 0031 — No provenance/lineage and no ingest orchestration, alerting or health surface

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: deployment · **Severity**: high
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

- `tantra_data/adapters/manifest_store.py:102-118` — the commit payload carries only parent/tree/time/as_of/schema_version/summary: no source URL, fetched-at, source checksum, tool version or run id. `ManifestEntry` (`tantra_data/adapters/manifest_store.py:131-153`) has no source field.
- No scheduling, packaging or telemetry in `tantra_data/`: a search for `cron|schedul|Dockerfile|systemd|prometheus|sentry` finds only doc mentions, and `logging` is not imported by the providers or CLIs. CLIs `print(...)` results (e.g. `tantra_data/cli/catalog.py:484-486`).
- `pyproject.toml` `[project.scripts]` exposes only `tantra-catalog` and `tantra-coverage`; the other 9 ingest CLIs (`tantra_data/cli/ingest_*.py`, `export_bridge.py`) run as `python -m` with ad-hoc flags and non-standard exit codes.
- `data/coverage_ledger.json` (data root) is a flat JSON file mutated by `tantra_data/cli/coverage.py`, not transactional.

## Why

Nobody can answer "which fetch produced object X", which audit retention needs. A silently failed cron leaves stale data feeding strategies with no alert. Config is also fragmented (`configs/catalog.toml`, `TANTRA_DATA_DIR` hint at `tantra_data/store/mutual_funds.py:126`, per-CLI flags) with no single settings object.

## How to close it

1. Add a `provenance` JSON block (source, url, fetched_at_utc, http status/etag, run_id, git sha of tantra-data) to the commit payload, and a `runs` table in `catalog.duckdb`.
2. Unify the ingest CLIs under one `tantra-ingest <dataset>` console script with structured JSON logging, stable exit codes, and a `--metrics-textfile` / Prometheus pushgateway output.
3. Replace `coverage_ledger.json` with a transactional store (or an atomic write plus lock).
4. Scheduling side: define the job in `tantra-runtime-jobs` (or a Kubernetes CronJob) that calls `tantra-ingest` and alerts on non-zero exit and on a freshness SLA (coverage-ledger age); tracked in `tantra-runtime/docs/plans/0012-api-design-review-2026-11.md` R6 and root plan 0022.
5. Introduce a single settings object / env contract (`TANTRA_DATA_*`) for the CLIs.

**Verification**: golden test of the commit payload including provenance; exit-code table test for `tantra-ingest`; test that a failed fetch yields a non-zero exit and a `runs` row. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/`.

## Related

- [0030](0030-ingestion-robustness.md) (produces the fetch metadata), [0029](0029-gc-reachability-and-grace.md) (retention policy).
- `tantra-runtime/docs/plans/0012-api-design-review-2026-11.md` R6; root plan 0022; root gap 0028 (configuration contract).
