# 0027 — Bridge export commits a one-entry tree: `main` loses every other dataset

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: critical
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

`ManifestStore.commit` has no snapshot semantics: the new commit's tree contains only the entries passed in.

- `tantra_data/service/bridge_exporter.py:450-470` — `_project_v3` builds a single `ManifestEntry` and calls `self._manifest.commit(..., entries=[entry], parent=parent)`.
- `tantra_data/adapters/manifest_store.py:255-283` — `commit()` inserts ONLY the passed `entries` under the new `commit_sha` and then fast-forwards `refs/heads/main` (`:280-283`). The parent's entries are not carried forward; `parent` only sets `parent_sha`.
- `tantra_data/adapters/manifest_store.py:376-386` — `resolve_partition` selects `manifest_entries WHERE commit_sha = <head>` only, with no parent walk.
- `tantra_data/cli/catalog.py:83-92` — `_entries_for_commit` has the same head-only behaviour.

## Why

Each `export_*` call makes `main` contain just that one dataset. After exporting `delivery`, a reader resolving `main` no longer sees `fundamentals` or `mutual_funds`. Combined with gap [0029](0029-gc-reachability-and-grace.md) (`gc` walks only ref tips), the objects that fell out of `main` become unreachable and eligible for deletion: this is real data loss, not just a visibility bug. Root gap 0018 (manifest/ref contract) assumes snapshot commits; the implementation does not provide them. Closed gap 0025 introduced the manifest but did not define this semantics.

## How to close it

1. **Immediate mitigation, before any `gc` run**: do not run `tantra-catalog gc` against a data root that has been written by `export_*` until step 2 lands. Carry parent entries forward in `ManifestStore.commit` as the smallest fix, with a regression test (step 4).
2. Define commit semantics explicitly as a full snapshot. `ManifestStore.commit` merges the `parent` commit's entries keyed by `(domain, dataset, exchange, asset_class, timeframe, time_bucket)`; the new entries replace matching keys and all other parent entries are retained.
3. Add an `entries_replace_only=False` switch so `compact` can still write a replacement tree intentionally.
4. Repair existing roots: add a one-off `tantra-catalog` repair that rebuilds a merged head from the commit history (walk `parent_sha`, latest entry per key wins), and write it as a new commit rather than rewriting history.
5. Document the snapshot contract alongside the Rust writer (see `tantra-market/docs/gaps/0014-lakehouse-writer-ref-cas.md`) so both writers agree.

**Verification**: in `tests/test_cas_manifest.py`, add a test committing two different datasets sequentially and assert both resolve at `main`; a hypothesis test for merge idempotence and key-replacement; a golden commit/tree JSON vector. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/test_cas_manifest.py tests/test_catalog_cli.py`.

## Related

- [0029](0029-gc-reachability-and-grace.md) — gc reachability; this gap plus 0029 yields data loss.
- [0028](0028-ref-cas-and-catalog-locking.md) — ref CAS / locking in the same `commit` path.
- Root gap 0018 (lakehouse manifest/ref contract); `tantra-market/docs/gaps/0014-lakehouse-writer-ref-cas.md`.
- Closed gap 0025 (introduced the manifest) and closed gap 0026 (`LakehouseBarReader` reads the head commit).
