# 0028 — No compare-and-swap or cross-process lock on refs / `catalog.duckdb`

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

Ref updates are unconditional, and the ref file and the `refs` table are dual-written without atomicity.

- `tantra_data/adapters/manifest_store.py:184-283` — `parent` is whatever the caller passed in; the store never checks it still equals the current `refs/heads/main` before `_set_ref`. Two ingest jobs that both resolve `main` = A and both commit leave last-writer-wins; the other commit is orphaned (lost update).
- `tantra_data/adapters/manifest_store.py:164-170` — `duckdb.connect(catalog.duckdb)` is held open for the object's lifetime. A second process gets a DuckDB file-lock error, or a reader fails while a writer runs. There is no `flock`, `Lock` or retry anywhere in the package.
- `tantra_data/adapters/manifest_store.py:293-305` — `_set_ref` does `os.replace` (no `fsync` of file or directory) and then a separate `INSERT OR REPLACE` into `refs`. A crash between the two leaves file and table disagreeing, and `_ref_sha` (`:307-326`) silently prefers the file.
- `tantra_data/adapters/manifest_store.py:286-291` — `tag()` overwrites an existing tag with no check, so `backtest-YYYY-MM-DD` pinning tags are mutable.

## Why

A nightly ingest running alongside a manual `tantra-catalog compact` (or the Rust writer) silently drops commits. A power loss can leave `main` pointing at a commit whose JSON was never flushed. Mutable tags break reproducible backtests. Running two ingest pods on a shared volume is unsafe (single-writer assumption everywhere).

## How to close it

1. `commit(..., expect_head: str | None)` raises `RefConflict` if the ref moved; callers retry with rebase (re-merge on the new head, see [0027](0027-bridge-export-drops-datasets-from-main.md)).
2. Advisory lock file `{root}/.lock` (`fcntl.flock`) around read-head to set-ref; document the identical protocol for the Rust writer so both sides serialise.
3. `fsync` the file and parent directory around `os.replace`; make the ref file the sole source of truth and treat the `refs` table as a derived cache rebuilt on open.
4. Refuse to overwrite an existing tag unless `force=True`.

**Verification**: multiprocessing race test (two writers, exactly one `RefConflict`); tmp-dir crash-injection test between `os.replace` and the table insert; tag-overwrite test. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/test_cas_manifest.py`.

## Related

- [0027](0027-bridge-export-drops-datasets-from-main.md), [0029](0029-gc-reachability-and-grace.md) (gc must take the same lock).
- Root gap 0018; `tantra-market/docs/gaps/0014-lakehouse-writer-ref-cas.md` (the Rust writer side of the same protocol).
- Plan 0011 R3 (object-store readiness) needs a `cas_ref` conditional-update primitive that this gap defines.
