# 0029 — `gc` considers only ref tips, has no grace period, and races in-flight writes

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

- `tantra_data/cli/catalog.py:143-151` — `_reachable_objects` unions `_entries_for_commit` for each ref tip only. Parent commits are never walked, although `as_of_ref` / commit-sha pinning (closed gap 0026, `LakehouseBarReader`) reads historical commits.
- `tantra_data/adapters/cas_store.py:131-153` — `gc_unreachable` unlinks any blob not in `reachable`, with no mtime grace window.
- `tantra_data/adapters/cas_store.py:113-117` — objects are published to the CAS before the commit that references them (`tantra_data/service/bridge_exporter.py:449` then `:464`), so a concurrent `gc` can delete a just-written, not-yet-committed object.
- `tantra_data/cli/catalog.py:478-488` — `_cmd_gc` takes no lock and has no `--keep-last` / `--older-than` option.

## Why

Today `gc` can delete a fresh object mid-ingest. After [0027](0027-bridge-export-drops-datasets-from-main.md) is fixed, `gc` would still delete objects reachable only from a historical or sha-pinned commit, breaking reproducible backtests. Together with 0027 as it stands, objects dropped from `main` are deleted outright: real data loss. There is also no retention policy for `objects/` + `commits/`, which SEBI-style audit retention needs.

## How to close it

1. Compute reachability by walking the `parent_sha` chain, bounded by a retention window (N commits and/or D days) and always including tagged commits.
2. Skip objects younger than a grace period (default 24h, by mtime) so in-flight writes survive.
3. Take the [0028](0028-ref-cas-and-catalog-locking.md) lock for the mark phase and the sweep.
4. Make `gc` dry-run by default; require an explicit `--apply` flag; add `--keep-last` and `--older-than`.
5. Record the retention policy in `docs/` so it can be cited for audit retention.

**Verification**: tests in `tests/test_catalog_cli.py` — history-pinned commit survives gc; fresh orphan survives; old orphan is pruned; default invocation deletes nothing. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/test_catalog_cli.py tests/test_cas_manifest.py`.

## Related

- [0027](0027-bridge-export-drops-datasets-from-main.md) (critical; fix first), [0028](0028-ref-cas-and-catalog-locking.md), [0032](0032-cas-read-verification.md).
- Root gap 0018; closed gaps 0025 and 0026.
