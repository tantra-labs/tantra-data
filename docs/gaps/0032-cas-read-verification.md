# 0032 — CAS reads do not verify the content hash; the commit tree can reference missing or corrupt objects

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

- `tantra_data/adapters/cas_store.py:123-128` — `read_cas_object` checks `exists()` then `pl.read_parquet(path)`; the sha is never recomputed.
- `tantra_data/adapters/cas_store.py:112-117` — if `target.exists()` the new bytes are discarded and the old file trusted (idempotent dedup), so a truncated or bit-rotted file permanently shadows the correct content.
- `tantra_data/adapters/manifest_store.py:131-153` — `_validate_entries` does not check that `object_sha` exists on disk before `commit`.
- The hash is over Parquet bytes (`tantra_data/adapters/cas_store.py:107-111`), so it depends on pyarrow version and writer settings; identical logical tables written by two library versions (or by the Rust writer) never dedup.
- No `fsync` of the object before it is published and the ref flips.

## Why

Silent corruption of a price object feeds backtests with wrong data and nothing detects it. A commit can reference an object that was never written. There is no write durability guarantee before the ref flips.

## How to close it

1. `read_cas_object(verify=True)` for hot paths (size + sha), with a cheap size check by default and a periodic `tantra-catalog verify --deep`.
2. In `commit`, assert every `object_sha` exists before inserting entries.
3. On dedup, compare size and re-hash on mismatch, replacing a corrupt target.
4. `fsync` the object before `os.replace`.
5. Evaluate hashing a canonical logical form (Arrow IPC) instead of file bytes; coordinate with the Rust writer before changing the address scheme.

**Verification**: corrupt-object fixture (read raises, dedup repairs); commit-with-missing-object test; golden-sha vector for a fixed table. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/test_cas_manifest.py tests/test_catalog_cli.py`.

## Related

- [0028](0028-ref-cas-and-catalog-locking.md) (durability ordering), [0029](0029-gc-reachability-and-grace.md).
- Root gap 0018 (Rust-Python byte-compat fixtures).
