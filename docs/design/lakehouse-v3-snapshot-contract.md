# Lakehouse v3 — Snapshot Commit Contract

**Status**: Canonical (closes `tantra-data` gap 0027; shared with `tantra-market` gap `0014` and root gap `0018`)

## 1. Snapshot Semantics on Commits

Every commit in a v3 lakehouse catalog (`{root}/commits/<commit_sha>.json`, `{root}/trees/<tree_hash>.json`, and `catalog.duckdb`'s `manifest_entries`) represents a **complete catalog snapshot**, never an isolated partial delta.

* **Parent merge (default, `entries_replace_only=False`)**: When `ManifestStore.commit(..., parent=parent)` runs, it loads the authoritative snapshot of `parent` (reading `trees/<parent.tree_hash>.json` when present, falling back to `manifest_entries WHERE commit_sha = parent.commit_sha`), overlays the new batch by **logical partition key**:
  ```text
  EntryKey = (domain, dataset, exchange, asset_class, timeframe, time_bucket)
  ```
  Entries in the incoming batch replace matching `EntryKey` slots; all other `EntryKey` slots from `parent` are carried forward untouched.
* **Empty-batch commits**: Committing `entries=[]` with `parent=parent` and `entries_replace_only=False` records a follow-on snapshot commit containing the exact same entries and `tree_hash` as `parent` (a safe no-op marker).
* **Explicit full replacement (`entries_replace_only=True`)**: Reserved for full-catalog rewrites such as `tantra-catalog compact` (merging legacy Hive `part-*` flush files) or `tantra-catalog repair` (rebuilding a full snapshot head from parent history). Partial pipeline writers (such as `LakehouseBridgeExporter`) MUST leave `entries_replace_only=False`.

## 2. Deterministic Tree Serialization & Hashing

To guarantee reproducible `tree_hash` and `commit_sha` values across Python (`tantra-data`) and Rust (`tantra-market-store` — see `tantra-market/docs/gaps/0014-lakehouse-writer-ref-cas.md`):

1. **Snapshot sort order**: Entries in a tree are sorted strictly by:
   ```text
   (min_ts, max_ts, domain, dataset, exchange or "", asset_class or "", timeframe or "", time_bucket, object_sha)
   ```
2. **Canonical timestamps**: `min_ts`, `max_ts`, and `created_at_utc` MUST be timezone-aware UTC serialized with ISO-8601 `+00:00` offset (`YYYY-MM-DDTHH:MM:SS+00:00`, optional microseconds when non-zero). `as_of_ist` is ISO-8601 `YYYY-MM-DD`.
3. **Tree JSON & hash**:
   * Each entry serializes its 11 fields (`domain`, `dataset`, `exchange`, `asset_class`, `timeframe`, `time_bucket`, `object_sha`, `min_ts`, `max_ts`, `row_count`, `sort_key`) with JSON `null` for `None`.
   * Canonical bytes: UTF-8 JSON with keys sorted lexicographically and no whitespace (`separators=(",", ":")`, `sort_keys=True`).
   * `tree_hash = sha256(tree_canonical_bytes).hexdigest()`.
4. **Commit JSON & hash**:
   * Fields: `as_of_ist`, `created_at_utc`, `parent_sha`, `schema_version`, `summary`, `tree_hash`.
   * `commit_sha = sha256(canonical_commit_json_bytes).hexdigest()`.

## 3. Recovery for Legacy Single-Delta Roots

Historical roots written before snapshot merge landed can recover dropped datasets without rewriting commit history via:

```bash
tantra-catalog repair --root <lakehouse-root>
```

`repair` walks `refs/heads/main` backward through `parent_sha`, replays snapshots oldest-to-newest keyed by `EntryKey` (latest entry per key wins), and appends one new replacement commit parenting the current `main` head. Running `repair` again is a no-op once `main` already matches the merged snapshot.
