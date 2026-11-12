# Plan 0005 — tantra-data → Rust Data Bridge

## Objective

Close gaps 0007 and 0008: expose the Python-side filings and corporate-actions datasets to
Rust consumers through a single `tantra-data` → Rust FFI/Arrow bridge, unblocking the chat
agent's `filings_search` and `corporate_actions` tools.

## Gaps Tracked by This Plan

| Gap | Title | Severity |
|-----|-------|----------|
| [0007](../gaps/0007-filings-rust-bridge.md) | Filings data is Python-side only — no Rust consumer bridge | Medium |
| [0008](../gaps/0008-corporate-actions-rust-bridge.md) | Corporate-actions data is Python-side only — no Rust consumer bridge | Medium |

## Implementation Phases

1. Land the `tantra-data` → Rust bridge: Arrow zero-copy record batches over the existing
   DuckDB/Parquet lakehouse (the same mechanism `tantra-ffi` uses for the `tantra` Python
   wheel in the other direction), or expose a machine-readable projection the runtime can
   ingest.
2. Surface the filings dataset (`Filing`/`FilingType`) through it, keeping the exact-`Decimal`
   record shape and point-in-time visibility semantics (`filing_date <= as-of date`).
3. Surface the corporate-actions dataset (`Split`/`Bonus`/`Dividend`/`Rights`) through it.
4. Replace `FeedContentReader::filings` / `FeedMarketReader::corporate_actions` stand-ins in
   `tantra-runtime` with real reads.

> One bridge, multiple datasets — do not build a corporate-actions-only second bridge.

## Exit Criteria

- A Rust consumer can read filings and corporate actions through the bridge.
- `tantra-runtime` chat adapters return real data (no `PortError::unavailable`).
- `pytest -q`, `ruff check .`, `ruff format --check .`, `mypy tantra_data` pass.
- `docs/gaps/README.md` marks 0007 and 0008 closed with closing commits.

## Related

- `tantra_data/service/filings_reader.py` — `LakehouseFilingsReader`.
- `tantra_data/adapters/duckdb_store.py` — `DuckDbCorporateActionsStore`.
- `tantra-ffi` — Arrow zero-copy memory bridge mechanism.
- `tantra-agent` plan 0006 — `filings_search`/`corporate_actions` chat tools.
- [0009](../gaps/0009-corporate-actions-ingestion.md) — companion ingestion gap (a bridge
  over an empty table still answers "no actions recorded").
