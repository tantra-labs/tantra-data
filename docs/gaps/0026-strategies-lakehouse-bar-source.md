# 0026 — Lakehouse bar source for `tantra_library`

**Status**: open

## What's stubbed or hardcoded
`tantra-strategies` `tantra_library.bar_source.CsvBarSource` (CSV files confined to `TANTRA_LIBRARY_DATA_ROOT`)
is a stand-in; the library MCP server cannot yet read bars from the lakehouse. Cited in
`tantra-strategies/tantra_library/bar_source.py` (root plan 0016).

## Why
`tantra_library` defines a `BarSource` Protocol `load(ref, *, instrument, interval) -> list[Bar]`; no
lakehouse-backed implementation exists.

## How to close it
Implement a lakehouse v3 Parquet `BarSource` in `tantra_data` (resolving `ref` via `CatalogRepository.resolve_ref`):
Decimal prices (from Decimal128, never float), closed bars only, `ref` pinned to a data commit for
reproducibility, bounded row count, no look-ahead. Wire it into `tantra-library-mcp` as an alternative to
`CsvBarSource` and add tests against a CAS fixture.

## Progress

Provider side implemented: `tantra_data.service.library_bars.LakehouseBarReader` (2026-11-20). Remaining: wire into `tantra-library-mcp` (tantra-strategies).
