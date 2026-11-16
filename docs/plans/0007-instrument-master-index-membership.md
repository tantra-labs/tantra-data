# 0007 — Instrument Master, Classification & Point-in-Time Index Membership

**Status**: Proposed

## 0. Executive Summary & Objective

Give Tantra a native instrument master: listed securities with ISIN, NSE industry classification,
index constituents with effective dates (point-in-time, survivorship-bias aware), and the SEBI/AMFI
half-yearly market-cap categorisation. This is the provider for every universe, sector-relative and
cap-bucket screen. Native implementation only; no Trendlyne adapter.

References: research [`trendlyne-screener-alerts-mcp.md`](../../../docs/research/trendlyne-screener-alerts-mcp.md) (F2);
root roadmap [`0010`](../../../docs/plans/0010-screener-taxonomy-and-alerts.md).
Depends on: [tantra-core 0004](../../../tantra-core/docs/plans/0004-classification-and-alert-wire-types.md) (wire types).

## 1. Context & Gaps Covered

Today: `SymbolProfile` carries free-string `industry`/`sector`; there is no instrument master and
no index-constituent store. Universe resolution lives in the `tantra` wheel at
`tantra-ffi/python/tantra/markets/india/universes.py` (`resolve_universe`, `UniverseHistory`,
`UniverseUnavailable`, `alpha30_*`, `banknifty_members`; lifted from the examples mirror when
examples gaps 0004/0006 closed). It covers only a few hand-registered lists (nifty50, Alpha30, Bank Nifty).
This plan supplies the **data-backed** membership history: the wheel's `resolve_universe` should read
`UniverseReader` (or the snapshots it exports) rather than hand-maintained lists (see
[tantra-examples 0005](../../../tantra-examples/docs/plans/0005-categorized-examples.md)).

## 2. Component Specifications & Phases

### Phase A — Domain & ports
- Domain: `Instrument`, `InstrumentClassification` (mirrors core wire strings), `IndexMembership`,
  `CapBucketRecord`; Decimal only.
- Protocols (`tantra_data/ports/`): `InstrumentMasterReader`, `ClassificationReader`,
  `UniverseReader.resolve(name, as_of) -> list[InstrumentId]`. Unwired or unknown universe raises
  `UniverseUnavailable` (explicit, never an empty list).

### Phase B — DuckDB stores (parameterised SQL only, including parquet paths)
- `instruments(instrument, isin, exchange, listing_date, delisting_date, status)`
- `instrument_classification(instrument, as_of, macro_sector, sector, industry, basic_industry)`
- `index_membership(index_id, instrument, from_date, to_date)` — half-open interval, delisted names
  retained so historical universes are survivorship-bias aware.
- `cap_bucket_history(instrument, effective_date, rank, mcap, bucket)` from the half-yearly AMFI list.

### Phase C — Providers (behind the existing HTTP transport port)
- NSE `EQUITY_L` and BSE scrip master (ISIN, series, listing).
- NSE industry classification file; index constituent CSVs (Nifty 50/100/200/500, sectoral, Sensex)
  with effective dates; SEBI/AMFI categorisation list.
- Each provider has `parse_*`, `to_rows`, `ingest_*`; CLI `cli/ingest_master.py`; recorded fixtures.

### Phase D — Bridge
- Export `catalog/instruments`, `catalog/index_membership`, `catalog/classification` parquet
  projections for Rust readers (same pattern as closed gaps 0017–0019).

## 3. Test Plan (TDD)

In `tests/` only; no network (fixtures, fake transport):
- `tests/test_instrument_master.py`, `tests/test_index_membership.py` (as-of boundaries, delisted
  names present in past universes, overlapping-interval rejection), `tests/test_cap_bucket.py`
  (rank cutoffs), `tests/test_universe_reader.py` (`UniverseUnavailable` paths).
- Golden provider parse vectors; Hypothesis: membership query is monotone-consistent with intervals.
- SQL injection regression: path and name arguments are always bind parameters.

## 4. Dependencies & Tier Notes

L6 Python. Consumed by [0008](0008-screen-dsl-catalog-qvm.md),
[tantra-strategies 0002](../../../tantra-strategies/docs/plans/0002-universe-screen-context.md) (via
composition adapter), [tantra-runtime 0005](../../../tantra-runtime/docs/plans/0005-group-screener-alerts.md).
Classification and bucket strings must equal tantra-core wire strings; do not fork semantics.

## 5. Open Questions

- Licensing/terms of NSE index constituent files: verify before shipping live providers.
- Back-fill depth for historical membership (free sources may not go back far); record the gap if so.
