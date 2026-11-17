# 0016 — Intraday Option-Chain Snapshot Ingestion & Point-in-Time Series

**Status**: open

## What's Missing

`tantra-data` persists bars, filings, corporate actions, macro series, and
profiles — but **no intraday option-chain snapshots**. The Rust side
(`tantra-market` gaps 0001/0002, closed) reads live chain/OI and can store
per-day OI Parquet, yet there is no lakehouse-grade, point-in-time series of
strike-wise option positioning for **backtesting** Elephant Edge (EE+) level
fusion:

- Per-strike call/put **OI, ΔOI, IV, delta, gamma** at intraday timestamps
  (e.g. every 5 minutes during the session), per expiry.
- **Gamma-exposure (GEX) / gamma-flip historical series** (needed to test the
  long-gamma vs short-gamma regime gate on historical days).
- Point-in-time **as-of** reads so a backtest at 10:15 only sees the chain
  snapshot as of 10:15 (lookahead-free).

## Why

`tantra-strategies` plan 0001 and `tantra-market` gap 0005 consume these to
compute OI walls, ΔOI build-up, and the gamma flip for **historical** sessions.
Without the snapshots, the strategy can only fuse levels on live data; the
backtest would have no options-confluence leg — defeating research-live parity
for the options-aware signal rules.

## How to Close It

1. **`OptionChainSnapshot` store** (DuckDB + Parquet per root ADR [`0004`](../../../docs/design/0004-data-catalog-lakehouse-layout.md)):
   `derivatives/option_chain/exchange={EX}/underlying={SYM}/date=YYYY-MM-DD/`
   (one session-day partition sorted by `(expiry, strike, option_type, as_of_ts)`
   with 50K–100K row groups instead of per-5-minute tiny `YYYY-MM-DD-HHMM.parquet`
   files; live intraday append flushes compact post-close via
   [`tantra-data` plan 0009](../plans/0009-unified-lakehouse-v3-and-cas-manifest.md)),
   keyed by `(as_of, instrument, expiry, strike, option_type)`, `Decimal128(18, 4)`
   columns for OI / ΔOI / IV / delta / gamma — unifying with the Rust
   `ParquetOpenInterestStore` migration tracked in [`tantra-market` gap 0008](../../../tantra-market/docs/gaps/0008-lakehouse-v3-manifest-and-snapshot-reader.md).
2. **Ingestion pipeline**: poll the NSE public option-chain source during
   market hours (configurable interval, default 5 min), upsert snapshots,
   stamp `as_of` (UTC + IST session mapping). Intraday OI is provisional;
   reconcile against the official EOD file.
3. **Point-in-time reader**: `OptionsChainReader` port + DuckDB as-of join
   (`as_of <= reference_ts`) so `tantra-strategies`/`tantra-market` gap 0005
   reads only the chain known at each backtest bar.
4. **GEX series**: materialize per-strike GEX + net + flip per snapshot
   (Black-76 gamma × OI × lot size × spot scaling, exact Decimal) as a derived
   Parquet series under `derivatives/gex/exchange={EX}/year=YYYY/month=MM` for
   the regime gate.
5. Tests in `tests/` (TDD): as-of determinism, snapshot upsert idempotency,
   GEX golden vectors, expiry rollover handling.

## Verification

`pytest -q` green; a seeded week of snapshots yields the correct as-of series
(no lookahead); GEX per strike matches the golden vectors; the DuckDB/Parquet
layout matches ADR `0004` (`derivatives/option_chain/...`) and Rust-side naming.
