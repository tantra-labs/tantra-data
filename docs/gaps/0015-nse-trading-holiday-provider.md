# 0015 — No NSE trading-holiday provider: tantra-data owns the data, runtime seeds its own list

**Status**: closed (T17.1; see the `feat(data): NSE trading-holiday provider + store + bridge` commit — `tantra_data` now owns the reference data and the runtime consumer reads it from the data root, with the seed table kept only as the deterministic offline fallback)

## What's stubbed or hardcoded

Per `tantra-ledger` plan 0005 decision (T2.7, approved 2026-11-13),
**`tantra-data` owns the NSE trading-holiday reference data** and
`tantra-runtime-backtest::NseTradingCalendar` is the **consumer & provider of
`ClearingCalendar`** — it seeds the ledger's
`tantra_ledger_portfolio::ClearingCalendar::with_holidays` from its own
holiday list.

That list is today a hardcoded, year-bundled table inside
`tantra-runtime-backtest/src/calendar.rs` (`seed_standard_holidays`, 2025/2026
entries + Muhurat sessions), **not** a live ingestion path from this repo:

- The domain crate (`tantra-ledger-portfolio`) has correctly stopped
  hardcoding holidays (`ClearingCalendar::new()` is an empty weekend-only
  calendar), so the remaining stub lives here on the data side and in the
  runtime consumer's seed list.
- This repo's existing calendar surface is the **economic** calendar (gaps
  0010/0012); there is no NSE **trading-holiday** provider/reader.

## Why

- Holiday data is reference data: it belongs in a data provider (this repo)
  with a point-in-time store, not frozen into a domain crate or a runtime
  consumer's static list every year.
- A static list silently goes stale (new Muhurat sessions, one-off closures)
  and would drift if copied again.

## How to close it

1. Add an NSE trading-holiday provider (source selection: NSE holiday
   calendar / exchange circulars) in `tantra_data`, with a reader/port +
   DuckDB store (mirroring the economic-calendar pipeline, gap 0010).
2. Feed it to `tantra-runtime-backtest::NseTradingCalendar` (the consumer &
   provider of `ClearingCalendar`), replacing `seed_standard_holidays`, citing
   this gap and `tantra-ledger` plan 0005 in the code and commit message.

## Prerequisite / dependency

- `tantra-ledger` plan 0005 (implemented 2026-11-13, commit `437e6af`) —
  `ClearingCalendar::new()` is already empty; real holidays are injected via
  `with_holidays`. This gap is the last upstream piece for a fully
  data-driven holiday calendar.

## Closed by

1. `tantra_data` now owns the NSE trading-holiday master:
   `domain/trading_holidays.py` (`TradingHoliday`), a live-with-fixture-fallback
   provider (`providers/trading_holidays.py`, NSE `holiday-master?type=trading`
   live source; sandbox observes an HTTP 301 redirect so the bundled recorded
   fixture `tests/fixtures/trading_holidays_2025_2026.json` serves offline —
   `LIVE_VERIFICATION` pins the honest status), a DuckDB store
   (`store/trading_holidays.py`, idempotent per calendar year), the
   `TradingHolidayReader` port + `PythonTradingHolidayReader`, the
   `cli/ingest_trading_holidays.py` ingestion entry, and a
   `catalog/trading_holidays/<year>.parquet` bridge export
   (`service/bridge_exporter.py::export_trading_holidays`) for the Rust side.
2. `tantra-runtime-backtest::NseTradingCalendar` reads that projection from the
   data root (`from_data_root` / `from_data_root_env`, env `TANTRA_DATA_DIR`)
   instead of seeding its own list; `seed_standard_holidays` is kept only as the
   documented offline fallback so backtests stay deterministic when no data is
   deployed (choice noted in `calendar.rs`).
