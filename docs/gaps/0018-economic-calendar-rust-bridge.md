# Gap 0018 — Economic Calendar Rust Bridge Not Implemented

Severity: Medium · Status: Open · Tracked by: `tantra-agent/docs/plans/0007` Track 2 Phase 5

## What's Missing

Economic-calendar ingestion exists Python-side (Forex Factory weekly JSON
provider, `cli/ingest_calendar.py`, `default_calendar_rows()` — gaps 0010/0012,
closed) but there is **no Rust bridge**: no parquet projection export and no
`tantra-market-store` projection reader, so the chat `economic_calendar` tool
cannot reach the data.

## Requirements

1. Add an economic-calendar projection to `service/bridge_exporter.py`
   (`{root}/catalog/economic_calendar/*.parquet`, exact Decimal128 where
   applicable) mirroring the filings/corporate-actions bridge pattern.
2. Add `EconomicCalendarProjectionReader` in `tantra-market-store`.
3. Wire a `FeedCalendarReader` in `tantra-runtime` to it.
4. Until then the runtime adapter answers `PortError::unavailable` citing this
   gap — never fabricated events.

## Why It Matters

The `economic_calendar` chat tool (plan 0007 Track 2 Phase 5) lets agents answer
"what macro release is due this week / for India" questions.
