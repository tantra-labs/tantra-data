# Gap 0019 — Delivery-% (Delivery-Adjusted Volume) Rust Bridge Not Implemented

Severity: Medium · Status: Open · Tracked by: `tantra-agent/docs/plans/0007` Track 3 and plan 0010 Phase 2

## What's Missing

Delivery % / delivery-adjusted volume ingestion exists Python-side
(`DeliveryRecord` domain, bhavcopy delivery-column provider,
`DuckDbDeliveryStore`, `PythonDeliveryReader` — gap 0014, closed) but there is
**no Rust bridge**: no parquet projection export and no `tantra-market-store`
projection reader, so the chat `delivery_analysis` tool cannot reach the data.

## Requirements

1. Add a delivery projection to `service/bridge_exporter.py`
   (`{root}/catalog/delivery/*.parquet`, exact Decimal128) mirroring the
   filings/corporate-actions bridge (T5.5/T5.6 pattern).
2. Add `DeliveryProjectionReader` in `tantra-market-store`.
3. Wire `FeedMarketReader::delivery_stats` in `tantra-runtime` to it.
4. Until then the runtime adapter answers `PortError::unavailable` citing this
   gap — never fabricated delivery percentages.

## Why It Matters

Delivery-adjusted volume is leg 1 of the composite smart-money signal (agent
plan 0010): a delivery-pct × volume spike relative to its trailing average is a
strong accumulation/distribution tell when cross-validated against filings,
block prints, and OI buildup.
