# Gap 0021 — Mutual-Fund NAV / Holdings Data Ingestion Not Implemented

Severity: Medium · Status: Open · Tracked by: `tantra-agent/docs/plans/0007` Track 3 Phase 7

## What's Missing

No mutual-fund data exists anywhere in the workspace: no scheme master (AMFI-style
scheme code / name / category), no NAV history, no CAGR returns, no expense ratios,
and no portfolio holdings. The chat tools `fund_search` and `fund_holdings` answer
`PortError::unavailable` citing this gap until a data source is picked and ingested.

## Requirements

1. Pick a vendor (AMFI daily NAV dumps / scheme master; holdings from fund house or
   aggregator disclosures) and verify live availability.
2. Ingest scheme metadata + NAV + holdings into `tantra-data` (`MutualFundMeta` /
   `FundHolding`-shaped domain, DuckDB store, exact `Decimal` for NAV / weights /
   CAGRs — never `float`).
3. Add a `tantra-market-store` projection reader (bridge-exporter pattern) so the
   Rust chat tools can reach it.
4. Until then the runtime adapters answer `PortError::unavailable` citing this gap —
   never fabricated schemes.

## Why It Matters

The research loop answers questions like "which flexi-cap fund has the lowest
expense ratio" and "what does Parag Parikh hold" — both require real scheme and
holdings data, not model priors.
