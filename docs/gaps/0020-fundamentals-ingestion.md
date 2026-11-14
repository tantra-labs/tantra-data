# Gap 0020 — Company Fundamentals / Financial-Statement Ingestion Not Implemented

Severity: Medium · Status: Open · Tracked by: `tantra-agent/docs/plans/0007` Track 3 Phase 6

## What's Missing

No fundamentals data exists anywhere in the workspace: no balance-sheet / income /
cash-flow ingestion and no derived valuation ratios (P/E, P/B, EV/EBITDA, dividend
yield, ROCE, ROE, debt/equity, interest coverage). The chat tools
`fundamental_ratios` and `financial_statements` answer `PortError::unavailable`
citing this gap until a data source is picked and ingested.

## Requirements

1. Pick a vendor for Indian equity fundamentals (e.g. a NSE/BSE corporate-data feed
   or a research vendor) and verify live availability.
2. Ingest quarterly/annual financial statements + a point-in-time ratios view into
   `tantra-data` (`FinancialStatement` domain, DuckDB store, exact `Decimal` — never
   `float`).
3. Add a `tantra-market-store` projection reader (bridge-exporter pattern of gaps
   0007/0008/0017/0019) so the Rust chat tools can reach it.
4. Until then the runtime adapters answer `PortError::unavailable` citing this gap —
   never fabricated ratios.

## Why It Matters

Fundamentals are the valuation half of the research loop: a chat agent asked "is
Reliance cheap or expensive?" must ground its answer in real P/E / ROE / debt
figures, not model priors.
