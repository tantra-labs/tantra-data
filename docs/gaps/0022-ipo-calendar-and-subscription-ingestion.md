# Gap 0022 — IPO Calendar & Subscription Data Ingestion Not Implemented

Severity: Medium · Status: Open · Tracked by: `tantra-agent/docs/plans/0007` Track 3 Phase 8

## What's Missing

No primary-market data exists anywhere in the workspace: no upcoming-IPO calendar
(symbol / company / open-close dates / price band / lot size / issue size) and no
live subscription multiples (QIB / NII / RII / total). The chat tools `ipo_calendar`
and `ipo_subscription` answer `PortError::unavailable` citing this gap until a data
source is picked and ingested.

## Requirements

1. Pick a vendor for the NSE/BSE primary-market calendar + subscription feed and
   verify live availability.
2. Ingest issue metadata + subscription snapshots into `tantra-data` (`IpoIssue` /
   `IpoSubscription`-shaped domain, DuckDB store, exact `Decimal` for price bands /
   multiples — never `float`).
3. Add a `tantra-market-store` projection reader (bridge-exporter pattern) so the
   Rust chat tools can reach it.
4. Until then the runtime adapters answer `PortError::unavailable` citing this gap —
   never fabricated issues.

## Why It Matters

A research loop tracking the primary market needs the real upcoming-issue calendar
and live subscription numbers to answer "what's on the IPO calendar this week" and
"how subscribed is issue X".
