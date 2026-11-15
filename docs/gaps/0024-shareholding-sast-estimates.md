# 0024 — Shareholding, Pledge, SAST/Insider & Analyst-Estimate Ingestion

**Status**: open

## What's stubbed or hardcoded

Nothing is stubbed; the data does not exist anywhere in Tantra: per-stock quarterly shareholding
pattern (promoter / FII / DII / MF / public), promoter pledge, SAST and insider disclosures, and
analyst estimates. Only aggregate FII/DII flows (closed gap 0013) and block/bulk deals (closed gap 0005) exist.

## Why

Blocks the Shareholding, Forecaster and business/financial Red-Flag screen categories and insider-deal
alerts in the research taxonomy ([`trendlyne-screener-alerts-mcp.md`](../../../docs/research/trendlyne-screener-alerts-mcp.md), F9;
root roadmap [`0007`](../../../docs/plans/0007-screener-taxonomy-and-alerts.md)). Referenced by
[plan 0008](../plans/0008-screen-dsl-catalog-qvm.md).

## How to close it

1. Verify a free machine-readable source (NSE/BSE shareholding-pattern filings, SAST Reg. 29/31
   disclosures, PIT insider filings); record verification before any provider code. No fabricated data.
2. Domain + store + reader: `ShareholdingPattern`, `PledgeRecord`, `InsiderDisclosure`; point-in-time by
   filing date; Decimal percentages.
3. Parquet projection export for Rust readers (bridge pattern of gaps 0017–0019).
4. Analyst estimates: only if a licensable source exists; otherwise keep this part open and documented.
5. Tests in `tests/` with recorded fixtures; close and index in `docs/gaps/README.md`.
