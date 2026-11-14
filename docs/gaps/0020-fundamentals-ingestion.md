# Gap 0020 — Company Fundamentals / Financial-Statement Ingestion Not Implemented

Severity: Medium · Status: Open · Tracked by: `tantra-agent/docs/plans/0007` Track 3 Phase 6

> **Python-side landed (2026-11-14)**: the `tantra-data` Python side of this gap
> is implemented and verified live against `RELIANCE.NS` — yfinance vendor
> (`providers/fundamentals.py`), `FinancialStatement` / `FinancialRatios`
> domain, `DuckDbFinancialStatementStore` / `DuckDbFundamentalRatiosStore`,
> `PythonFundamentalsReader`, `cli/ingest_fundamentals.py`, and the bridge
> export (`export_fundamentals` → `catalog/fundamentals/`, `export_fundamental_ratios`
> → `catalog/fundamental_ratios/`, `--dataset fundamentals|fundamental-ratios`).
> The gap stays open until the Rust `tantra-market-store` projection reader and
> runtime wiring (requirement 3) land.

## What's Missing

No fundamentals data exists anywhere in the workspace: no balance-sheet / income /
cash-flow ingestion and no derived valuation ratios (P/E, P/B, EV/EBITDA, dividend
yield, ROCE, ROE, debt/equity, interest coverage). The chat tools
`fundamental_ratios` and `financial_statements` answer `PortError::unavailable`
citing this gap until a data source is picked and ingested.

## Requirements

1. Pick a vendor for Indian equity fundamentals (e.g. a NSE/BSE corporate-data feed
   or a research vendor) and verify live availability.
   **Done (Python side)**: vendor = yfinance (`Ticker("<SYMBOL>.NS")`); live-verified
   2026-11-14 against `RELIANCE.NS` (FY2026 Total Revenue 10,572,190,000,000 INR,
   Net Income 807,750,000,000, Total Assets 21,781,400,000,000, plus
   `trailingPE`/`priceToBook`/`enterpriseToEbitda`/`dividendYield` in `.info`).
2. Ingest quarterly/annual financial statements + a point-in-time ratios view into
   `tantra-data` (`FinancialStatement` domain, DuckDB store, exact `Decimal` — never
   `float`).
   **Done (Python side)**: `FinancialStatement` (income / balance-sheet / cash-flow
   line items, annual + quarterly, exact `Decimal`, `filing_date >= period_end_date`
   anti-look-ahead guard) and `FinancialRatios` point-in-time snapshot (P/E, P/B,
   EV/EBITDA, dividend yield from the vendor; ROCE, ROE, debt/equity, interest
   coverage derived with exact `Decimal` arithmetic from the latest annual
   statement, falling back to vendor values — never fabricated). Stored in
   `DuckDbFinancialStatementStore` / `DuckDbFundamentalRatiosStore`; read via
   `PythonFundamentalsReader` (canonical `Decimal(18, 4)` Polars frames);
   `cli/ingest_fundamentals.py` fetches live.
3. Add a `tantra-market-store` projection reader (bridge-exporter pattern of gaps
   0007/0008/0017/0019) so the Rust chat tools can reach it.
   **Pending — separate orchestrator slice.** The Python projection side is in
   place: `export_fundamentals` writes `{root}/catalog/fundamentals/<filing_date>.parquet`
   (per filing date, point-in-time) and `export_fundamental_ratios` writes
   `{root}/catalog/fundamental_ratios/<as_of_date>.parquet`, both exact
   `Decimal128(18, 4)`; `export_bridge.py --dataset fundamentals|fundamental-ratios|all`.
4. Until then the runtime adapters answer `PortError::unavailable` citing this gap —
   never fabricated ratios.

## Why It Matters

Fundamentals are the valuation half of the research loop: a chat agent asked "is
Reliance cheap or expensive?" must ground its answer in real P/E / ROE / debt
figures, not model priors.
