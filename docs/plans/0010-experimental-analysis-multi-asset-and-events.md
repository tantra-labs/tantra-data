# 0010 — Multi-Asset Datasets (FX, Commodities, Continuous F&O & Spreads) & Event Studies (`tantra-data`)

**Status**: Proposed (2026-11-18) — Track **T20.1**

Master plan: root [`docs/plans/0014-experimental-analysis-statistical-workbench.md`](../../../docs/plans/0014-experimental-analysis-statistical-workbench.md).  
Builds on: [`0009-unified-lakehouse-v3-and-cas-manifest.md`](0009-unified-lakehouse-v3-and-cas-manifest.md) (Lakehouse v3) and [`0007-instrument-master-index-membership.md`](0007-instrument-master-index-membership.md).

---

## 0. Executive Summary & Objective

Extend `tantra-data`'s Lakehouse v3 pipelines beyond NSE Equities/Indices/MFs so `Experimental-Analysis` (`tantra-market`, `tantra-runtime`, `tantra-ui-web`) can run statistical and event-reaction studies across **F&O, Indices, ETFs, Equities, Forex, and Commodities** — including **F&O futures roll/basis spreads and option bid-ask liquidity snapshots** — with deterministic point-in-time corporate & exchange event catalogs.

---

## 1. Multi-Asset Ingestion, Symbology & F&O Spread Continuity

1. **ETFs & Indices Breadth**:
   - Ensure Lakehouse v3 catalogs ETF symbols (`NIFTYBEES`, `BANKBEES`, `GOLDBEES`, `LIQUIDBEES`, sector ETFs) and key Indices (`NIFTY 50`, `BANK NIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `SENSEX`, `INDIA VIX`) across `1d` and `5m` partitions (`2014–2026+`).
2. **F&O Continuous Series, Basis & Bid-Ask Spreads (`derivatives/`)**:
   - Maintain `is_fno` + lot-size/tick-size/expiry attributes in `reference/instruments`.
   - Store near-month vs next-month (`Week+1` / `M+1` vs `M+2`) futures close series and **Spot–Futures Basis Spread** (`F_near - S`, annualized roll yield in bps) so `Week +1 Carry` and F&O Stock/Index return studies distinguish spot returns from tradable futures carry/rollover friction.
   - Store end-of-day / `5m` snapshot top-of-book **bid-ask spread** (`bid`, `ask`, `spread_abs`, `spread_bps`, `oi`, `iv`) for active F&O contracts (`NFO` / `MCX` / `CDS`) to power realistic execution slippage and multi-leg option spread evaluation in `tantra-market-finance`.
3. **Forex & Commodities (`domain=fx`, `domain=commodities`)**:
   - Add Lakehouse v3 partitions and loader targets (e.g., `USDINR`, `EURINR`, `GBPINR`, `JPYINR` + `MCX:GOLD`, `SILVER`, `CRUDEOIL`, `NATURALGAS`, `COPPER` / global spot benchmarks via `yfinance` / broker adapters) with explicit exchange session clocks (`IST` equity `09:15–15:30`, `MCX` `09:00–23:30/23:55`, `FX` `24h`).

---

## 2. Corporate & Exchange Event Projections (Modes 2–3 & Tab 12)

Expose clean Parquet/DuckDB projections consumable by `tantra-market-store`:
1. **Buyback Events**: announcement date, board approval date, record date, tender/open-market type, offer price (`Decimal`), and size.
2. **Quarterly Earnings Dates**: reporting timestamp (`pre-market` vs `post-market` flag so `T+0` gap alignment is exact).
3. **F&O / Index Inclusion & Exclusion History**: circular announcement date, effective date, action (`Inclusion` / `Exclusion`), and benchmark ID.

---

## 3. Verification

- Schema/partition unit tests verifying `Decimal128(18, 4)` alignment across FX, Commodities, and F&O basis/spread columns, session timestamp normalization in IST/UTC, and zero dangling broken aliases in `catalog.duckdb`.
