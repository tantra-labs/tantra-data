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

---

## Status — Python side LANDED (2026-11-14) — gap stays OPEN

The Python ingestion slice of this gap is complete and live-verified; the gap
stays **OPEN** because (a) the Rust `tantra-market-store` projection reader +
`tantra-runtime-api-rest` wiring are a separate orchestrator slice, and (b) the
**holdings** half of this gap has **no verified live source yet**.

### What landed (Python side)

* **Vendor (live-verified: yes)** — AMFI daily NAV dump
  `https://www.amfiindia.com/spages/NAVAll.txt` (official, free, machine-readable,
  no cookie challenge). Verified live 2026-11-14: 14,455 data rows, semicolon
  format `Scheme Code;ISIN Growth;ISIN Div Reinvestment;Scheme Name;Plan;Option;
  Net Asset Value;Date`, section headers for fund category + AMC.
* **Provider** — `tantra_data/providers/mutual_funds.py`:
  `parse_nav_all` (deterministic, handles the current 8-field and a legacy
  7-field repurchase/sale format, skips non-numeric codes and non-positive NAVs
  — never fabricated), `rows_to_meta` / `rows_to_navs`, exact-`Decimal`
  `derive_cagr` (annualized growth over the observed NAV span), `derive_returns`,
  `fetch_mutual_funds`, `ingest_mutual_funds` (returns derived from accumulated
  store history).
* **Domain** — `tantra_data/domain/mutual_funds.py`: frozen self-validating
  `MutualFundMeta` (scheme code/name/category/ISIN/amc/plan/option + as-of date),
  `MutualFundNav` (scheme code, nav date, exact-`Decimal` NAV + repurchase/sale
  price when the vendor supplies them), `MutualFundReturn` (exact-`Decimal`
  `cagr_1y`, `expense_ratio` where the vendor supplies it — the current AMFI dump
  does not carry it, so it is `None`, never fabricated). Zero floats throughout.
* **Stores** — `adapters/mutual_fund_store.py`,
  `adapters/mutual_fund_nav_store.py`, `adapters/mutual_fund_return_store.py`
  (DuckDB, DELETE-then-INSERT idempotent upserts, `as_of_date <= as_of` /
  date-window queries). Ports: `MutualFundMetaRepository` /
  `MutualFundNavRepository` / `MutualFundReturnRepository` +
  `MutualFundReader` in `ports/`.
* **Reader** — `service/mutual_funds_reader.py`: `PythonMutualFundReader` with
  `MUTUAL_FUND_SCHEMA` / `MF_NAV_SCHEMA` / `MF_RETURN_SCHEMA`
  (`pl.Decimal(18, 4)` for every NAV / return column).
* **CLI** — `cli/ingest_mutual_funds.py` (registered in `cli/__main__.py`);
  bridge export `export_mutual_funds` → `{root}/catalog/mutual_funds/
  <as_of_date>.parquet`, `export_mf_nav` → `{root}/catalog/mf_nav/<nav_date>.
  parquet`, `export_mf_returns` → `{root}/catalog/mf_returns/<as_of_date>.
  parquet` (exact `pa.decimal128(18, 4)`); `--dataset mutual-funds|mf-nav|
  mf-returns` in `cli/export_bridge.py`.
* **Live smoke test (2026-11-14)**: `ingest-mutual-funds` ingested
  14,214 schemes / 14,214 NAV rows / 14,214 derived-return records from the live
  dump; `export-bridge --dataset mutual-funds|mf-nav|mf-returns` wrote exact
  `decimal128(18, 4)` parquet.
* **Tests** — `tests/test_mutual_funds_ingestion.py`: 17 tests. Full suite
  190 green; `ruff check .`, `ruff format --check .`, `mypy tantra_data/` clean.

### HOLDINGS — NOT ingested (no verified live source)

Free machine-readable fund-house / aggregator holdings disclosures did **not**
verify live during this task: the paywalled aggregators and disclosure formats
checked were not reliably machine-readable. **No holdings were fabricated.** The
`fund_holdings` tool remains `PortError::unavailable` citing this gap. A future
slice must pick and live-verify a holdings source before holdings can be ingested.

### Immediate Next Step (orchestrator)

Rust `tantra-market-store` mutual-fund projection readers over
`catalog/mutual_funds/*.parquet` / `catalog/mf_nav/*.parquet` /
`catalog/mf_returns/*.parquet` (`FeedMarketReader::with_mutual_funds` style, per
gaps 0017/0018/0019), wire `fund_search` (and later `fund_holdings`, once a
holdings source is verified) and close the gap doc.
