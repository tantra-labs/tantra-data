# 0023 — `tantra_data.store.mutual_funds` store surface not shipped

**Status**: open

**Area**: `tantra_data` (funds) — `tantra_data.store` package

## What's stubbed or hardcoded

`tantra-examples`' mutual-fund screener (`tantra_examples/screening/mutual_funds.py`
`screen_mutual_funds`) lazy-imports:

```python
from tantra_data.store.mutual_funds import DuckDbMutualFundsStore
```

`tantra_data` has **no `store/` package at all** (`tantra_data/` ships
`adapters`, `cli`, `domain`, `loaders`, `ports`, `providers`, `service`), so the import
raises `ModuleNotFoundError` and the function re-raises its "needs the optional
'tantra-data' package" degradation error. Symptom (observed 2026-11-16):

```
tests/unit/test_screening_cli.py::test_mutual_fund_screener_execution
  → skipped: "tantra_data.store.mutual_funds not shipped yet (tantra-data provider gap)"
```

The AMFI **NAV ingestion** exists (gap 0021 closed: `loaders/amfi.py`, `cli/ingest_mutual_funds.py`,
`catalog/mutual_funds` + `mf_nav` + `mf_returns`), but the *DuckDB store surface* the
screener's lookthrough (holdings → equity fundamentals via `companies.isin_code`,
weighted portfolio ROCE/P/E, overlap) depends on was never shipped.

## Why

`screen_mutual_funds` was ported against a `tantra_data.store.mutual_funds`
module the provider never delivered; the consumer's degradation path (raise a clear
`ModuleNotFoundError` mentioning the `[mutual-funds]` extra) is exercised by
`test_screen_mutual_funds_reports_missing_extra_clearly`, which passes — but the
feature itself is unreachable.

## How to close it

Ship a `tantra_data/store/mutual_funds.py` in `tantra-data` providing
`DuckDbMutualFundsStore` with the schema the consumer queries:

- `mf_holdings.isin` ↔ `companies.isin_code` lookthrough (weighted portfolio ROCE,
  weighted portfolio P/E, market-cap breakdown);
- NAV/AUM/TER/category/plan/option records from the AMFI ingestion (`mf_nav`,
  `mf_returns`, `catalog/mutual_funds`);
- portfolio-overlap queries.

Then `tantra-examples` can drop the degraded skip and the
`test_mutual_fund_screener_execution` test (currently re-gated) can run against the
provisioned `mutual_funds.duckdb` (see `tantra-examples` plan 0004 — the
data-provisioning microservice).

## Related

- tantra-examples plan 0004 (data-provisioning microservice, `mutual_funds` dataset)
- tantra-examples gap 0003 / `TANTRA_DATA_DIR` offline seam
- tantra-data gap 0021 (AMFI NAV ingestion — closed)
