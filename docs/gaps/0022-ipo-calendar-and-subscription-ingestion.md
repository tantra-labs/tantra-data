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

---

## Status — Python side LANDED (2026-11-14) — gap stays OPEN

The Python ingestion slice of this gap is complete and live-verified; the gap
stays **OPEN** because the Rust `tantra-market-store` projection reader +
`tantra-runtime-api-rest` wiring are a separate orchestrator slice.

### What landed (Python side)

* **Vendor (live-verified: yes)** — NSE primary-market feeds via a
  cookie-challenged `curl_cffi` chrome-impersonated session (mirrors
  `providers/institutional_flow.py`): `/api/ipo-current-issue`,
  `/api/all-upcoming-issues?category=ipo`, `/api/public-past-issues`, and per-issue
  `/api/ipo-detail?symbol=…&series=EQ|SME`. Verified live 2026-11-14: all four
  endpoints return HTTP 200; the current/upcoming lists were empty at probe time
  (no issue open/booked that Saturday), the past feed returned the full listed
  calendar (~1,492 issues; rows shaped `{companyName, ipoStartDate, ipoEndDate,
  issuePrice, listingDate, priceRange, securityType, symbol}`), and the detail
  endpoint returned `issueInfo.dataList` (Issue Period / Price Range / Lot Size /
  Issue Size) + `activeCat.dataList` (srNo 1=QIB, 2=NII, 2.1=b-NII, 2.2=s-NII,
  3=RII, Total row).
* **Provider** — `tantra_data/providers/ipos.py`: `parse_ipo_issues`
  (deterministic over the documented payload; price-band extraction from raw
  `"Rs.88 to Rs.94"` strings or explicit low/high; ISO and NSE `dd-MMM-yyyy`
  dates; rows without a key / dates / band are skipped — never fabricated),
  `rows_to_issues` / `rows_to_subscriptions`, `fetch_ipo_issues` (live fetch +
  best-effort detail enrichment for open/upcoming and 30-day-recent listings, so
  the past feed is not hammered request-by-request), `ingest_ipos`.
* **Domain** — `tantra_data/domain/ipos.py`: frozen self-validating `IpoIssue`
  (symbol/company/board EQ|SME/status open|upcoming|listed, open/close/listing
  dates, exact-`Decimal` `price_band_low`/`price_band_high`/`issue_price`/
  `lot_size`/`issue_size_cr`, `as_of_date` snapshot) and `IpoSubscription`
  (symbol, `as_of_date`, exact-`Decimal` `qib`/`nii`/`bnii`/`snii`/`rii`/`total`).
  Zero floats throughout.
* **Stores** — `adapters/ipo_store.py`, `adapters/ipo_subscription_store.py`
  (DuckDB, DELETE-then-INSERT idempotent upserts, `as_of_date <= as_of` / status
  filters). Ports: `IpoRepository` / `IpoSubscriptionRepository` + `IpoReader`.
* **Reader** — `service/ipos_reader.py`: `PythonIpoReader` with `IPO_SCHEMA` /
  `IPO_SUBSCRIPTION_SCHEMA` (`pl.Decimal(18, 4)` for every band / lot / size /
  multiple column).
* **CLI** — `cli/ingest_ipos.py` (registered in `cli/__main__.py`); bridge export
  `export_ipos` → `{root}/catalog/ipos/<as_of_date>.parquet` and
  `export_ipo_subscriptions` → `{root}/catalog/ipo_subscriptions/<as_of_date>.
  parquet` (exact `pa.decimal128(18, 4)`); `--dataset ipos|ipo-subscriptions` in
  `cli/export_bridge.py`.
* **Live smoke test (2026-11-14)**: `ingest-ipos` ingested 1,492 issues and
  31 subscription snapshots (recent listings enriched from the detail endpoint);
  `export-bridge --dataset ipos|ipo-subscriptions` wrote exact
  `decimal128(18, 4)` parquet.
* **Tests** — `tests/test_ipos_ingestion.py`: 14 tests (incl. the live-verified
  `activeCat` srNo mapping). Full suite 190 green; `ruff check .`,
  `ruff format --check .`, `mypy tantra_data/` clean.

### Notes

* NSE reports subscription multiples for *listed* issues as `0.00` once the book
  closes; live multiples are available while the issue is open. The provider
  stores exactly what the exchange reports — nothing is derived or fabricated.
* BSE IPO endpoints were probed but returned error pages / no usable JSON, so NSE
  is the sole vendor.

### Immediate Next Step (orchestrator)

Rust `tantra-market-store` IPO projection readers over
`catalog/ipos/*.parquet` / `catalog/ipo_subscriptions/*.parquet`
(`FeedMarketReader::with_ipos` style, per gaps 0017/0018/0019), wire
`ipo_calendar` / `ipo_subscription` chat tools and close the gap doc.
