# 0024 — Shareholding, Pledge, SAST/Insider & Analyst-Estimate Ingestion

**Status**: closed (T17.5)

## What was missing

Per-stock quarterly shareholding pattern (promoter / FII / DII / MF / public),
promoter pledge, SAST and insider disclosures, and analyst estimates did not
exist anywhere in Tantra. Only aggregate FII/DII flows (closed gap 0013) and
block/bulk deals (closed gap 0005) existed.

## Why

Blocks the Shareholding, Forecaster and business/financial Red-Flag screen
categories and insider-deal alerts in the research taxonomy
([`trendlyne-screener-alerts-mcp.md`](../../../docs/research/trendlyne-screener-alerts-mcp.md), F9;
root roadmap [`0010`](../../../docs/plans/0010-screener-taxonomy-and-alerts.md)). Referenced by
[plan 0008](../plans/0008-screen-dsl-catalog-qvm.md).

## Source verification (recorded 2026-11-17, before any provider code)

| Dataset | Source | Sandbox result | Verdict |
|---|---|---|---|
| Shareholding patterns | BSE `api.bseindia.com/BseIndiaAPI/api/Corp_Shareholding_ng/w?scripcode=500325&flag=0&indtype=` | HTTP 200 `application/json`, 107 quarterly filing rows (scrip 500325); latest iXBRL filing `bseindia.com/XBRLFILES/SHPXBRLDataXML/500325_*.html` HTTP 200 `text/html`, 1.27 MB with structured `in-bse-shp` taxonomy facts | **verified live** |
| Shareholding patterns | NSE `corporate-share-holdings-master` (various param shapes) | HTTP 200 but always `{"data":[],"msg":"no data found"}` | failed (no usable payload) |
| Shareholding patterns | NSE `quote-shareholding?symbol=RELIANCE` | HTTP 404 "Resource not found" | failed (dead endpoint) |
| Shareholding patterns | screener.in company page | HTTP 200 HTML (shareholding embedded in HTML, no public CSV endpoint verified) | failed (no machine-readable CSV) |
| Promoter pledge | NSE `api/corporate-pledgedata?index=equities` | HTTP 200 `application/json`, 1,555 companies / 1,558 pledge rows (`shp` quarter end, `broadcastDt`, `percPromoterHolding`, `numSharesPledged`, `percSharesPledged`) | **verified live** |
| Promoter pledge (numeric) | BSE SHP iXBRL | The current taxonomy carries only the boolean "whether promoter shares are encumbered under Pledge" — no numeric pledged quantity | failed (not in structured facts) |
| Insider (PIT) | NSE `api/corporates-pit?index=equities&symbol=RELIANCE` | HTTP 200 `application/json`, 20 structured PIT disclosure rows (name, category, trade/intimation dates, buy/sell quantities + values, before/after holdings) | **verified live** |
| SAST Reg 29/31 | NSE `corporate-sast` / `sast-data` | HTTP 404 "Resource not found" | failed (no structured endpoint) |
| SAST Reg 29/31 | NSE `corporate-announcements` feed | HTTP 200; SAST categories ("Disc. under Reg.30 of SEBI (SAST) Reg.2011", "Disclosure under SEBI Takeover Regulations", …) appear as **attachment-only text** (PDFs), no structured numbers | failed (unstructured) |

Verification notes: the BSE API gate is a TLS-fingerprint check — a bare
`curl` client receives an Akamai `403 Access Denied` shell, while the repo's
`curl_cffi` Chrome-impersonating session gets clean JSON. NSE feeds use the
same `curl_cffi` session pattern.

## What shipped

* **Domain** (`tantra_data/domain/shareholding.py`): `ShareholdingPattern`
  (per-category holding by quarter, point-in-time by `filing_date`, exact
  `Decimal` percentage + int share count), `PledgeRecord` (quarter pledge
  snapshot, exact `Decimal` percentages), `InsiderDisclosure` (SEBI PIT
  disclosure, `BUY`/`SELL`/`PLEDGE` transaction types, exact `Decimal`
  values/percentages), plus `pledged_pct_of_promoter()` (exact red-flag ratio).
* **Providers** (live-with-fixture-fallback, `source: fixture` labels, precise
  `on_live_error` failures, `LIVE_VERIFICATION` statuses pinned machine-readable):
  `providers/bse_shareholding.py` (index + iXBRL parse of the SEBI
  summary-category facts), `providers/nse_pledge.py` (pledgedata parse, `Nil`
  handling where the source reports no value), `providers/nse_pit.py`
  (corporates-pit parse, `"Nil"` before/after holdings mapped to 0 — never
  dropped).
* **Store** (`store/shareholding.py`): `DuckDbShareholdingStore` at
  `{root}/shareholding.duckdb` with `shareholding_patterns`, `pledge_records`,
  `insider_disclosures` tables — idempotent upserts, point-in-time
  `patterns_as_of` / `pledges_as_of` reads (no look-ahead), exact
  `DECIMAL(18,4)` financial columns.
* **Ports** (`ports/shareholding.py`): `ShareholdingReader`, `PledgeReader`,
  `InsiderDisclosureReader` protocols; **readers**
  (`service/shareholding_reader.py`): `PythonShareholdingReader`,
  `PythonPledgeReader`, `PythonInsiderDisclosureReader`.
* **CLI** (`cli/ingest_shareholding.py`): `--dataset shareholding|pledge|insider|all`,
  `--scripcode` (BSE, default 500325), `--symbol` (NSE PIT, default RELIANCE).
* **Bridge** (`service/bridge_exporter.py`): `export_shareholding` →
  `catalog/shareholding/<filing_date>.parquet`, `export_pledge` →
  `catalog/pledge/<quarter_end>.parquet`, `export_insider_disclosures` →
  `catalog/insider_disclosures/<intimation_date>.parquet` (exact
  `Decimal128(18,4)` percentages, `date32` dates); wired into
  `cli/export_bridge.py --dataset shareholding|pledge|insider-disclosures`.
* **Fixtures** (recorded verbatim from the live payloads on 2026-11-17,
  labelled `source: fixture`): `tests/fixtures/shareholding_bse_ixbrl.json`
  (36 summary categories, RELIANCE Sep-2026 quarter),
  `tests/fixtures/nse_pledgedata.json` (12 pledge rows),
  `tests/fixtures/nse_pit_reliance.json` (20 RELIANCE PIT disclosures).

## What stays open (documented, never fabricated)

* **SAST Reg 29/31 disclosures** — no structured free source verified (NSE
  sast endpoints HTTP 404; the corporate-announcements feed carries SAST as
  attachment-only PDF text). Kept open; the `LIVE_VERIFICATION` status of
  `providers/nse_pit.py` records this precisely.
* **Analyst estimates** — no licensable free source verified; kept open per
  the gap spec.
* **Promoter-pledge numeric rows from BSE iXBRL** — the current SHP taxonomy
  exposes only the boolean encumbrance flag; the NSE `corporate-pledgedata`
  feed covers the numeric pledge snapshot.

## Tests

`tests/test_shareholding.py` (37): domain validation + exact `Decimal`
invariants, provider parse tests against the recorded fixtures, live-with-
fallback contracts (`on_live_error` surfacing, never fabricated), DuckDB store
round-trips / idempotency / point-in-time no-look-ahead reads, reader ports,
ingestion CLI, and the three Parquet bridge exports (schema + exactness).
