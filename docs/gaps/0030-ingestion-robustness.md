# 0030 — Ingestion HTTP layer: no retry/backoff/rate-limit, non-atomic cache writes, 8 copy-pasted NSE session bootstraps

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

- Per-provider NSE home-then-API session bootstrap, each with its own constants (all `requests.Session(impersonate="chrome12x")` from `curl_cffi`): `tantra_data/providers/nse_pit.py:216`, `nse_pledge.py:183`, `tantra_data/providers/option_chain.py:314`, `tantra_data/providers/trading_holidays.py:258`, `tantra_data/providers/ipos.py:500`, `tantra_data/providers/institutional_flow.py:131`, `tantra_data/providers/corporate_actions.py:140`, `bse_shareholding.py:349` (all under `tantra_data/providers/`).
- Bare `requests.get(url, timeout=N)` with no retry: `tantra_data/providers/delivery.py:158`, `bhavcopy.py:193`, `tantra_data/providers/news.py:123`, `tantra_data/providers/block_deals.py:164`, `forex_factory.py:169`; `urlopen` in `tantra_data/providers/mutual_funds.py:296`, `tantra_data/loaders/nse.py:194`, `tantra_data/cli/ingest_bars_v3.py:60-63`. The only `retry|backoff|sleep` hit is the option-chain poll loop (`tantra_data/cli/ingest_option_chain.py:121`).
- `tantra_data/loaders/nse.py:195` — `csv_path.write_bytes(response.read())` is non-atomic, and the next run treats any existing file as complete (`if not csv_path.exists()`), so a killed download poisons the cache.
- Four overlapping bhavcopy downloaders: `tantra_data/loaders/nse.py:185-196`, `tantra_data/cli/ingest_bars_v3.py:49-66`, `tantra_data/providers/bhavcopy.py:193`, `tantra_data/providers/bhavcopy_archive.py`.
- Browser/TLS fingerprint impersonation (`impersonate="chrome124"`) to pass Akamai; no per-source licence or terms-of-use note anywhere in `docs/`.

## Why

A transient 5xx or 429 fails the whole day's ingest with no retry. NSE blocks aggressive clients, and a partially written bhavcopy is silently reused. Impersonation is a terms-of-use risk, so scraping NSE web endpoints needs an explicit licence/provenance decision before any non-research use. The project is non-commercial, so the aim is documented provenance and attribution per source, not a blocker. Interface-first rule (`.agents/rules/interface-first-and-modularity.md`) calls for an `HttpTransport` port.

## How to close it

1. Add `tantra_data/providers/http.py` with an `HttpTransport` Protocol and an `NseSession` (cookie warm-up, shared UA, per-host token bucket, exponential backoff with jitter honouring `Retry-After`, bounded response size).
2. Migrate the eight bootstraps and the bare `requests.get`/`urlopen` sites onto it.
3. Atomic temp-file + rename for all cache writes; verify size/checksum before treating a cached file as complete.
4. Collapse the four bhavcopy downloaders into one.
5. Add `docs/research/data-source-licensing.md`: per source, URL, terms-of-use stance, attribution, and vendor alternative (NSE data products, Dhan, Zerodha historical).

**Verification**: fake-transport tests (429 then 200; `Retry-After`), truncated-download test, a test that every provider uses `HttpTransport`. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/`.

## Related

- [0031](0031-provenance-scheduling-observability.md) (provenance records the fetch details this layer produces).
- Plan 0011 R1 (dynamic-SQL hardening) and R3 (object-store seam) are independent.
