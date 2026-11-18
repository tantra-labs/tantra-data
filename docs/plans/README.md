# Plans Index — `tantra-data`

Per workspace rule `.agents/rules/gap-tracking.md`:
- Gaps are individual features (`docs/gaps/XXXX-*.md`).
- Plans are collections of gaps (`docs/plans/XXXX-*.md`).
- When implemented, plan documents are cleaned up from `docs/plans/` and recorded in this index.
- Numbering is per-repo, sequential, never reused.

## Active Plans

| # | Title | Target Scope | Status |
|---|---|---|---|
| [0007](0007-instrument-master-index-membership.md) | Instrument Master, Classification & Point-in-Time Index Membership | `tantra_data` | Proposed — research `docs/research/trendlyne-screener-alerts-mcp.md`; root plan `docs/plans/0010` (F2); supersedes examples gaps 0004/0006 universe portions |
| [0008](0008-screen-dsl-catalog-qvm.md) | Unified Screen DSL, Screen Catalog & QVM Score | `tantra_data` | Proposed — research `docs/research/trendlyne-screener-alerts-mcp.md`; root plan `docs/plans/0010` (F3–F5); depends on 0007 |
| [0009](0009-unified-lakehouse-v3-and-cas-manifest.md) | Unified Lakehouse v3 Taxonomy, Symbology Aliases & Git-Style CAS Manifest | `tantra_data` | **Implemented 2026-11-17** — root ADR [`docs/design/0004`](../../../docs/design/0004-data-catalog-lakehouse-layout.md) + root plan [`0011`](../../../docs/plans/0011-unified-data-catalog-lakehouse-v3.md); Phases A–C (`ee802e9`/`398eeb7`/`d64c877`): CAS object store + `catalog.duckdb` manifest + `tantra-catalog` CLI; closes gap `0025` |

## Implemented Plans

| # | Title | Target Scope | Implemented In |
|---|---|---|---|
| 0001 | Lakehouse & Port Architecture for MCP Quantitative Tools and Screening | `tantra-data` | `MarketReader`, `ScreenerReader`, `MacroReader`, `ReferenceExporter` read ports in `tantra_data/ports/data_feeds.py`; `LakehouseMarketReader`, `VectorizedScreener`, `InMemoryReferenceExporter`, `PythonMacroReader` in `tantra_data/service/`; screener domain types (`ScreenQuery`, `ScreenCondition`, `ComparisonOp`); 16 tests in `tests/test_data_feeds.py`. |
| 0002 | Filings Ingestion, Economic Series Breadth | `tantra-data` | Macro series ingestion: `MacroObservation` domain, `DuckDbMacroStore`, `MacroSeriesRepository` port, `PythonMacroReader.get_series` (`tests/test_macro.py`); filings ingestion: `Filing`/`FilingType` domain, `DuckDbFilingsStore`, `FilingsRepository`/`FilingsReader` ports, `LakehouseFilingsReader` (`tests/test_filings.py`). Both gaps 0002 and 0003 closed. 18 new tests total. |
| 0003 | Calendar & Reference Data | `tantra-data` (calendar) | Closed in `cd9fea6`: vendor selected — Forex Factory weekly JSON feed (verified live `nfs.faireconomy.media/ff_calendar_thisweek.json`); `providers/forex_factory.py` (`parse_forex_factory`/`events_to_rows`/`fetch_forex_factory`/`ingest_forex_factory`), `cli/ingest_calendar.py`, `default_calendar_rows()` live-with-stub-fallback in `providers/calendar.py`. Gaps 0010 + 0012 closed; tests in `tests/test_forex_factory.py`. |
| 0004 | Content & Disclosure Ingestion | `tantra-data` (block deals, news) | Closed in `3a6b247` (block/bulk deals: `BlockDeal` domain, `providers/block_deals.py`, `DuckDbBlockDealsStore`, `PythonBlockDealsReader`) + `7af9a81` (news: `NewsArticle` domain, Moneycontrol RSS `providers/news.py`, `DuckDbNewsStore`, `PythonNewsReader`). Gaps 0005 + 0006 closed. |
| 0005 | tantra-data → Rust Data Bridge | `tantra_data` (filings, corporate actions) + `tantra-runtime`/`tantra-ffi` | Closed in `tantra-market` `b040f68` (`FilingsProjectionReader` + `CorporateActionsProjectionReader` over `{root}/catalog/filings`|`corporate_actions` parquet projections) + `tantra-runtime` `db60aaa` (`FeedContentReader::with_filings`, `FeedMarketReader::with_corporate_actions`, `ChatRuntime::from_env(data_root)`). Gaps 0007 + 0008 closed; `tests/chat_projection_tests.rs` (4 tests). |
| 0006 | Lakehouse Ingestion Completeness | `tantra-data` (catalog, corporate actions) | Closed in `cd9fea6` (NSE corporate-actions provider → `DuckDbCorporateActionsStore`, gap 0009) + `3245b3b` (bhavcopy backfill `cli/ingest_catalog.py`; `providers/bhavcopy.py` tolerates the live NSE header — gap 0004). |

## Roadmap Dismantling Note

The historical roadmap (`docs/roadmap/roadmap.md`, `docs/roadmap/migration-verification.md`)
has been dismantled:
- Its two milestones (clean package layout & domain models; point-in-time corporate actions
  & quality suite) are complete and captured in `docs/design/code-migration-map.md` and
  `docs/design/architecture.md`.
- All remaining open work lives here as plans 0003–0006, tracking the open gaps in
  `docs/gaps/README.md`.

> **2026-11-13 (T5 track)**: plans 0003–0006 all implemented; the four plan documents were
> removed from `docs/plans/` per gap-tracking §4 and recorded above. `tantra-data`'s remaining
> open gaps are 0015 (NSE trading-holiday provider, data owner) and 0016 (EE+ intraday
> option-chain snapshot lakehouse). The task-list T5 track is complete.
