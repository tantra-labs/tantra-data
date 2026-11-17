# 0008 — Unified Screen DSL, Screen Catalog & QVM Score

**Status**: Proposed (updated 2026-11-16)

## 0. Executive Summary & Objective

Make the Python screener expressive enough for categorised screening: timeframe-aware conditions,
`between`, nested AND/OR, classification and peer-relative filters, aligned losslessly with the
converged `tantra-core` `ScreenerScanRequest`; own the **`ScreenCategory` taxonomy** and versioned
TOML preset catalog; and add an open, transparent Quality / Value / Momentum (QVM) composite score.
QVM is deliberately not named DVM and its weights are published.

References: research [`trendlyne-screener-alerts-mcp.md`](../../../docs/research/trendlyne-screener-alerts-mcp.md) (F3, F4, F5);
root roadmaps [`0010`](../../../docs/plans/0010-screener-taxonomy-and-alerts.md) and [`0008`](../../../docs/plans/0008-trade-signal-generator.md).
Depends on: [tantra-core 0004 (Implemented 2026-11-17)](../../../tantra-core/docs/plans/README.md#implemented-plans),
[0007](0007-instrument-master-index-membership.md) (universes, classification),
[tantra-market 0002](../../../tantra-market/docs/plans/0002-chart-pattern-analyzer.md) (candlestick & geometric pattern codes).

## 1. Context & Gaps Covered

`ScreenCondition(field, operator, value)` / `ScreenQuery(universe, conditions, order_by, limit)` in
`ports/data_feeds.py` have no timeframe, no `between`, no nested grouping; `VectorizedScreener.execute_screen`
checks only the last candle column. Examples chains live in `tantra-examples/examples/screening/chains/*.toml`.

## 2. Component Specifications & Phases

### Phase A — DSL unification
- `ScreenCondition` gains `timeframe`, `between` (with upper bound), classification filters and
  peer-relative values (`pe < sector_median(pe)`, `return_3m > index_return(NIFTY500, 3m)`).
- Nested groups: `ScreenGroup(operator: "AND" | "OR", items: list[ScreenCondition | ScreenGroup])`.
- Lossless mapping to/from the converged `tantra-core` `ScreenerScanRequest` (via `tantra-ffi` / JSON schema); round-trip tested against golden `tantra-core` vectors.

### Phase B — `VectorizedScreener` v2 & L3 Runtime Bridge
- DuckDB joins: bars + indicators, `fundamental_ratios`, `fundamentals` (point-in-time by
  `filing_date`, never `period_end`), classification, cap-bucket history, QVM scores, and index membership. Bind parameters only.
- Crossing ops evaluated over the last two bars; peer stats (`sector_median`, `industry_median`, `index_return`) computed per classification group.
- **Evaluator parity**: expose a clean `ScreenerReader.execute_screen` / RPC-friendly entry point (and Parquet outputs for `qvm_scores` + peer stats) so L3 [`tantra-runtime` 0007](../../../tantra-runtime/docs/plans/0007-group-screener-alerts.md) can evaluate cross-sectional/fundamental Alpha-Alerts rules without duplicating DuckDB SQL logic in Rust.

### Phase C — Screen catalog (`tantra_data/catalog_screens/*.toml`, versioned, owning `ScreenCategory`)
- Define the `ScreenCategory` taxonomy (`StrEnum` + validated slug in `tantra_data`, exported in catalog JSON rather than `tantra-core` L0): `expert`, `qvm_score`, `price_volume`, `intraday`, `swing`, `relative_value`, `relative_returns`, `financials`, `technicals`, `moving_averages`, `shareholding`, `candlesticks`, `red_flags`, `pe_buy_sell_zone`, `other`.
- Presets: relative value set (PE, P/BV, ROCE, ROE, PEG, dividend vs peers); MA crosses incl. golden/death
  and price vs SMA/EMA; candlestick & geometric patterns (Darvas box, VCP, flags — aligned to frozen `tantra-market-signals::catalog::REASON_CODES` / `ChartPattern` via `tantra-ffi` rather than re-implementing duplicate candlestick/pattern rules in DuckDB SQL); relative returns vs sector/index; PE buy/sell zone vs own
  3/5/10-yr history; Piotroski F-score; Greenblatt Magic Formula; FCF quality; debt-free +
  positive FCF + low PE; red flags (financial subset until gap 0024 closes). Each preset has `id`, `version`,
  `category`, `description`, `supported_frequencies`, and a golden expected result on fixture data.

### Phase D — QVM score
- Spec TOML: factor definitions and **declared weights**; per-factor `Decimal` percentile rank (0–100)
  within a configurable universe (all-NSE, an index, a sector). Note: compute percentile ranks in exact `Decimal` (do not route QVM rank math through Polars `float`).
- Bands default Quality 55/35, Value 50/30, Momentum 60/30 (Good/Mid/Bad), configurable.
- Combo-label table (8 G/M/B combinations), eligibility floor (e.g. market cap), point-in-time inputs,
  explainability (per-factor contributions in the result), plus transition flags for Alpha Alerts (`high_durability_crossover`, `strong_performer_getting_expensive`, `momentum_trap`, `falling_comet`, `rising_momentum`).
- Store `qvm_scores(instrument, as_of, quality, value, momentum, label, spec_version)` and export `catalog/qvm_scores` Parquet projection for Rust readers.

## 3. Test Plan (TDD)

In `tests/`: `test_screen_dsl.py` (mapping round-trip vs golden core JSON), `test_screener_v2.py`,
`test_screen_catalog.py` (every preset golden-tested + `ScreenCategory` validation), `test_qvm.py`. Hypothesis properties: rank in
`[0,100]`, monotone in input, no look-ahead (changing post-`as_of` data leaves score unchanged), weights sum to 1.

## 4. Dependencies & Tier Notes

L6; `Decimal` throughout; ports raise explicit Unavailable errors. Feeds
[tantra-agent 0013](../../../tantra-agent/docs/plans/0013-screener-mcp-tools.md),
[tantra-runtime 0007](../../../tantra-runtime/docs/plans/0007-group-screener-alerts.md),
[tantra-examples 0005](../../../tantra-examples/docs/plans/0005-categorized-examples.md).
Shareholding/Forecaster/red-flag-business categories blocked by [gap 0024](../gaps/0024-shareholding-sast-estimates.md).

## 5. Open Questions

- Exact QVM factor inputs and weights; calibrate on history before freezing spec v1.
- Transport for `ScreenerEvaluatorPort` when L3 `tantra-runtime` evaluates DuckDB-backed screens (in-process PyO3 vs local data-service HTTP/IPC).
