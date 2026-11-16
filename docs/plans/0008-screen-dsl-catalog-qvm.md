# 0008 — Unified Screen DSL, Screen Catalog & QVM Score

**Status**: Proposed

## 0. Executive Summary & Objective

Make the Python screener expressive enough for categorised screening: timeframe-aware conditions,
`between`, nested AND/OR, classification and peer-relative filters, aligned losslessly with
tantra-core `ScreenerScanRequest`; ship a versioned TOML preset catalog; and add an open, transparent
Quality / Value / Momentum (QVM) composite score. QVM is deliberately not named DVM and its weights
are published.

References: research [`trendlyne-screener-alerts-mcp.md`](../../../docs/research/trendlyne-screener-alerts-mcp.md) (F3, F4, F5);
root roadmaps [`0010`](../../../docs/plans/0010-screener-taxonomy-and-alerts.md) and [`0008`](../../../docs/plans/0008-trade-signal-generator.md).
Depends on: [tantra-core 0004](../../../tantra-core/docs/plans/0004-classification-and-alert-wire-types.md),
[0007](0007-instrument-master-index-membership.md) (universes, classification),
[tantra-market 0002](../../../tantra-market/docs/plans/0002-chart-pattern-analyzer.md) (candlestick & geometric pattern codes).

## 1. Context & Gaps Covered

`ScreenCondition(field, operator, value)` / `ScreenQuery(universe, conditions, order_by, limit)` in
`ports/data_feeds.py` have no timeframe, no `between`, no grouping; `VectorizedScreener.execute_screen`
checks only the last candle column. Examples chains live in `tantra-examples/examples/screening/chains/*.toml`.

## 2. Component Specifications & Phases

### Phase A — DSL unification
- `ScreenCondition` gains `timeframe`, `between` (with upper bound), classification filters and
  peer-relative values (`pe < sector_median(pe)`, `return_3m > index_return(NIFTY500, 3m)`).
- Nested groups: `ScreenGroup(match_all, conditions | groups)`.
- Lossless mapping to/from tantra-core `ScreenerScanRequest` (schema from FFI/HTTP); round-trip tested.

### Phase B — `VectorizedScreener` v2
- DuckDB joins: bars + indicators, `fundamental_ratios`, `fundamentals` (point-in-time by
  `filing_date`, never `period_end`), classification and index membership. Bind parameters only.
- Crossing ops evaluated over the last two bars; peer stats computed per classification group.

### Phase C — Screen catalog (`tantra_data/catalog_screens/*.toml`, versioned, keyed by `ScreenCategory`)
Presets: relative value set (PE, P/BV, ROCE, ROE, PEG, dividend vs peers); MA crosses incl. golden/death
and price vs SMA/EMA; candlestick & geometric patterns (Darvas box, VCP, flags — aligned to frozen `tantra-market-signals::catalog::REASON_CODES` / `ChartPattern` via `tantra-ffi` rather than re-implementing duplicate candlestick/pattern rules in DuckDB SQL); relative returns vs sector/index; PE buy/sell zone vs own
3/5/10-yr history; Piotroski F-score; Greenblatt Magic Formula; FCF quality; debt-free +
positive FCF + low PE; red flags (financial subset until gap 0024 closes). Each preset has id, version,
category, description, and a golden expected result on fixture data.

### Phase D — QVM score
- Spec TOML: factor definitions and **declared weights**; per-factor `Decimal` percentile rank (0–100)
  within a configurable universe (all-NSE, an index, a sector).
- Bands default Quality 55/35, Value 50/30, Momentum 60/30 (Good/Mid/Bad), configurable.
- Combo-label table (8 G/M/B combinations), eligibility floor (e.g. market cap), point-in-time inputs,
  explainability (per-factor contributions in the result).
- Store `qvm_scores(instrument, as_of, quality, value, momentum, label, spec_version)`.

## 3. Test Plan (TDD)

In `tests/`: `test_screen_dsl.py` (mapping round-trip vs golden core JSON), `test_screener_v2.py`,
`test_screen_catalog.py` (every preset golden-tested), `test_qvm.py`. Hypothesis properties: rank in
[0,100], monotone in input, no look-ahead (changing post-`as_of` data leaves score unchanged), weights sum to 1.

## 4. Dependencies & Tier Notes

L6; Decimal throughout; ports raise explicit Unavailable errors. Feeds
[tantra-agent 0013](../../../tantra-agent/docs/plans/0013-screener-mcp-tools.md),
[tantra-runtime 0007](../../../tantra-runtime/docs/plans/0007-group-screener-alerts.md),
[tantra-examples 0005](../../../tantra-examples/docs/plans/0005-categorized-examples.md).
Shareholding/Forecaster/red-flag-business categories blocked by [gap 0024](../gaps/0024-shareholding-sast-estimates.md).

## 5. Open Questions

- Exact QVM factor inputs and weights; calibrate on history before freezing spec v1.
- Where peer statistics are computed when the Rust runtime serves the scan (Rust vs data layer).
