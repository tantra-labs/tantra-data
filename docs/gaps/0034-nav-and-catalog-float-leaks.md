# 0034 — Float leaks at the AMFI NAV loader and float-typed Parquet normalisation paths

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

- `tantra_data/loaders/amfi.py:162-168` — `_positive_float` parses NAV text with `float(text)` and `math.isfinite`; the result feeds `NavPoint` (`tantra_data/loaders/amfi.py:71`), which `AmfiNavLoader` serves to examples and strategies.
- Contrast: the domain layer enforces zero floats (`tantra_data/domain/fno_spreads.py:28-29`) and `tantra_data/providers/mutual_funds.py` writes exact `DECIMAL` into `DuckDbMutualFundsStore`, so there are two NAV paths with different precision.
- `tantra_data/cli/catalog.py:159-200` — `_normalize_ts` / `_normalize_money` coerce legacy flush segments with `pl.concat(..., how="diagonal_relaxed")`; legacy bars written as Float64 are cast to Decimal after the fact, so binary-float rounding is baked in before `Decimal128(38, 8)`.

## Why

A NAV such as `12.3456` becomes a binary float, and cumulative/IRR calculations downstream drift versus the exact AMFI string. This breaks the zero-floats rule (`.agents/rules/interface-first-and-modularity.md` §2.3) for balances and prices.

## How to close it

1. Parse NAV with `Decimal(text)` and quantize to 4 dp.
2. Delete the `AmfiNavLoader` path in favour of `PythonMutualFundReader` (also root plan 0019 P5.2 dead-code removal).
3. Make `_normalize_money` cast from string/`Decimal` only and fail on `Float64` input, with an explicit `--allow-float-legacy` flag for one-off migration.

**Verification**: hypothesis round-trip on NAV text (`str -> Decimal -> str` identity at 4 dp); a Float64 input raises; migration flag test. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/`.

## Related

- Root plan 0019 P5.2 (reader wiring), partially.
- Root gap 0015 (single numeric source) for the broader float policy; closed gap 0023 (exact `DECIMAL` NAV store).
