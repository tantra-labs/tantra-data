# 0033 — IST/UTC date boundary: `date.today()` stamps `as_of_ist` from host-local time

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0011](../plans/0011-design-review-2026-11.md)

## What's stubbed or hardcoded

- `tantra_data/cli/catalog.py:426`, `:464`, `:802` — `as_of_ist=date.today()` (host timezone, not Asia/Kolkata).
- `tantra_data/cli/ingest_catalog.py:50` — `today = date.today()` selects the backfill end date.
- `tantra_data/providers/ipos.py:289`, `:518` and `tantra_data/providers/fundamentals.py:448` — same for provider `as_of`.
- `tantra_data/adapters/manifest_store.py:97-99` and `tantra_data/loaders/amfi.py:~157` — naive datetimes are force-tagged UTC.

## Why

In a UTC container (the normal Kubernetes default) a 00:30 IST run is still "yesterday", so `as_of_ist` is wrong by a day. The no-look-ahead reader (`LakehouseBarReader`, closed gap 0026) uses it for session cut-offs, so the error flows straight into point-in-time correctness. Domain-style code should receive time from an injected clock rather than query it (`.agents/rules/interface-first-and-modularity.md`).

## How to close it

1. Add one `ist_today(clock)` helper (zoneinfo `Asia/Kolkata`) and a `Clock` port injected into CLIs and providers.
2. Replace each `date.today()` / `datetime.now()` site listed above.
3. Reject naive datetimes at the manifest boundary instead of force-tagging UTC.
4. Enable the ruff `DTZ011` (and related `DTZ`) rules to ban `date.today()` / naive `datetime.now()` going forward.

**Verification**: freeze the clock at 2026-11-23T19:00Z and assert `as_of_ist` is 2026-11-24; naive-datetime rejection test; `ruff check .` with `DTZ` enabled. Gates: `ruff check .`, `ruff format --check .`, `mypy --strict`, `pytest -q tests/`.

## Related

- Root gap 0020 (time semantics); closed gap 0026 (`LakehouseBarReader`).
- `tantra-core/docs/gaps/0007-calendar-session-model.md` (session model on the Rust side).
