# Plan 0003 — Calendar & Reference Data

## Objective

Close gaps 0010 and 0012: select a real calendar data source and replace the stubbed
calendar events, completing the calendar pipeline that the reader/port/store already
implement.

## Gaps Tracked by This Plan

| Gap | Title | Severity |
|-----|-------|----------|
| [0010](../gaps/0010-economic-calendar-ingestion.md) | Economic/earnings/dividends/splits/IPO calendar ingestion (real vendor source) | Medium |
| [0012](../gaps/0012-calendar-stubbed-data.md) | Calendar data stubbed pending external source decision | Medium |

## Implementation Phases

1. Select a concrete external calendar data source/vendor (no vendor name invented or
   assumed).
2. Replace `tantra_data/providers/calendar.py::stubbed_calendar_rows()` with a live
   ingestion path through the existing `CalendarRepository` / `DuckDbCalendarStore`.
3. Remove `docs/gaps/0012-calendar-stubbed-data.md` once the placeholder events are gone.
4. Add ingestion tests in `tests/test_calendar.py` (or a dedicated ingestion test module).

## Exit Criteria

- `stubbed_calendar_rows()` no longer feeds `LakehouseCalendarReader` by default.
- `pytest -q`, `ruff check .`, `ruff format --check .`, `mypy tantra_data` pass.
- `docs/gaps/README.md` marks 0010 and 0012 closed with the closing commit.

## Related

- `tantra_data/domain/calendar.py` — `CalendarEvent`.
- `tantra_data/service/calendar_reader.py` — `LakehouseCalendarReader`.
- `tantra-runtime` plan 0003 Phase 3 — `/market-data/calendar` REST endpoint.
