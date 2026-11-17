"""NSE trading-holiday pipeline tests (gap 0015 — T17.1).

Covers the domain value object, the live-with-fixture-fallback provider
(parse / live-failure fallback / on_live_error), the DuckDB store round-trip
and idempotency, the reader port, the ingestion CLI, and the ``trading_holidays``
Parquet bridge export that the ``tantra-runtime-backtest::NseTradingCalendar``
consumer reads.

The live NSE holiday-master feed has never been verifiable from the sandbox
(nseindia.com answers with an HTTP 301 redirect from its Akamai edge layer), so
the provider's live-with-fixture-fallback is exercised with a monkeypatched
live fetch that fails, asserting the labelled ``source: fixture`` rows are
returned and the precise live failure is surfaced via ``on_live_error`` (never
fabricated as live data).
"""

from __future__ import annotations

import importlib
import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data import (
    DuckDbTradingHolidayStore,
    PythonTradingHolidayReader,
    TradingHoliday,
    TradingHolidayReader,
)
from tantra_data.cli.ingest_trading_holidays import default_years
from tantra_data.cli.ingest_trading_holidays import main as ingest_cli_main
from tantra_data.providers.trading_holidays import (
    LIVE_VERIFICATION,
    TradingHolidayLiveError,
    fetch_trading_holidays,
    fetch_trading_holidays_fixture,
    parse_trading_holidays,
)
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter

FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "trading_holidays_2025_2026.json"
)

# NSE published weekday+weekend holiday counts for the fixture years.
YEAR_COUNTS = {2025: 18, 2026: 20}


def test_domain_validation() -> None:
    holiday = TradingHoliday(date=date(2025, 8, 15), description="Independence Day", kind="holiday")
    assert holiday.date == date(2025, 8, 15)
    assert holiday.description == "Independence Day"
    assert holiday.kind == "holiday"


def test_domain_rejects_bad_kind() -> None:
    try:
        TradingHoliday(date=date(2025, 8, 15), description="Independence Day", kind="half-day")
    except ValueError as exc:
        assert "kind" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for bad kind")


def test_domain_rejects_empty_description() -> None:
    try:
        TradingHoliday(date=date(2025, 8, 15), description="  ", kind="holiday")
    except ValueError as exc:
        assert "description" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for empty description")


def test_importing_provider_requires_no_network() -> None:
    module = importlib.import_module("tantra_data.providers.trading_holidays")
    assert callable(module.fetch_trading_holidays)
    assert "not-verified-live" in LIVE_VERIFICATION


def test_parse_fixture_matches_nse_published_lists() -> None:
    rows = parse_trading_holidays(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert len(rows) == sum(YEAR_COUNTS.values())
    for year, count in YEAR_COUNTS.items():
        assert len([r for r in rows if r.date.year == year]) == count
    # Diwali Laxmi Pujan carries the Muhurat session in both years.
    muhurat = [r for r in rows if r.kind == "muhurat"]
    assert {r.date for r in muhurat} == {date(2025, 10, 21), date(2026, 11, 8)}
    # The seed-table dates the runtime consumer historically hardcoded.
    assert {r.date for r in rows if r.date.year == 2025} >= {
        date(2025, 1, 26),
        date(2025, 8, 15),
        date(2025, 10, 21),
        date(2025, 10, 22),
        date(2025, 12, 25),
    }
    assert all(isinstance(r.date, date) for r in rows)


def test_parse_live_array_shape_with_nse_date_format() -> None:
    text = json.dumps(
        [
            {"tradingDate": "15-Aug-2025", "description": "Independence Day"},
            {"tradingDate": "21-Oct-2025", "description": "Diwali Laxmi Pujan", "kind": "muhurat"},
        ]
    )
    rows = parse_trading_holidays(text)
    assert [r.date for r in rows] == [date(2025, 8, 15), date(2025, 10, 21)]
    assert [r.kind for r in rows] == ["holiday", "muhurat"]


def test_parse_rejects_invalid_payloads() -> None:
    try:
        parse_trading_holidays("not json")
    except ValueError as exc:
        assert "JSON" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for invalid JSON")

    try:
        parse_trading_holidays(json.dumps({"source": "fixture"}))  # no trading_holidays key
    except ValueError as exc:
        assert "trading_holidays" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for missing array")

    try:
        parse_trading_holidays(
            json.dumps([{"date": "2025-08-15", "description": "X", "kind": "bad"}])
        )
    except ValueError as exc:
        assert "kind" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for bad kind")


def test_fetch_trading_holidays_falls_back_to_fixture(monkeypatch) -> None:
    def network_down():
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(
        "tantra_data.providers.trading_holidays._fetch_live_trading_holidays", network_down
    )
    rows = fetch_trading_holidays(2025)
    assert len(rows) == YEAR_COUNTS[2025]
    assert {r.date.year for r in rows} == {2025}
    assert all(r.date.year == 2025 for r in rows)


def test_fetch_trading_holidays_no_year_returns_all_fixture_rows(monkeypatch) -> None:
    def network_down():
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(
        "tantra_data.providers.trading_holidays._fetch_live_trading_holidays", network_down
    )
    rows = fetch_trading_holidays()
    assert len(rows) == sum(YEAR_COUNTS.values())


def test_fetch_trading_holidays_surfaces_live_error(monkeypatch) -> None:
    errors: list[TradingHolidayLiveError] = []

    def network_down():
        raise RuntimeError("connection refused")

    monkeypatch.setattr(
        "tantra_data.providers.trading_holidays._fetch_live_trading_holidays", network_down
    )
    rows = fetch_trading_holidays(2025, on_live_error=errors.append)
    assert len(rows) == YEAR_COUNTS[2025]
    assert len(errors) == 1
    assert errors[0].status_code is None
    assert "holiday-master" in errors[0].url or "nseindia" in errors[0].url


def test_fetch_trading_holidays_fixture_is_labeled_fixture() -> None:
    rows = fetch_trading_holidays_fixture(2026)
    assert len(rows) == YEAR_COUNTS[2026]
    assert {r.date.year for r in rows} == {2026}


def test_fetch_trading_holidays_fixture_year_outside_coverage_is_empty() -> None:
    assert fetch_trading_holidays_fixture(2027) == []


def test_store_round_trip_and_year_filter(tmp_path) -> None:
    store = DuckDbTradingHolidayStore(tmp_path)
    try:
        store.put(fetch_trading_holidays_fixture())
        assert store.years() == [2025, 2026]
        year_2026 = store.holidays_for_year(2026)
        assert {r.date.year for r in year_2026} == {2026}
        assert len(year_2026) == YEAR_COUNTS[2026]
        # ordered by date
        assert [r.date for r in year_2026] == sorted(r.date for r in year_2026)
        # Muhurat row survives the round-trip.
        assert any(r.date == date(2026, 11, 8) and r.kind == "muhurat" for r in year_2026)
    finally:
        store.close()


def test_store_put_is_idempotent_per_year(tmp_path) -> None:
    store = DuckDbTradingHolidayStore(tmp_path)
    try:
        store.put(fetch_trading_holidays_fixture(2025))
        store.put(fetch_trading_holidays_fixture(2025))  # same year -> replace, never duplicate
        assert len(store.holidays_for_year(2025)) == YEAR_COUNTS[2025]
        assert store.years() == [2025]
        # A stale row is removed when the year's master is refreshed.
        store.put(
            [
                TradingHoliday(
                    date=date(2025, 8, 15), description="Independence Day", kind="holiday"
                ),
                TradingHoliday(date=date(2025, 12, 25), description="Christmas", kind="holiday"),
            ]
        )
        refreshed = store.holidays_for_year(2025)
        assert len(refreshed) == 2
        assert {r.date for r in refreshed} == {date(2025, 8, 15), date(2025, 12, 25)}
    finally:
        store.close()


def test_reader_implements_port(tmp_path) -> None:
    store = DuckDbTradingHolidayStore(tmp_path)
    try:
        store.put(fetch_trading_holidays_fixture(2025))
        reader = PythonTradingHolidayReader(store)
        assert isinstance(reader, TradingHolidayReader)
        assert len(reader.holidays_for_year(2025)) == YEAR_COUNTS[2025]
    finally:
        store.close()


def test_ingest_cli_upserts_into_store(tmp_path, capsys, monkeypatch) -> None:
    rows = fetch_trading_holidays_fixture(2026)
    monkeypatch.setattr(
        "tantra_data.cli.ingest_trading_holidays.fetch_trading_holidays",
        lambda year, on_live_error=None: [r for r in rows if r.date.year == year],
    )
    rc = ingest_cli_main(["--root", str(tmp_path), "--year", "2026"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Ingested 20 trading holidays for 2026" in captured.out
    assert (tmp_path / "trading_holidays.duckdb").exists()

    store = DuckDbTradingHolidayStore(tmp_path)
    try:
        assert len(store.holidays_for_year(2026)) == YEAR_COUNTS[2026]
    finally:
        store.close()


def test_ingest_cli_default_years_current_and_next() -> None:
    from datetime import UTC, datetime

    now_year = datetime.now(UTC).year
    assert default_years() == [now_year, now_year + 1]


def test_ingest_cli_warns_on_live_failure(tmp_path, capsys, monkeypatch) -> None:
    rows = fetch_trading_holidays_fixture(2025)

    def failing_fetch(year, on_live_error=None):
        if on_live_error is not None:
            on_live_error(TradingHolidayLiveError("live down", url="https://x", status_code=None))
        return [r for r in rows if r.date.year == year]

    monkeypatch.setattr(
        "tantra_data.cli.ingest_trading_holidays.fetch_trading_holidays", failing_fetch
    )
    rc = ingest_cli_main(["--root", str(tmp_path), "--year", "2025"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "WARNING trading-holiday live fetch failed" in captured.err


def test_export_trading_holidays_writes_per_year_parquet(tmp_path) -> None:
    store = DuckDbTradingHolidayStore(tmp_path / "db")
    try:
        store.put(fetch_trading_holidays_fixture())
    finally:
        store.close()

    store2 = DuckDbTradingHolidayStore(tmp_path / "db")
    exporter = LakehouseBridgeExporter(None, None, trading_holidays=store2)
    try:
        assert exporter.export_trading_holidays(tmp_path / "catalog") == 2
    finally:
        store2.close()

    files = sorted((tmp_path / "catalog" / "trading_holidays").glob("*.parquet"))
    assert [file.name for file in files] == ["2025.parquet", "2026.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == ["holiday_date", "description", "kind"]
    assert table.schema.field("holiday_date").type == pa.date32()
    assert table.schema.field("description").type == pa.string()
    assert table.schema.field("kind").type == pa.string()
    data = table.to_pydict()
    assert len(data["holiday_date"]) == YEAR_COUNTS[2025]
    assert date(2025, 8, 15) in data["holiday_date"]
    assert set(data["kind"]) == {"holiday", "muhurat"}
    # The 2026 file is sorted by date and carries the Muhurat session.
    table_2026 = pq.read_table(files[1]).to_pydict()
    assert len(table_2026["holiday_date"]) == YEAR_COUNTS[2026]
    assert table_2026["holiday_date"] == sorted(table_2026["holiday_date"])
    assert table_2026["holiday_date"][-1] == date(2026, 12, 25)


def test_export_trading_holidays_missing_store_writes_nothing(tmp_path) -> None:
    exporter = LakehouseBridgeExporter(None, None)
    assert exporter.export_trading_holidays(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "trading_holidays").exists()
