"""Tests for the Forex Factory economic-calendar provider (T5.1).

The vendor decision for the ``Economic`` calendar category is the Forex
Factory public weekly JSON feed. These tests cover parsing, row mapping,
the (mocked) live fetch, the ``CalendarEvent``/DuckDB round-trip, and the
offline stub fallback of ``default_calendar_rows``. No test hits the live
feed: the fetch test monkeypatches ``requests.get``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest import mock

import pytest
import requests

from tantra_data.adapters.duckdb_store import DuckDbCalendarStore
from tantra_data.providers.calendar import (
    CalendarRow,
    default_calendar_rows,
    rows_to_events,
    stubbed_calendar_rows,
)
from tantra_data.providers.forex_factory import (
    ForexFactoryEvent,
    events_to_rows,
    fetch_forex_factory,
    parse_forex_factory,
)

CANNED_FOREX_FACTORY_JSON = json.dumps(
    [
        {
            "title": "CPI y/y",
            "country": "CNY",
            "date": "2026-11-08T20:30:00-05:00",
            "impact": "Low",
            "forecast": "0.8%",
            "previous": "0.9%",
        },
        {
            "title": "Gross Domestic Product q/q",
            "country": "USD",
            "date": "2026-11-09T13:30:00-05:00",
            "impact": "High",
            "forecast": "1.2%",
            "previous": "",
        },
    ]
)


def test_parse_forex_factory_parses_verified_feed_shape() -> None:
    events = parse_forex_factory(CANNED_FOREX_FACTORY_JSON)

    assert len(events) == 2
    first = events[0]
    assert isinstance(first, ForexFactoryEvent)
    assert first.title == "CPI y/y"
    assert first.country == "CNY"
    assert first.scheduled_at == datetime(2026, 11, 9, 1, 30, tzinfo=UTC)
    assert first.scheduled_at.tzinfo is not None
    assert first.impact == "Low"
    assert first.forecast == Decimal("0.8")
    assert first.previous == Decimal("0.9")

    second = events[1]
    assert second.title == "Gross Domestic Product q/q"
    assert second.country == "USD"
    assert second.scheduled_at == datetime(2026, 11, 9, 18, 30, tzinfo=UTC)
    assert second.impact == "High"
    assert second.forecast == Decimal("1.2")
    assert second.previous is None


def test_events_to_rows_maps_economic_category() -> None:
    rows = events_to_rows(parse_forex_factory(CANNED_FOREX_FACTORY_JSON))

    assert len(rows) == 2
    first = rows[0]
    assert isinstance(first, CalendarRow)
    assert first.category == "Economic"
    assert first.country == "CNY"
    assert first.currency == "CNY"
    assert first.event_name == "CPI y/y"
    assert first.impact_level == "Low"
    assert first.instrument is None
    assert first.actual is None
    assert first.forecast == Decimal("0.8")
    assert first.prior == Decimal("0.9")
    assert first.period_label is None
    assert first.scheduled_at.tzinfo is not None

    second = rows[1]
    assert second.forecast == Decimal("1.2")
    assert second.prior is None


def test_fetch_forex_factory_uses_mocked_requests() -> None:
    response = mock.Mock()
    response.status_code = 200
    response.text = CANNED_FOREX_FACTORY_JSON
    with mock.patch.object(requests, "get", return_value=response) as fake_get:
        rows = fetch_forex_factory()

    fake_get.assert_called_once()
    assert len(rows) == 2
    assert all(isinstance(row, CalendarRow) for row in rows)
    assert rows[0].country == "CNY"
    assert rows[0].forecast == Decimal("0.8")
    assert rows[0].scheduled_at.tzinfo is not None


def test_forex_factory_rows_round_trip_through_calendar_store() -> None:
    rows = events_to_rows(parse_forex_factory(CANNED_FOREX_FACTORY_JSON))
    events = rows_to_events(rows)

    store = DuckDbCalendarStore(":memory:")
    try:
        for event in events:
            store.upsert(event)
        stored = store.query(category="Economic")
    finally:
        store.close()

    assert len(stored) == 2
    first = stored[0]
    assert first.category == "Economic"
    assert first.country == "CNY"
    assert first.forecast == Decimal("0.8")
    assert first.prior == Decimal("0.9")
    assert first.actual is None
    assert first.scheduled_at.tzinfo is not None


def test_parse_forex_factory_rejects_invalid_json() -> None:
    with pytest.raises(ValueError, match="not valid JSON"):
        parse_forex_factory("this is not valid json {")


def test_default_calendar_rows_falls_back_to_stub_on_network_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _network_down() -> list[CalendarRow]:
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(
        "tantra_data.providers.calendar.fetch_forex_factory", _network_down
    )

    rows = default_calendar_rows()

    assert rows == stubbed_calendar_rows()
    assert len(rows) == 7
