"""Forex Factory economic-calendar feed provider (T5.1 vendor decision).

The live economic-calendar source selected for ``tantra-data`` is Forex
Factory's public weekly economic-calendar JSON feed::

    https://nfs.faireconomy.media/ff_calendar_thisweek.json

The feed needs no API key and is machine-readable JSON. Verified shape
(live 2026-11-13)::

    [{"title": "CPI y/y", "country": "CNY", "date": "2026-11-08T20:30:00-05:00",
      "impact": "Low", "forecast": "0.8%", "previous": "0.9%"}, ...]

``title`` is the event name, ``country`` is a 3-letter currency/country code
(``AUD``, ``CNY``, ``USD``, ``INR``, ...), ``date`` is ISO-8601 with a UTC
offset, ``impact`` is one of ``High``/``Medium``/``Low``, and
``forecast``/``previous`` are strings that may be empty and may end in ``%``
(the ``%`` is stripped before exact ``Decimal`` conversion).

The feed is global economic releases and maps onto the ``Economic`` calendar
category. The other categories (Earnings/Dividends/Splits/IPO) remain served
by the existing corporate-action/filings ingestion.

Zero-floats invariant: every numeric value is an exact ``decimal.Decimal``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

from tantra_data.domain.calendar import CalendarEvent

if TYPE_CHECKING:
    from tantra_data.providers.calendar import CalendarRow

_FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_FEED_TIMEOUT_SECONDS = 15
_VALID_IMPACT_LEVELS = ("High", "Medium", "Low")


@dataclass(frozen=True, slots=True)
class ForexFactoryEvent:
    """One parsed Forex Factory economic-calendar event."""

    title: str
    country: str
    scheduled_at: datetime
    impact: str
    forecast: Decimal | None
    previous: Decimal | None


class _CalendarRepository(Protocol):
    """Minimal structural contract for a calendar store (``.upsert``)."""

    def upsert(self, event: CalendarEvent) -> None: ...


def _decimal_or_none(text: object) -> Decimal | None:
    """Convert a feed string to an exact ``Decimal``, stripping a trailing ``%``.

    Empty/whitespace strings and ``None`` map to ``None``.
    """
    if text is None:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    if raw.endswith("%"):
        raw = raw[:-1].strip()
    if not raw:
        return None
    return Decimal(raw)


def parse_forex_factory(text: str) -> list[ForexFactoryEvent]:
    """Parse the Forex Factory weekly calendar JSON into typed events.

    Raises ``ValueError`` on malformed input: invalid JSON, missing titles,
    non-3-letter country codes, tz-naive dates, or unknown impact levels.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("forex factory calendar data is not valid JSON") from exc
    if not isinstance(payload, list):
        raise ValueError("forex factory calendar data must be a JSON array")
    events: list[ForexFactoryEvent] = []
    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError("forex factory calendar entries must be JSON objects")
        title = str(raw.get("title", "")).strip()
        if not title:
            raise ValueError("forex factory calendar entry is missing a non-empty title")
        country = str(raw.get("country", "")).strip().upper()
        if len(country) != 3 or not country.isalpha():
            raise ValueError(f"invalid forex factory country code: {country!r}")
        date_text = raw.get("date")
        try:
            dt = datetime.fromisoformat(str(date_text))
        except ValueError as exc:
            raise ValueError(f"invalid forex factory date: {date_text!r}") from exc
        if dt.tzinfo is None:
            raise ValueError(f"forex factory date must be timezone-aware: {date_text!r}")
        events.append(
            ForexFactoryEvent(
                title=title,
                country=country,
                scheduled_at=dt.astimezone(UTC),
                impact=_impact_level(raw.get("impact")),
                forecast=_decimal_or_none(raw.get("forecast")),
                previous=_decimal_or_none(raw.get("previous")),
            )
        )
    return events


def _impact_level(text: object) -> str:
    impact = str(text).strip().upper()
    if impact not in ("HIGH", "MEDIUM", "LOW"):
        raise ValueError(f"invalid forex factory impact level: {impact!r}")
    return impact.title()


def events_to_rows(
    events: list[ForexFactoryEvent], currency: str | None = None
) -> list[CalendarRow]:
    """Map parsed Forex Factory events onto canonical ``CalendarRow`` objects.

    The feed's 3-letter currency code (e.g. ``"CNY"``, ``"USD"``) doubles as
    both the row's ``country`` and ``currency`` unless ``currency`` overrides
    the latter. Every event maps to the ``Economic`` category with no
    instrument and no actual value.
    """
    from tantra_data.providers.calendar import CalendarRow

    return [
        CalendarRow(
            instrument=None,
            scheduled_at=event.scheduled_at,
            country=event.country,
            currency=event.country if currency is None else currency,
            category="Economic",
            event_name=event.title,
            impact_level=event.impact,
            actual=None,
            forecast=event.forecast,
            prior=event.previous,
            period_label=None,
        )
        for event in events
    ]


def fetch_forex_factory() -> list[CalendarRow]:
    """Fetch and parse the current Forex Factory weekly calendar feed.

    Uses a lazy ``requests`` import so the provider stays importable in
    offline setups. Raises ``RuntimeError`` on network failure or a non-200
    HTTP status.
    """
    import requests

    try:
        response = requests.get(_FEED_URL, timeout=_FEED_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise RuntimeError(f"forex factory calendar fetch failed: {exc}") from exc
    if response.status_code != 200:
        raise RuntimeError(
            f"forex factory calendar fetch returned HTTP {response.status_code}"
        )
    return events_to_rows(parse_forex_factory(response.text))


def ingest_forex_factory(
    repository: _CalendarRepository, limit: int | None = None
) -> int:
    """Fetch, map, and upsert the live feed into a calendar repository.

    Returns the number of events upserted. When ``limit`` is given, only the
    first ``limit`` rows are upserted.
    """
    from tantra_data.providers.calendar import rows_to_events

    rows = fetch_forex_factory()
    if limit is not None:
        rows = rows[:limit]
    events = rows_to_events(rows)
    for event in events:
        repository.upsert(event)
    return len(events)


__all__ = [
    "ForexFactoryEvent",
    "events_to_rows",
    "fetch_forex_factory",
    "ingest_forex_factory",
    "parse_forex_factory",
]
