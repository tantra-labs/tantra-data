"""NSE trading-holiday provider (gap 0015).

Live source::

    https://www.nseindia.com/api/holiday-master?type=trading

This is the public NSE holiday-master endpoint the exchange frontend calls for
the **trading** holiday list (the capital-market segment). It needs no API key.
The fetch uses the repo's working ``curl_cffi`` Chrome-impersonating session
pattern (``providers/institutional_flow.py`` / ``providers/option_chain.py``)
with browser-like headers.

Verification status (checked 2026-11-17): **NOT verified live** from the
sandbox — ``nseindia.com`` answers the ``holiday-master`` path with an HTTP 301
redirect (the site's Akamai edge layer), so no payload is ever received. Live
data is never fabricated: the provider uses the repo's live-with-stub-fallback
pattern and falls back to the bundled recorded fixture
``tests/fixtures/trading_holidays_2025_2026.json`` (labelled
``source: fixture``), whose rows were transcribed from the exchange's published
circulars (NSE/CMTR/65587, NSE/CMTR/71775, NSE/CMTR/72260, NSE/CMTR/70319,
NSE/CMTR/76941). The precise live failure is surfaced through the optional
``on_live_error`` callback as :class:`TradingHolidayLiveError`; the current
status is pinned machine-readably in :data:`LIVE_VERIFICATION`.

The fixture covers calendar years 2025 and 2026 (matching the runtime
consumer's offline seed scope). A ``year`` outside that coverage that cannot be
served live returns an empty list — never fabricated rows.

Zero-floats invariant: the rows carry dates/descriptions/kinds only — no
numeric values, and no ``float`` anywhere.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from tantra_data.domain.trading_holidays import TradingHoliday

__all__ = [
    "HOLIDAY_MASTER_URL",
    "LIVE_VERIFICATION",
    "TradingHolidayLiveError",
    "fetch_trading_holidays",
    "fetch_trading_holidays_fixture",
    "parse_trading_holidays",
    "read_trading_holidays",
]

NSE_HOME_URL = "https://www.nseindia.com"
HOLIDAY_MASTER_URL = f"{NSE_HOME_URL}/api/holiday-master"
HOLIDAY_MASTER_TYPE = "trading"
REFERER_URL = "https://www.nseindia.com/market-info/holiday-timings"
NSE_DATE_FORMAT = "%d-%b-%Y"
REQUEST_TIMEOUT_SECONDS = 20
_VALID_KINDS = ("holiday", "muhurat")

#: Browser-like headers for the live feed, matching the working NSE providers
#: (``providers/institutional_flow.py``) plus an explicit ``User-Agent`` and
#: ``Accept-Language`` so NSE's Akamai layer is more likely to serve JSON.
LIVE_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": REFERER_URL,
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

#: Current live-verification status of the NSE holiday-master feed (checked
#: 2026-11-17). Kept machine-readable so tooling and tests can assert the
#: provider's honest verification state.
LIVE_VERIFICATION = (
    "not-verified-live (checked 2026-11-17): nseindia.com answers the "
    "holiday-master path with an HTTP 301 redirect from the sandbox (Akamai "
    "edge layer), so no payload is received. Live data is never fabricated: "
    "fetch_trading_holidays falls back to the labelled recorded fixture "
    "(source=fixture, transcribed from NSE circulars NSE/CMTR/65587 / "
    "NSE/CMTR/71775 / NSE/CMTR/72260 / NSE/CMTR/70319 / NSE/CMTR/76941) and "
    "surfaces the precise failure via on_live_error / TradingHolidayLiveError."
)

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "trading_holidays_2025_2026.json"
)


class TradingHolidayLiveError(RuntimeError):
    """A precise failure from the live NSE holiday-master feed.

    ``status_code`` is the HTTP status when the endpoint responded (``None``
    for transport-level failures such as timeouts); ``url`` identifies the
    request. Callers use this to distinguish a blocked endpoint (redirect/JS
    challenge), a transport failure, or a live payload that does not cover the
    requested year.
    """

    def __init__(
        self,
        message: str,
        *,
        url: str,
        status_code: int | None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code


def parse_trading_holidays(text: str) -> list[TradingHoliday]:
    """Parse a trading-holiday JSON payload into typed rows.

    Accepts either the bundled fixture shape (an object with a
    ``trading_holidays`` array) or the live holiday-master array shape (a bare
    JSON array). Every row needs a ``date``/``tradingDate`` (ISO ``YYYY-MM-DD``
    or NSE ``DD-MMM-YYYY``), a non-empty ``description``, and a ``kind`` of
    ``"holiday"`` or ``"muhurat"`` (default ``"holiday"``). Raises
    ``ValueError`` on malformed input.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("trading-holiday data is not valid JSON") from exc
    if isinstance(payload, dict):
        rows_payload = payload.get("trading_holidays")
        if not isinstance(rows_payload, list):
            raise ValueError("trading-holiday payload is missing its trading_holidays array")
    elif isinstance(payload, list):
        rows_payload = payload
    else:
        raise ValueError("trading-holiday payload must be a JSON object or array")
    return [_parse_holiday_row(row) for row in rows_payload]


def _parse_holiday_row(raw: object) -> TradingHoliday:
    if not isinstance(raw, dict):
        raise ValueError("trading-holiday entries must be JSON objects")
    date_text = raw.get("date") or raw.get("tradingDate")
    if not isinstance(date_text, str) or not date_text.strip():
        raise ValueError(f"trading-holiday entry is missing a date: {raw!r}")
    holiday_date = _parse_holiday_date(date_text.strip())
    description = str(raw.get("description", "")).strip()
    kind = str(raw.get("kind", "holiday")).strip().lower()
    if kind not in _VALID_KINDS:
        raise ValueError(f"invalid trading-holiday kind {kind!r}; must be one of {_VALID_KINDS}")
    return TradingHoliday(date=holiday_date, description=description, kind=kind)


def _parse_holiday_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.strptime(text, NSE_DATE_FORMAT).date()
    except ValueError as exc:
        raise ValueError(f"invalid trading-holiday date: {text!r}") from exc


def read_trading_holidays(path: str | Path) -> list[TradingHoliday]:
    """Read and parse a trading-holiday JSON file from disk."""
    with open(path, encoding="utf-8") as handle:
        return parse_trading_holidays(handle.read())


def fetch_trading_holidays(
    year: int | None = None,
    on_live_error: Callable[[TradingHolidayLiveError], None] | None = None,
) -> list[TradingHoliday]:
    """Fetch NSE trading holidays for ``year`` (all years when ``None``).

    Live-with-stub-fallback (mirrors ``providers/option_chain.py`` /
    ``providers/calendar.py::default_calendar_rows``): attempts the live NSE
    ``holiday-master`` feed and, on any failure (offline, HTTP error, blocked
    endpoint, or a live payload that does not cover ``year``), falls back to
    the bundled recorded fixture (``source: fixture``) so the pipeline stays
    runnable offline. The fixture is never presented as live data.

    The precise live failure is passed to ``on_live_error`` (when given) as a
    :class:`TradingHolidayLiveError` carrying the ``url`` and HTTP
    ``status_code`` (``None`` for transport failures), so callers can log or
    alert without ever fabricating data.
    """
    try:
        rows = _fetch_live_trading_holidays()
        if year is None:
            return rows
        matched = [row for row in rows if row.date.year == year]
        if matched:
            return matched
        covered = sorted({row.date.year for row in rows})
        raise TradingHolidayLiveError(
            f"live holiday-master returned no {year} rows (only {covered} available)",
            url=HOLIDAY_MASTER_URL,
            status_code=200,
        )
    except Exception as exc:
        if not isinstance(exc, TradingHolidayLiveError):
            exc = TradingHolidayLiveError(
                f"trading-holiday live fetch failed: {exc}",
                url=HOLIDAY_MASTER_URL,
                status_code=None,
            )
        if on_live_error is not None:
            on_live_error(exc)
        return fetch_trading_holidays_fixture(year)


def _fetch_live_trading_holidays() -> list[TradingHoliday]:
    """Fetch the live NSE holiday-master JSON payload (lazy ``curl_cffi``).

    Every failure mode is wrapped in :class:`TradingHolidayLiveError`: a
    non-200 response (e.g. the 301 redirect the sandbox observes), a malformed
    payload, and transport errors (timeouts, connection errors).
    """
    session: Any = _requests_session()
    session.get(NSE_HOME_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response = session.get(
        HOLIDAY_MASTER_URL,
        params={"type": HOLIDAY_MASTER_TYPE},
        headers=LIVE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    status_code: int | None = None
    try:
        status_code = int(response.status_code)
    except (TypeError, ValueError):
        status_code = None
    if status_code != 200:
        raise TradingHolidayLiveError(
            f"trading-holiday live fetch failed with HTTP {status_code} for "
            f"type={HOLIDAY_MASTER_TYPE}",
            url=HOLIDAY_MASTER_URL,
            status_code=status_code,
        )
    try:
        return parse_trading_holidays(response.text)
    except ValueError as exc:
        raise TradingHolidayLiveError(
            f"trading-holiday live fetch returned an unusable payload: {exc}",
            url=HOLIDAY_MASTER_URL,
            status_code=status_code,
        ) from exc


def _requests_session() -> Any:
    """Create a Chrome-impersonating session (lazy ``curl_cffi`` import).

    Split out so tests can inject a fake transport without importing
    ``curl_cffi``.
    """
    from curl_cffi import requests

    return requests.Session(impersonate="chrome124")


def fetch_trading_holidays_fixture(year: int | None = None) -> list[TradingHoliday]:
    """Return the bundled recorded fixture rows (``source: fixture``).

    Filters to ``year`` when given; ``None`` returns every fixture row. The
    fixture must be labelled ``source: fixture``; any payload that is not is
    rejected so a mislabelled fallback can never masquerade as live data.

    The bundled fixture covers calendar years 2025 and 2026; a ``year``
    outside that coverage returns an empty list (never fabricated rows).
    """
    text = _FIXTURE_PATH.read_text(encoding="utf-8")
    payload = json.loads(text)
    if payload.get("source") != "fixture":
        raise ValueError("fixture fallback payload must be labelled source=fixture")
    rows = parse_trading_holidays(text)
    if year is None:
        return rows
    return [row for row in rows if row.date.year == year]
