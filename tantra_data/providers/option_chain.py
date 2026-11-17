"""NSE intraday option-chain provider (gap 0016).

Live source::

    https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol={symbol}

This is the endpoint the current NSE frontend calls (confirmed 2026-11-17 by
reading the live ``dist/js/sections/option-chain-v3.js`` bundle); NSE replaced
the legacy ``https://www.nseindia.com/api/option-chain-indices`` path, which
now returns ``HTTP 404`` "Resource not found". The feed is fetched with a
``curl_cffi`` Chrome-impersonating session plus browser-like headers
(``User-Agent``, ``Referer``, ``Accept-Language``, ``Accept``), matching the
repo's working NSE provider ``providers/institutional_flow.py``.

Verification status (retried 2026-11-17, T12.3 follow-on): still **NOT
verified live** from the sandbox. The legacy ``option-chain-indices`` endpoint
returns ``HTTP 404`` (removed by NSE, see above); the current
``option-chain-v3`` endpoint returns ``200`` with an empty JSON object (``{}``)
even through the full home -> option-chain-page browser flow — the API is gated
by an Akamai JS sensor (``_abck``/``bm_sz`` challenge cookies) that
``curl_cffi`` cannot complete, so the sandbox receives no data. This is the
exact, honest failure: nothing is fabricated and no live success is claimed.

The provider therefore uses the repo's live-with-stub-fallback pattern
(``providers/calendar.py::default_calendar_rows``): it attempts the live fetch
and, on any failure (offline, HTTP error, blocked endpoint, empty payload, or a
malformed payload), falls back to the bundled recorded fixture
``tests/fixtures/option_chain_nifty.json``, which is labelled
``source: fixture`` — never fabricated as live data. The precise live failure
is surfaced as :class:`OptionChainLiveError` through the optional
``on_live_error`` callback of :func:`fetch_option_chain`; the current status is
pinned machine-readably in :data:`LIVE_VERIFICATION`.

Zero-floats invariant: every value is parsed with exact ``decimal.Decimal``
arithmetic, never ``float``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from tantra_data.domain.option_chain import OptionChainSnapshot, StrikePositioning

__all__ = [
    "LIVE_VERIFICATION",
    "OPTION_CHAIN_URL",
    "OptionChainLiveError",
    "fetch_option_chain",
    "fetch_option_chain_fixture",
    "parse_option_chain",
    "read_option_chain",
]

NSE_HOME_URL = "https://www.nseindia.com"
#: Current NSE frontend option-chain endpoint (replaces the removed
#: ``option-chain-indices`` path; NSE returns 404 for the legacy path).
OPTION_CHAIN_URL = f"{NSE_HOME_URL}/api/option-chain-v3"
OPTION_CHAIN_TYPE = "Indices"
REFERER_URL = "https://www.nseindia.com/option-chain"
IST = ZoneInfo("Asia/Kolkata")
NSE_DATE_FORMAT = "%d-%b-%Y"
NSE_TIMESTAMP_FORMAT = "%d-%b-%Y %H:%M:%S"
REQUEST_TIMEOUT_SECONDS = 20

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

#: Current live-verification status of the NSE option-chain feed (updated by the
#: T12.3 follow-on, 2026-11-17). Kept machine-readable so tooling and tests can
#: assert the provider's honest verification state.
LIVE_VERIFICATION = (
    "not-verified-live (retried 2026-11-17): the legacy option-chain-indices "
    "endpoint returns HTTP 404 'Resource not found' (NSE moved to "
    "option-chain-v3?type=Indices&symbol=, confirmed from the live page JS); "
    "option-chain-v3 returns an empty JSON object from this sandbox (Akamai "
    "JS-challenge-gated). Live data is never fabricated: fetch_option_chain "
    "falls back to the labelled fixture and surfaces the precise failure via "
    "on_live_error / OptionChainLiveError."
)

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "option_chain_nifty.json"
)


class OptionChainLiveError(RuntimeError):
    """A precise failure from the live NSE option-chain feed.

    ``status_code`` is the HTTP status when the endpoint responded (``None``
    for transport-level failures such as timeouts); ``symbol`` and ``url``
    identify the request. Callers use this to distinguish a blocked endpoint,
    a moved/removed endpoint (404), an empty/JS-challenge-gated payload (200
    with no ``records``) from a genuinely-populated live response.
    """

    def __init__(
        self,
        message: str,
        *,
        symbol: str,
        url: str,
        status_code: int | None,
    ) -> None:
        super().__init__(message)
        self.symbol = symbol
        self.url = url
        self.status_code = status_code


def parse_option_chain(text: str, symbol: str) -> OptionChainSnapshot:
    """Parse an NSE ``option-chain-v3`` JSON payload into a snapshot.

    The provider snapshots the near expiry (the first entry of
    ``records.expiryDates``); every ``records.data`` row for that expiry
    contributes a call (``CE``) and a put (``PE``) strike positioning. The
    snapshot ``as_of`` is the payload's ``records.timestamp`` (IST) converted
    to UTC; the fixture payload is labelled ``source: fixture``.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("option-chain data is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("option-chain data is not valid JSON")
    records = payload.get("records")
    if not isinstance(records, dict):
        raise ValueError("option-chain payload is missing its records")
    expiry_dates = records.get("expiryDates")
    if not isinstance(expiry_dates, list) or not expiry_dates:
        raise ValueError("option-chain payload has no expiry dates")
    near_expiry_text = cast(str, expiry_dates[0])
    data = records.get("data")
    if not isinstance(data, list):
        raise ValueError("option-chain payload has no data rows")

    expiry = _parse_expiry(near_expiry_text)
    strikes: list[StrikePositioning] = []
    for element in data:
        if not isinstance(element, dict):
            continue
        if element.get("expiryDate") != near_expiry_text:
            continue
        try:
            strike = Decimal(str(element["strikePrice"]))
        except (KeyError, InvalidOperation) as exc:
            raise ValueError(f"option-chain row has an invalid strike price: {element!r}") from exc
        for option_type, leg_name in (("CE", "CE"), ("PE", "PE")):
            leg = element.get(leg_name)
            if not isinstance(leg, dict):
                continue
            strikes.append(_strike_from_leg(strike, option_type, element, leg))
    if not strikes:
        raise ValueError("option-chain payload has no parseable strikes for the near expiry")

    source = payload.get("source")
    return OptionChainSnapshot(
        as_of=_timestamp_from_records(records),
        underlying=symbol,
        expiry=expiry,
        strikes=strikes,
        source=source if isinstance(source, str) else "nse",
    )


def _strike_from_leg(
    strike: Decimal,
    option_type: str,
    element: dict[str, object],
    leg: dict[str, object],
) -> StrikePositioning:
    try:
        return StrikePositioning(
            strike=strike,
            option_type=option_type,
            open_interest=_decimal_field(leg, "openInterest"),
            oi_change=_decimal_field(leg, "changeinOpenInterest"),
            implied_volatility=_decimal_field(leg, "impliedVolatility"),
            delta=_decimal_field(leg, "delta"),
            gamma=_decimal_field(leg, "gamma"),
        )
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise ValueError(f"option-chain row has invalid fields: {element!r}") from exc


def _decimal_field(leg: dict[str, object], name: str) -> Decimal:
    value = leg[name]
    if value is None:
        raise KeyError(name)
    return Decimal(str(value))


def _parse_expiry(text: str) -> date:
    return datetime.strptime(text.strip(), NSE_DATE_FORMAT).date()


def _timestamp_from_records(records: dict[str, object]) -> datetime:
    timestamp = records.get("timestamp")
    if isinstance(timestamp, str):
        try:
            naive = datetime.strptime(timestamp.strip(), NSE_TIMESTAMP_FORMAT)
            return naive.replace(tzinfo=IST).astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def read_option_chain(path: str | Path, symbol: str) -> OptionChainSnapshot:
    """Read and parse an option-chain JSON file from disk."""
    with open(path, encoding="utf-8") as handle:
        return parse_option_chain(handle.read(), symbol)


def fetch_option_chain(
    symbol: str,
    on_live_error: Callable[[OptionChainLiveError], None] | None = None,
) -> OptionChainSnapshot:
    """Fetch the near-expiry option-chain snapshot for ``symbol``.

    Live-with-stub-fallback (mirrors ``providers/calendar.py::
    default_calendar_rows``): attempts the live NSE ``option-chain-v3`` feed
    and, on any failure (offline, HTTP error, blocked endpoint, empty payload,
    or a malformed payload), falls back to the bundled recorded fixture
    (``source: fixture``) so the pipeline stays runnable offline. The fixture
    is never presented as live data.

    The precise live failure is passed to ``on_live_error`` (when given) as an
    :class:`OptionChainLiveError` carrying the ``symbol``, ``url`` and HTTP
    ``status_code`` (``None`` for transport failures), so callers can log or
    alert on blocked-vs-moved-vs-empty without ever fabricating data.
    """
    try:
        return _fetch_live_option_chain(symbol)
    except Exception as exc:
        if not isinstance(exc, OptionChainLiveError):
            exc = OptionChainLiveError(
                f"option-chain live fetch failed for {symbol}: {exc}",
                symbol=symbol,
                url=OPTION_CHAIN_URL,
                status_code=None,
            )
        if on_live_error is not None:
            on_live_error(exc)
        return fetch_option_chain_fixture(symbol)


def _fetch_live_option_chain(symbol: str) -> OptionChainSnapshot:
    """Fetch the live NSE option-chain JSON payload (lazy ``curl_cffi``).

    Every failure mode is wrapped in :class:`OptionChainLiveError`: a non-200
    response (e.g. the removed legacy endpoint), a ``200`` empty/JS-gated
    payload, and transport errors (timeouts, connection errors).
    """
    session: Any = _requests_session()
    session.get(NSE_HOME_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response = session.get(
        OPTION_CHAIN_URL,
        params={"type": OPTION_CHAIN_TYPE, "symbol": symbol},
        headers=LIVE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    status_code: int | None = None
    try:
        status_code = int(response.status_code)
    except (TypeError, ValueError):
        status_code = None
    if status_code != 200:
        detail = ""
        if status_code == 404:
            detail = (
                " (the legacy option-chain-indices path was removed by NSE; "
                "the current frontend endpoint is option-chain-v3)"
            )
        raise OptionChainLiveError(
            f"option-chain live fetch failed with HTTP {status_code} for {symbol}{detail}",
            symbol=symbol,
            url=OPTION_CHAIN_URL,
            status_code=status_code,
        )
    try:
        return parse_option_chain(response.text, symbol)
    except ValueError as exc:
        raise OptionChainLiveError(
            f"option-chain live fetch returned an unusable payload for {symbol}: {exc}",
            symbol=symbol,
            url=OPTION_CHAIN_URL,
            status_code=status_code,
        ) from exc


def _requests_session() -> Any:
    """Create a Chrome-impersonating session (lazy ``curl_cffi`` import).

    Split out so tests can inject a fake transport without importing
    ``curl_cffi``.
    """
    from curl_cffi import requests

    return requests.Session(impersonate="chrome124")


def fetch_option_chain_fixture(symbol: str) -> OptionChainSnapshot:
    """Return the bundled recorded fixture snapshot (``source: fixture``).

    The fixture must be labelled ``source: fixture``; any payload that is not
    is rejected so a mislabelled fallback can never masquerade as live data.
    """
    snapshot = read_option_chain(_FIXTURE_PATH, symbol)
    if snapshot.source != "fixture":
        raise ValueError("fixture fallback payload must be labelled source=fixture")
    return snapshot
