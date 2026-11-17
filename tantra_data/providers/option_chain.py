"""NSE intraday option-chain provider (gap 0016).

Live source::

    https://www.nseindia.com/api/option-chain-indices?symbol={symbol}

with a ``curl_cffi`` Chrome-impersonating session and the NSE ``Referer``
header, matching ``providers/institutional_flow.py``.

Verification status: the live ``option-chain-indices`` endpoint returned
``HTTP 404`` from the sandbox on 2026-11-16/17 (``option-chain-equities`` was
reachable, but the indices endpoint was not), so the live feed could NOT be
verified here. The provider therefore uses the repo's live-with-stub-fallback
pattern (``providers/calendar.py::default_calendar_rows``): it attempts the
live fetch and, on any failure (offline, HTTP error, blocked endpoint, or a
malformed payload), falls back to the bundled recorded fixture
``tests/fixtures/option_chain_nifty.json``, which is labelled
``source: fixture`` — never fabricated as live data.

Zero-floats invariant: every value is parsed with exact ``decimal.Decimal``
arithmetic, never ``float``.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from tantra_data.domain.option_chain import OptionChainSnapshot, StrikePositioning

__all__ = [
    "fetch_option_chain",
    "fetch_option_chain_fixture",
    "parse_option_chain",
    "read_option_chain",
]

NSE_HOME_URL = "https://www.nseindia.com"
OPTION_CHAIN_URL = f"{NSE_HOME_URL}/api/option-chain-indices"
REFERER_URL = "https://www.nseindia.com/option-chain"
IST = ZoneInfo("Asia/Kolkata")
NSE_DATE_FORMAT = "%d-%b-%Y"
NSE_TIMESTAMP_FORMAT = "%d-%b-%Y %H:%M:%S"

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "option_chain_nifty.json"
)


def parse_option_chain(text: str, symbol: str) -> OptionChainSnapshot:
    """Parse an NSE ``option-chain-indices`` JSON payload into a snapshot.

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


def fetch_option_chain(symbol: str) -> OptionChainSnapshot:
    """Fetch the near-expiry option-chain snapshot for ``symbol``.

    Live-with-stub-fallback (mirrors ``providers/calendar.py::
    default_calendar_rows``): attempts the live NSE ``option-chain-indices``
    feed and, on any failure, falls back to the bundled recorded fixture
    (``source: fixture``) so the pipeline stays runnable offline. The fixture
    is never presented as live data.
    """
    try:
        return _fetch_live_option_chain(symbol)
    except Exception:
        return fetch_option_chain_fixture(symbol)


def _fetch_live_option_chain(symbol: str) -> OptionChainSnapshot:
    """Fetch the live NSE option-chain JSON payload (lazy ``curl_cffi``)."""
    from curl_cffi import requests

    session: requests.Session = requests.Session(impersonate="chrome124")
    session.get(NSE_HOME_URL, timeout=20)
    response = session.get(
        OPTION_CHAIN_URL,
        params={"symbol": symbol},
        headers={"Accept": "application/json", "Referer": REFERER_URL},
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f"option-chain fetch failed with HTTP {response.status_code}")
    return parse_option_chain(response.text, symbol)


def fetch_option_chain_fixture(symbol: str) -> OptionChainSnapshot:
    """Return the bundled recorded fixture snapshot (``source: fixture``).

    The fixture must be labelled ``source: fixture``; any payload that is not
    is rejected so a mislabelled fallback can never masquerade as live data.
    """
    snapshot = read_option_chain(_FIXTURE_PATH, symbol)
    if snapshot.source != "fixture":
        raise ValueError("fixture fallback payload must be labelled source=fixture")
    return snapshot
