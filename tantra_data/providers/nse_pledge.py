"""NSE promoter-pledge / encumbrance provider (gap 0024).

Live source (verified 2026-11-17 from the sandbox)::

    https://www.nseindia.com/api/corporate-pledgedata?index=equities

returns a JSON payload ``{"comNameList": [...], "data": [...]}`` with one
pledge snapshot per listed company — 1,555 companies on the verification
fetch, each row carrying the shareholding-pattern quarter end (``shp``), the
broadcast datetime (``broadcastDt``), promoter holding percentage, and the
number/percentage of promoter shares pledged/encumbered.

The fetch uses the repo's working ``curl_cffi`` Chrome-impersonating session
pattern with browser-like headers. Live data is never fabricated: the provider
uses the live-with-stub-fallback pattern and falls back to the bundled
recorded fixture ``tests/fixtures/nse_pledgedata.json`` (labelled
``source: fixture``), whose rows were recorded verbatim from the live
payload. The precise live failure is surfaced through the optional
``on_live_error`` callback as :class:`NsePledgeLiveError`; the current status
is pinned machine-readably in :data:`LIVE_VERIFICATION`.

Zero-floats invariant: every percentage is parsed as an exact
``decimal.Decimal``; every share count is an exact ``int`` — no ``float``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from tantra_data.domain.shareholding import PledgeRecord

__all__ = [
    "LIVE_VERIFICATION",
    "NSE_HOME_URL",
    "PLEDGE_URL",
    "NsePledgeLiveError",
    "fetch_pledge_records",
    "fetch_pledge_records_fixture",
    "parse_pledge_payload",
]

NSE_HOME_URL = "https://www.nseindia.com"
PLEDGE_URL = f"{NSE_HOME_URL}/api/corporate-pledgedata"
REFERER_URL = "https://www.nseindia.com/companies-listing/corporate-filings-pledged-data"
REQUEST_TIMEOUT_SECONDS = 30
DATE_FORMAT = "%d-%b-%Y"

LIVE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": REFERER_URL,
    "Accept": "application/json",
}

#: Current live-verification status of the NSE pledge feed (checked
#: 2026-11-17 from the sandbox): ``corporate-pledgedata?index=equities``
#: returned HTTP 200 application/json with 1,555 companies / 1,558 pledge
#: rows. Kept machine-readable so tooling and tests can assert the provider's
#: honest verification state.
LIVE_VERIFICATION = (
    "verified-live (checked 2026-11-17): nseindia.com/api/corporate-pledgedata"
    "?index=equities returned HTTP 200 application/json with 1,555 company "
    "pledge snapshots (rows carry shp quarter end, broadcastDt, "
    "percPromoterHolding, numSharesPledged, percSharesPledged)."
)

_FIXTURE_PATH = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nse_pledgedata.json"


class NsePledgeLiveError(RuntimeError):
    """A precise failure from the live NSE pledge feed.

    ``status_code`` is the HTTP status when the endpoint responded (``None``
    for transport-level failures such as timeouts); ``url`` identifies the
    request.
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


def _parse_nse_date(text: str) -> date:
    """Parse an NSE ``DD-MMM-YYYY`` date string.

    The source rows carry datetime strings like ``"17-Nov-2026 16:31:12"``;
    only the leading ``DD-MMM-YYYY`` token is significant.
    """
    token = text.strip().split(" ", 1)[0]
    return datetime.strptime(token, DATE_FORMAT).date()


def parse_pledge_payload(text: str, source: str = "nse") -> list[PledgeRecord]:
    """Parse the NSE ``corporate-pledgedata`` JSON payload.

    The verified live payload is an object with ``comNameList`` and ``data``
    arrays; each data row is shaped like::

        {"comName": "20 Microns Limited", "shp": "30-Sep-2026",
         "broadcastDt": "17-Nov-2026 16:31:12", "numSharesPledged": "1559600",
         "percSharesPledged": "4.42", "percPromoterHolding": "    45.04",
         "totPromoterHolding": "15893364", "totIssuedShares": "35286502", ...}

    ``source`` labels the rows (``"nse"`` for live payloads, ``"fixture"`` for
    the recorded fallback). Rows missing the quarter end (``shp``) or the
    broadcast date are skipped (they carry no usable as-of). Raises
    ``ValueError`` on malformed input.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("NSE pledge data is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("NSE pledge data must be a JSON object")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("NSE pledge payload is missing its data array")
    usable = [row for row in rows if _is_usable_pledge_row(row)]
    return [_parse_pledge_row(row, source) for row in usable]


def _is_usable_pledge_row(element: object) -> bool:
    """A pledge row is usable when it carries the company name and the two
    as-of dates (quarter end ``shp`` and ``broadcastDt``)."""
    return (
        isinstance(element, dict)
        and bool(str(element.get("comName", "")).strip())
        and isinstance(element.get("shp"), str)
        and isinstance(element.get("broadcastDt"), str)
    )


def _parse_pledge_row(element: object, source: str) -> PledgeRecord:
    if not isinstance(element, dict):
        raise ValueError("NSE pledge rows must be JSON objects")
    company = str(element.get("comName", "")).strip()
    shp_text = element.get("shp")
    broadcast_text = element.get("broadcastDt")
    if not company or not isinstance(shp_text, str) or not isinstance(broadcast_text, str):
        raise ValueError(f"NSE pledge row missing company/date fields: {element!r}")
    quarter_end = _parse_nse_date(shp_text)
    broadcast_date = _parse_nse_date(broadcast_text)
    try:
        pledged_shares = int(str(element.get("numSharesPledged", "0")).strip())
        pledged_pct = Decimal(str(element.get("percSharesPledged", "0")).strip())
        promoter_holding_pct = Decimal(str(element.get("percPromoterHolding", "0")).strip())
        promoter_shares = int(str(element.get("totPromoterHolding", "0")).strip())
        total_issued_shares = int(str(element.get("totIssuedShares", "0")).strip())
    except (ValueError, InvalidOperation, TypeError) as exc:
        raise ValueError(f"NSE pledge row has invalid numeric fields: {element!r}") from exc
    return PledgeRecord(
        instrument=company,
        company_name=company,
        quarter_end=quarter_end,
        broadcast_date=broadcast_date,
        promoter_holding_pct=promoter_holding_pct,
        pledged_shares=pledged_shares,
        pledged_pct=pledged_pct,
        promoter_shares=promoter_shares,
        total_issued_shares=total_issued_shares,
        source=source,
    )


def _requests_session() -> Any:
    """Create a Chrome-impersonating session (lazy ``curl_cffi`` import)."""
    from curl_cffi import requests

    return requests.Session(impersonate="chrome124")


def fetch_pledge_records(
    on_live_error: Callable[[NsePledgeLiveError], None] | None = None,
) -> list[PledgeRecord]:
    """Fetch the latest NSE promoter-pledge snapshot for every company.

    Live-with-stub-fallback (mirrors ``providers/trading_holidays.py``): on
    any live failure (offline, blocked endpoint, HTTP error, or a malformed
    payload) the provider falls back to the bundled recorded fixture
    (``source: fixture``), surfacing the precise failure via ``on_live_error``
    as :class:`NsePledgeLiveError`.
    """
    try:
        return _fetch_live_pledge_records()
    except Exception as exc:
        if not isinstance(exc, NsePledgeLiveError):
            exc = NsePledgeLiveError(
                f"NSE pledge live fetch failed: {exc}",
                url=PLEDGE_URL,
                status_code=None,
            )
        if on_live_error is not None:
            on_live_error(exc)
        return fetch_pledge_records_fixture()


def _fetch_live_pledge_records() -> list[PledgeRecord]:
    """Fetch the live NSE pledge payload (lazy ``curl_cffi``)."""
    session: Any = _requests_session()
    try:
        session.get(NSE_HOME_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        raise NsePledgeLiveError(
            f"NSE home fetch failed: {exc}", url=NSE_HOME_URL, status_code=None
        ) from exc
    status_code: int | None = None
    try:
        response = session.get(
            PLEDGE_URL,
            params={"index": "equities"},
            headers=LIVE_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        status_code = int(response.status_code)
    except Exception as exc:
        raise NsePledgeLiveError(
            f"NSE pledge fetch failed: {exc}", url=PLEDGE_URL, status_code=None
        ) from exc
    if status_code != 200:
        raise NsePledgeLiveError(
            f"NSE pledge fetch failed with HTTP {status_code}",
            url=PLEDGE_URL,
            status_code=status_code,
        )
    try:
        return parse_pledge_payload(response.text)
    except ValueError as exc:
        raise NsePledgeLiveError(
            f"NSE pledge fetch returned an unusable payload: {exc}",
            url=PLEDGE_URL,
            status_code=status_code,
        ) from exc


def fetch_pledge_records_fixture() -> list[PledgeRecord]:
    """Return the bundled recorded fixture rows (``source: fixture``).

    The fixture payload must be labelled ``source: fixture``; any payload that
    is not is rejected so a mislabelled fallback can never masquerade as live
    data. Rows are labelled ``source: fixture``.
    """
    text = _FIXTURE_PATH.read_text(encoding="utf-8")
    payload = json.loads(text)
    if payload.get("source") != "fixture":
        raise ValueError("fixture fallback payload must be labelled source=fixture")
    return parse_pledge_payload(text, source="fixture")
