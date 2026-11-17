"""NSE SEBI-PIT insider-trading disclosure provider (gap 0024).

Live source (verified 2026-11-17 from the sandbox)::

    https://www.nseindia.com/api/corporates-pit?index=equities&symbol={SYMBOL}

returns a JSON payload ``{"acqNameList": [...], "data": [...]}`` of structured
insider-trading disclosures for the symbol — 20 rows for RELIANCE on the
verification fetch. Each row carries the insider name, person category, the
covered trade dates, buy/sell quantities and values, the acquisition mode
(``acqMode``), and the before/after shareholdings — exactly what insider-deal
alerts need.

The fetch uses the repo's working ``curl_cffi`` Chrome-impersonating session
pattern with browser-like headers. Live data is never fabricated: the provider
uses the live-with-stub-fallback pattern and falls back to the bundled
recorded fixture ``tests/fixtures/nse_pit_reliance.json`` (labelled
``source: fixture``), whose rows were recorded verbatim from the live payload.
The precise live failure is surfaced through the optional ``on_live_error``
callback as :class:`NsePitLiveError`; the current status is pinned
machine-readably in :data:`LIVE_VERIFICATION`.

SAST (SEBI Substantial Acquisition of Shares and Takeovers) Reg 29/31
disclosures are **not** available as structured rows from any free endpoint
verified in the sandbox: NSE's ``corporate-sast`` / ``sast-data`` paths answer
HTTP 404, and the ``corporate-announcements`` feed carries SAST entries as
attachment-only text. That dataset stays open-and-documented (see the gap
file); this provider covers the structured PIT insider-trading feed only.

Zero-floats invariant: every percentage and money value is parsed as an exact
``decimal.Decimal``; every share count is an exact ``int`` — no ``float``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from tantra_data.domain.shareholding import InsiderDisclosure

__all__ = [
    "LIVE_VERIFICATION",
    "NSE_HOME_URL",
    "PIT_URL",
    "NsePitLiveError",
    "fetch_insider_disclosures",
    "fetch_insider_disclosures_fixture",
    "parse_pit_payload",
]

NSE_HOME_URL = "https://www.nseindia.com"
PIT_URL = f"{NSE_HOME_URL}/api/corporates-pit"
REFERER_URL = "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading"
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

#: Current live-verification status of the NSE PIT feed (checked 2026-11-17
#: from the sandbox): ``corporates-pit?index=equities&symbol=RELIANCE``
#: returned HTTP 200 application/json with 20 structured disclosure rows
#: (acqMode, name, trade/intimation dates, buy/sell quantities and values,
#: before/after holdings). SAST Reg 29/31 stays open: no structured free
#: endpoint verified (``corporate-sast``/``sast-data`` are HTTP 404; the
#: announcements feed carries SAST as attachment-only text).
LIVE_VERIFICATION = (
    "verified-live (checked 2026-11-17): nseindia.com/api/corporates-pit"
    "?index=equities&symbol=RELIANCE returned HTTP 200 application/json with "
    "20 structured PIT disclosure rows. SAST (SEBI Substantial Acq of shares "
    "& Takeovers) Reg 29/31 stays open-and-documented: no structured free "
    "source verified (NSE sast endpoints HTTP 404; announcements feed is "
    "attachment-only)."
)

_FIXTURE_PATH = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nse_pit_reliance.json"


class NsePitLiveError(RuntimeError):
    """A precise failure from the live NSE PIT feed.

    ``status_code`` is the HTTP status when the endpoint responded (``None``
    for transport-level failures such as timeouts); ``url`` identifies the
    request; ``symbol`` is the requested symbol.
    """

    def __init__(
        self,
        message: str,
        *,
        url: str,
        status_code: int | None,
        symbol: str,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.symbol = symbol


def _parse_nse_date(text: str) -> datetime:
    """Parse an NSE ``DD-MMM-YYYY`` date string."""
    return datetime.strptime(text.strip(), DATE_FORMAT)


def parse_pit_payload(text: str, symbol: str, source: str = "nse") -> list[InsiderDisclosure]:
    """Parse the NSE ``corporates-pit`` JSON payload for ``symbol``.

    The verified live payload is an object with ``acqNameList`` and ``data``
    arrays; each data row is shaped like::

        {"acqMode": "Off Market", "acqName": "BALANADU NARAYAN",
         "acqfromDt": "13-Feb-2026", "intimDt": "16-Feb-2026",
         "personCategory": "Other", "tdpTransactionType": "Sell",
         "secAcq": "2320", "buyQuantity": "0", "sellquantity": "0",
         "secVal": "3294168", "befAcqSharesNo": "3920",
         "befAcqSharesPer": "0", "afterAcqSharesNo": "1600",
         "afterAcqSharesPer": "0", "company": "Reliance Industries Limited",
         "did": "563850", ...}

    ``source`` labels the rows (``"nse"`` for live payloads, ``"fixture"`` for
    the recorded fallback). Rows missing the trade date, intimation date, or a
    usable transaction type are skipped (they carry no usable as-of). Raises
    ``ValueError`` on malformed input.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("NSE PIT data is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("NSE PIT data must be a JSON object")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("NSE PIT payload is missing its data array")
    disclosures: list[InsiderDisclosure] = []
    for row in rows:
        disclosure = _try_parse_pit_row(row, symbol, source)
        if disclosure is not None:
            disclosures.append(disclosure)
    return disclosures


def _try_parse_pit_row(element: object, symbol: str, source: str) -> InsiderDisclosure | None:
    """Parse one PIT row, or return ``None`` when the row is unusable."""
    if not isinstance(element, dict):
        raise ValueError("NSE PIT rows must be JSON objects")
    trade_text = element.get("acqfromDt")
    intim_text = element.get("intimDt")
    name = element.get("acqName")
    transaction_raw = element.get("tdpTransactionType")
    if not isinstance(trade_text, str) or not isinstance(intim_text, str):
        return None
    if not isinstance(name, str) or not name.strip():
        return None
    if not isinstance(transaction_raw, str) or not transaction_raw.strip():
        return None
    trade_date = _parse_nse_date(trade_text).date()
    intimation_date = _parse_nse_date(intim_text).date()
    try:
        disclosure = InsiderDisclosure(
            instrument=str(element.get("symbol", symbol)),
            company_name=str(element.get("company", "")).strip(),
            disclosure_id=int(str(element.get("did", "0")).strip()),
            trade_date=trade_date,
            intimation_date=intimation_date,
            name=name.strip(),
            person_category=str(element.get("personCategory", "")).strip(),
            transaction_type=transaction_raw,
            acq_mode=str(element.get("acqMode", "")).strip(),
            acquired_shares=_pit_int(element.get("secAcq")),
            buy_quantity=_pit_int(element.get("buyQuantity")),
            sell_quantity=_pit_int(element.get("sellquantity")),
            value=Decimal(_pit_num(element.get("secVal"))),
            before_shares=_pit_int(element.get("befAcqSharesNo")),
            before_pct=Decimal(_pit_num(element.get("befAcqSharesPer"))),
            after_shares=_pit_int(element.get("afterAcqSharesNo")),
            after_pct=Decimal(_pit_num(element.get("afterAcqSharesPer"))),
            source=source,
        )
    except (ValueError, InvalidOperation, TypeError):
        return None
    return disclosure


def _pit_int(value: object) -> int:
    """NSE PIT share counts: ``"Nil"``/``"-"``/empty mean zero (not reported)."""
    text = _pit_num(value)
    return int(text)


def _pit_num(value: object) -> str:
    """Normalize an NSE PIT numeric field: ``"Nil"``/``"-"``/empty/``None``
    become ``"0"``; otherwise the stripped text is kept verbatim."""
    if value is None:
        return "0"
    text = str(value).strip()
    if not text or text.upper() == "NIL" or text == "-":
        return "0"
    return text


def _requests_session() -> Any:
    """Create a Chrome-impersonating session (lazy ``curl_cffi`` import)."""
    from curl_cffi import requests

    return requests.Session(impersonate="chrome124")


def fetch_insider_disclosures(
    symbol: str,
    on_live_error: Callable[[NsePitLiveError], None] | None = None,
) -> list[InsiderDisclosure]:
    """Fetch the PIT insider-trading disclosures for ``symbol``.

    Live-with-stub-fallback (mirrors ``providers/trading_holidays.py``): on
    any live failure the provider falls back to the bundled recorded fixture
    (``source: fixture``), surfacing the precise failure via ``on_live_error``
    as :class:`NsePitLiveError`.
    """
    try:
        return _fetch_live_pit_disclosures(symbol)
    except Exception as exc:
        if not isinstance(exc, NsePitLiveError):
            exc = NsePitLiveError(
                f"NSE PIT live fetch failed: {exc}",
                url=PIT_URL,
                status_code=None,
                symbol=symbol,
            )
        if on_live_error is not None:
            on_live_error(exc)
        return fetch_insider_disclosures_fixture(symbol)


def _fetch_live_pit_disclosures(symbol: str) -> list[InsiderDisclosure]:
    """Fetch the live NSE PIT payload (lazy ``curl_cffi``)."""
    session: Any = _requests_session()
    try:
        session.get(NSE_HOME_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        raise NsePitLiveError(
            f"NSE home fetch failed: {exc}", url=NSE_HOME_URL, status_code=None, symbol=symbol
        ) from exc
    status_code: int | None = None
    try:
        response = session.get(
            PIT_URL,
            params={"index": "equities", "symbol": symbol},
            headers=LIVE_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        status_code = int(response.status_code)
    except Exception as exc:
        raise NsePitLiveError(
            f"NSE PIT fetch failed: {exc}", url=PIT_URL, status_code=None, symbol=symbol
        ) from exc
    if status_code != 200:
        raise NsePitLiveError(
            f"NSE PIT fetch failed with HTTP {status_code} for symbol={symbol}",
            url=PIT_URL,
            status_code=status_code,
            symbol=symbol,
        )
    try:
        return parse_pit_payload(response.text, symbol)
    except ValueError as exc:
        raise NsePitLiveError(
            f"NSE PIT fetch returned an unusable payload: {exc}",
            url=PIT_URL,
            status_code=status_code,
            symbol=symbol,
        ) from exc


def fetch_insider_disclosures_fixture(symbol: str | None = None) -> list[InsiderDisclosure]:
    """Return the bundled recorded fixture rows (``source: fixture``).

    The bundled fixture covers RELIANCE. When ``symbol`` is given and differs
    from the fixture's symbol, an empty list is returned (the recorded rows
    would not belong to that symbol — never fabricated). Returned rows carry
    the recorded ``source: fixture`` label and the recorded symbol.
    """
    payload = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    if payload.get("source") != "fixture":
        raise ValueError("fixture fallback payload must be labelled source=fixture")
    fixture_symbol = str(payload.get("symbol", "RELIANCE"))
    if symbol is not None and symbol.upper() != fixture_symbol.upper():
        return []
    return parse_pit_payload(
        _FIXTURE_PATH.read_text(encoding="utf-8"), fixture_symbol, source="fixture"
    )
