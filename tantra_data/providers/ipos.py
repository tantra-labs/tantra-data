"""NSE primary-market IPO calendar + subscription provider (gap 0022).

Fetches the official NSE IPO feeds (``/api/ipo-current-issue``,
``/api/all-upcoming-issues?category=ipo``, ``/api/public-past-issues`` and the
per-issue ``/api/ipo-detail`` endpoint) via ``curl_cffi`` (the exchange's
cookie challenge blocks plain ``requests``/``curl``, mirroring
``providers/institutional_flow.py``) and maps them onto canonical
:class:`~tantra_data.domain.ipos.IpoIssue` /
:class:`~tantra_data.domain.ipos.IpoSubscription` rows.

Verified live (2026-11-14): all four endpoints return HTTP 200 behind a
chrome-impersonated session. ``/api/ipo-current-issue`` and
``/api/all-upcoming-issues`` returned empty lists at probe time (no issue open
or booked that Saturday); ``/api/public-past-issues`` returned the full listed
calendar (e.g. ``MAXENGS``, ``VPFL``) with rows shaped ``{companyName,
ipoStartDate, ipoEndDate, issuePrice, listingDate, priceRange, securityType,
symbol}``; ``/api/ipo-detail`` returned ``issueInfo.dataList`` (title/value
rows: Issue Period, Price Range, Lot Size, Issue Size) and ``activeCat.dataList``
(category-wise subscription rows with ``srNo`` 1=QIB, 2=NII, 2.1=b-NII,
2.2=s-NII, 3=RII, and a Total row).

Zero-floats invariant: every price-band endpoint, lot size, issue size, and
subscription multiple is converted to an exact ``decimal.Decimal`` at the
parse boundary (``Decimal(str(value))``) — never ``float``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol

from tantra_data.domain.ipos import IpoIssue, IpoSubscription

__all__ = [
    "IpoIssueRow",
    "IpoSubscriptionRow",
    "ParsedIpo",
    "fetch_ipo_issues",
    "ingest_ipos",
    "parse_ipo_issues",
    "rows_to_issues",
    "rows_to_subscriptions",
]

NSE_HOME_URL = "https://www.nseindia.com"
NSE_IPO_REFERER = "https://www.nseindia.com/market-data/all-upcoming-issues-ipo"
CURRENT_ISSUES_URL = f"{NSE_HOME_URL}/api/ipo-current-issue"
UPCOMING_ISSUES_URL = f"{NSE_HOME_URL}/api/all-upcoming-issues?category=ipo"
PAST_ISSUES_URL = f"{NSE_HOME_URL}/api/public-past-issues"
IPO_DETAIL_URL = f"{NSE_HOME_URL}/api/ipo-detail"
SOURCE = "nse"

_DATE_FORMATS = ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y")
_NUMBER_RE = re.compile(r"[\d,]+(?:\.\d+)?")
_CRORE_RE = re.compile(r"Rs\.?\s*([\d,]+(?:\.\d+)?)\s*(?:Crore|Cr\b)", re.IGNORECASE)

_HEADERS = {
    "Accept": "application/json",
    "Referer": NSE_IPO_REFERER,
}


@dataclass(frozen=True)
class IpoIssueRow:
    """One parsed issue row (vendor-shaped, before domain mapping)."""

    symbol: str
    company_name: str
    board: str
    status: str
    open_date: date
    close_date: date
    listing_date: date | None
    price_band_low: Decimal
    price_band_high: Decimal
    issue_price: Decimal | None
    lot_size: Decimal | None
    issue_size_cr: Decimal | None
    as_of_date: date


@dataclass(frozen=True)
class IpoSubscriptionRow:
    """One parsed subscription snapshot (vendor-shaped, before domain mapping)."""

    symbol: str
    as_of_date: date
    qib: Decimal | None
    nii: Decimal | None
    bnii: Decimal | None
    snii: Decimal | None
    rii: Decimal | None
    total: Decimal | None


@dataclass(frozen=True)
class ParsedIpo:
    """Canonical parsed rows from one NSE IPO payload."""

    as_of_date: date
    issue_rows: list[IpoIssueRow]
    subscription_rows: list[IpoSubscriptionRow]


def _parse_date(value: object) -> date:
    """Parse a date from an ISO or NSE ``dd-MMM-yyyy`` string / date object."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    # NSE month tokens are uppercase (``04-NOV-2026``); title-case and retry.
    titled = text.title()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(titled, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unsupported IPO date: {text!r}")


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    if str(value).strip() == "-":
        return None
    return _parse_date(value)


def _required_date(value: object) -> date | None:
    """Parse a required date, or ``None`` when the vendor marks it missing."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in ("", "-"):
        return None
    try:
        return _parse_date(value)
    except ValueError:
        return None


def _extract_numbers(text: str) -> list[Decimal]:
    """Extract all decimal numbers from vendor text (commas stripped)."""
    return [Decimal(match.group().replace(",", "")) for match in _NUMBER_RE.finditer(text)]


def _optional_number(value: object) -> Decimal | None:
    """Convert a vendor value / text to an exact ``Decimal``, or ``None``."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if not text:
        return None
    numbers = _extract_numbers(text)
    if not numbers:
        return None
    return numbers[0]


def _required_number(value: object, field: str) -> Decimal:
    number = _optional_number(value)
    if number is None:
        raise ValueError(f"invalid {field!r} value in IPO payload: {value!r}")
    return number


def _band_from_raw(raw: dict[object, object]) -> tuple[Decimal, Decimal] | None:
    """Resolve the price band from explicit low/high or a raw band string."""
    low_raw = raw.get("price_band_low")
    high_raw = raw.get("price_band_high")
    if low_raw is not None and high_raw is not None:
        low = _required_number(low_raw, "price_band_low")
        high = _required_number(high_raw, "price_band_high")
        return low, high
    band_text = raw.get("price_band")
    if band_text is None:
        return None
    numbers = _extract_numbers(str(band_text))
    if not numbers:
        return None
    if len(numbers) == 1:
        return numbers[0], numbers[0]
    return numbers[0], numbers[-1]


def _parse_issue_row(raw: dict[object, object], as_of: date) -> IpoIssueRow | None:
    """Parse one issue dict; rows without a key, dates, or price band are skipped."""
    symbol = raw.get("symbol")
    company = raw.get("company_name")
    if not isinstance(symbol, str) or not symbol.strip():
        return None
    if not isinstance(company, str) or not company.strip():
        return None
    open_date = _required_date(raw.get("open_date"))
    close_date = _required_date(raw.get("close_date"))
    if open_date is None or close_date is None:
        return None
    band = _band_from_raw(raw)
    if band is None:
        return None
    low, high = band
    return IpoIssueRow(
        symbol=symbol.strip(),
        company_name=company.strip(),
        board=str(raw.get("board") or "EQ"),
        status=str(raw.get("status") or "upcoming"),
        open_date=open_date,
        close_date=close_date,
        listing_date=_optional_date(raw.get("listing_date")),
        price_band_low=low,
        price_band_high=high,
        issue_price=_optional_number(raw.get("issue_price")),
        lot_size=_optional_number(raw.get("lot_size")),
        issue_size_cr=_optional_number(raw.get("issue_size_cr")),
        as_of_date=as_of,
    )


def _parse_subscription_row(raw: dict[object, object], as_of: date) -> IpoSubscriptionRow | None:
    """Parse one subscription dict; rows without a symbol are skipped."""
    symbol = raw.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return None
    sub_as_of = _optional_date(raw.get("as_of_date")) or as_of
    return IpoSubscriptionRow(
        symbol=symbol.strip(),
        as_of_date=sub_as_of,
        qib=_optional_number(raw.get("qib")),
        nii=_optional_number(raw.get("nii")),
        bnii=_optional_number(raw.get("bnii")),
        snii=_optional_number(raw.get("snii")),
        rii=_optional_number(raw.get("rii")),
        total=_optional_number(raw.get("total")),
    )


def parse_ipo_issues(payload: object) -> ParsedIpo:
    """Deterministically parse a documented NSE IPO payload.

    The documented payload shape is::

        {
          "as_of_date": "2026-11-14",
          "issues": [
            {
              "symbol": "MAXENGS",
              "company_name": "Maxwell Engineering Solutions Limited",
              "board": "SME",                 # EQ | SME
              "status": "listed",             # open | upcoming | listed
              "open_date": "30-Oct-2026",
              "close_date": "03-Nov-2026",
              "listing_date": "06-Nov-2026",
              "price_band": "Rs.150 to Rs.158",   # optional raw band string
              "price_band_low": "150",            # explicit low/high win when present
              "price_band_high": "158",
              "issue_price": "158",
              "lot_size": "800",
              "issue_size_cr": "20.72",
            }, ...
          ],
          "subscriptions": [
            {
              "symbol": "MAXENGS",
              "as_of_date": "2026-11-14",
              "qib": "0.00", "nii": "0.00", "bnii": "0.00",
              "snii": "0.00", "rii": "0.00", "total": "0.00",
            }, ...
          ],
        }

    Every numeric value is converted to an exact ``Decimal`` at the parse
    boundary; rows without a usable key or price band are skipped (never
    fabricated).
    """
    if not isinstance(payload, dict):
        raise ValueError("IPO payload must be a dict")
    as_of = _parse_date(payload.get("as_of_date") or date.today())
    issue_rows: list[IpoIssueRow] = []
    raw_issues = payload.get("issues")
    if isinstance(raw_issues, list):
        for raw in raw_issues:
            if not isinstance(raw, dict):
                continue
            row = _parse_issue_row(raw, as_of)
            if row is not None:
                issue_rows.append(row)
    subscription_rows: list[IpoSubscriptionRow] = []
    raw_subscriptions = payload.get("subscriptions")
    if isinstance(raw_subscriptions, list):
        for raw in raw_subscriptions:
            if not isinstance(raw, dict):
                continue
            sub_row = _parse_subscription_row(raw, as_of)
            if sub_row is not None:
                subscription_rows.append(sub_row)
    return ParsedIpo(
        as_of_date=as_of,
        issue_rows=issue_rows,
        subscription_rows=subscription_rows,
    )


def rows_to_issues(rows: list[IpoIssueRow]) -> list[IpoIssue]:
    """Map parsed issue rows onto canonical domain values."""
    return [
        IpoIssue(
            symbol=row.symbol,
            company_name=row.company_name,
            board=row.board,
            status=row.status,
            open_date=row.open_date,
            close_date=row.close_date,
            listing_date=row.listing_date,
            price_band_low=row.price_band_low,
            price_band_high=row.price_band_high,
            issue_price=row.issue_price,
            lot_size=row.lot_size,
            issue_size_cr=row.issue_size_cr,
            as_of_date=row.as_of_date,
            source=SOURCE,
        )
        for row in rows
    ]


def rows_to_subscriptions(rows: list[IpoSubscriptionRow]) -> list[IpoSubscription]:
    """Map parsed subscription rows onto canonical domain values."""
    return [
        IpoSubscription(
            symbol=row.symbol,
            as_of_date=row.as_of_date,
            qib=row.qib,
            nii=row.nii,
            bnii=row.bnii,
            snii=row.snii,
            rii=row.rii,
            total=row.total,
            source=SOURCE,
        )
        for row in rows
    ]


def _normalize_board(value: object | None) -> str:
    if value is None:
        return "EQ"
    text = str(value).strip().upper()
    return "SME" if text == "SME" else "EQ"


def _list_issue_to_dict(raw: object, status: str) -> dict[object, object]:
    """Map one raw NSE list-row dict onto the documented issue shape."""
    if not isinstance(raw, dict):
        return {}
    return {
        "symbol": raw.get("symbol") or raw.get("htmSym"),
        "company_name": raw.get("companyName") or raw.get("company"),
        "board": _normalize_board(raw.get("securityType") or raw.get("series")),
        "status": status,
        "open_date": raw.get("issueStartDate") or raw.get("ipoStartDate"),
        "close_date": raw.get("issueEndDate") or raw.get("ipoEndDate"),
        "listing_date": raw.get("listingDate"),
        "price_band": raw.get("priceRange") or raw.get("priceBand"),
        "issue_price": raw.get("issuePrice"),
        "lot_size": raw.get("lotSize"),
        "issue_size_cr": raw.get("issueSize"),
    }


def _extract_crore(text: str) -> str | None:
    """Extract a ``Rs. X Crore`` figure from issue-size text (commas stripped)."""
    match = _CRORE_RE.search(text)
    if match is None:
        return None
    return match.group(1).replace(",", "")


def _get_json(session: Any, url: str, headers: dict[str, str]) -> object:
    """GET ``url`` with the cookie-challenged NSE session and return JSON."""
    response = session.get(url, headers=headers, timeout=20)
    if response.status_code != 200:
        raise RuntimeError(f"IPO fetch failed with HTTP {response.status_code} for {url}")
    return response.json()


def _fetch_detail(session: Any, symbol: str, board: str, headers: dict[str, str]) -> object | None:
    """Best-effort per-issue detail fetch; ``None`` when the endpoint fails."""
    try:
        response = session.get(
            IPO_DETAIL_URL,
            params={"symbol": symbol, "series": board},
            headers=headers,
            timeout=20,
        )
        if response.status_code != 200:
            return None
        return response.json()
    except Exception:
        return None


def _detail_issue_fields(detail: object, issue: dict[object, object]) -> dict[object, object]:
    """Extract lot size / issue size / band / dates from the detail endpoint."""
    fields: dict[object, object] = {}
    info: dict[str, str] = {}
    if isinstance(detail, dict):
        issue_info = detail.get("issueInfo")
        data_list = issue_info.get("dataList", []) if isinstance(issue_info, dict) else []
        for row in data_list:
            if isinstance(row, dict) and row.get("title"):
                title = str(row["title"])
                value = row.get("value")
                if value is not None:
                    info[title] = str(value)
    if not issue.get("price_band") and info.get("Price Range"):
        fields["price_band"] = info["Price Range"]
    if issue.get("lot_size") is None and info.get("Lot Size"):
        fields["lot_size"] = info["Lot Size"]
    if issue.get("issue_size_cr") is None and info.get("Issue Size"):
        crore = _extract_crore(info["Issue Size"])
        if crore is not None:
            fields["issue_size_cr"] = crore
    period = info.get("Issue Period")
    if isinstance(period, str) and " to " in period:
        start_text, end_text = (part.strip() for part in period.split(" to ", 1))
        if not issue.get("open_date"):
            fields["open_date"] = start_text
        if not issue.get("close_date"):
            fields["close_date"] = end_text
    return fields


def _detail_subscription(detail: object, symbol: str, as_of: date) -> dict[object, object] | None:
    """Map the detail endpoint's ``activeCat`` rows onto a subscription dict."""
    values: dict[str, object] = {}
    if not isinstance(detail, dict):
        return None
    active_cat = detail.get("activeCat")
    data_list = active_cat.get("dataList", []) if isinstance(active_cat, dict) else []
    for row in data_list:
        if not isinstance(row, dict):
            continue
        sr_no = row.get("srNo")
        category = str(row.get("category") or "").strip()
        if sr_no is None or sr_no == "Sr.No." or category == "Category":
            if sr_no in (None, "") and category == "Total":
                values["total"] = row.get("noOfTotalMeant")
            continue
        sr_text = str(sr_no).strip()
        if sr_text == "1":
            values["qib"] = row.get("noOfTotalMeant")
        elif sr_text == "2":
            values["nii"] = row.get("noOfTotalMeant")
        elif sr_text == "2.1":
            values["bnii"] = row.get("noOfTotalMeant")
        elif sr_text == "2.2":
            values["snii"] = row.get("noOfTotalMeant")
        elif sr_text == "3":
            values["rii"] = row.get("noOfTotalMeant")
    return {
        "symbol": symbol,
        "as_of_date": as_of.isoformat(),
        "qib": values.get("qib"),
        "nii": values.get("nii"),
        "bnii": values.get("bnii"),
        "snii": values.get("snii"),
        "rii": values.get("rii"),
        "total": values.get("total"),
    }


def fetch_ipo_issues(as_of: date | None = None) -> ParsedIpo:
    """Fetch the NSE primary-market feeds and parse them into canonical rows.

    ``curl_cffi`` with Chrome impersonation is required because the bare
    ``www.nseindia.com`` endpoints block plain ``requests``/``curl``; it is
    imported lazily so this module imports without it. Open/upcoming issues
    and issues listed within the last 30 days are enriched best-effort from
    the per-issue detail endpoint (lot size, issue size, subscription
    multiples) — a detail failure never aborts the fetch, and older listed
    issues are carried with list-level fields only so the past-issues feed
    is not hammered request-by-request.
    """
    from datetime import timedelta

    from curl_cffi import requests

    session: requests.Session = requests.Session(impersonate="chrome124")
    session.get(NSE_HOME_URL, timeout=20)

    current = _get_json(session, CURRENT_ISSUES_URL, _HEADERS)
    upcoming = _get_json(session, UPCOMING_ISSUES_URL, _HEADERS)
    past = _get_json(session, PAST_ISSUES_URL, _HEADERS)

    issue_dicts: list[dict[object, object]] = []
    if isinstance(current, list):
        for raw in current:
            issue_dicts.append(_list_issue_to_dict(raw, "open"))
    if isinstance(upcoming, list):
        for raw in upcoming:
            issue_dicts.append(_list_issue_to_dict(raw, "upcoming"))
    if isinstance(past, list):
        for raw in past:
            issue_dicts.append(_list_issue_to_dict(raw, "listed"))

    as_of_date = as_of or date.today()
    recent_cutoff = as_of_date - timedelta(days=30)
    subscription_dicts: list[dict[object, object]] = []
    for issue in issue_dicts:
        symbol = issue.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            continue
        status = str(issue.get("status") or "")
        if status not in ("open", "upcoming"):
            listing = issue.get("listing_date")
            if listing is None:
                continue
            try:
                listing_date = _parse_date(listing)
            except ValueError:
                continue
            if listing_date < recent_cutoff:
                continue
        board = str(issue.get("board") or "EQ")
        detail = _fetch_detail(session, symbol.strip(), board, _HEADERS)
        if detail is None:
            continue
        issue.update(_detail_issue_fields(detail, issue))
        subscription = _detail_subscription(detail, symbol.strip(), as_of_date)
        if subscription is not None:
            subscription_dicts.append(subscription)

    payload: dict[str, object] = {
        "as_of_date": as_of_date.isoformat(),
        "issues": issue_dicts,
        "subscriptions": subscription_dicts,
    }
    return parse_ipo_issues(payload)


class _IpoStore(Protocol):
    """Minimal structural contract for an IPO issue store."""

    def upsert(self, issue: IpoIssue) -> None: ...


class _SubscriptionStore(Protocol):
    """Minimal structural contract for an IPO subscription store."""

    def upsert(self, subscription: IpoSubscription) -> None: ...


def ingest_ipos(
    ipo_store: _IpoStore,
    subscription_store: _SubscriptionStore,
    as_of: date | None = None,
) -> tuple[int, int]:
    """Fetch live NSE IPO data and upsert issues and subscriptions.

    Returns ``(issue_count, subscription_count)``.
    """
    parsed = fetch_ipo_issues(as_of)
    issues = rows_to_issues(parsed.issue_rows)
    subscriptions = rows_to_subscriptions(parsed.subscription_rows)
    for issue in issues:
        ipo_store.upsert(issue)
    for subscription in subscriptions:
        subscription_store.upsert(subscription)
    return len(issues), len(subscriptions)
