"""NSE corporate-actions ingestion (tantra-data gap 0009).

Parses the JSON payload served by the NSE corporate-actions API and maps each
record onto the canonical ``tantra_data`` domain value objects (``Bonus``,
``Split``, ``Dividend``, ``Rights``) for the DuckDB corporate-actions store.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from tantra_data.domain.corporate_actions import (
    Bonus,
    CorporateAction,
    Dividend,
    Rights,
    Split,
)

__all__ = [
    "CorporateActionRow",
    "fetch_corporate_actions",
    "parse_corporate_actions",
    "rows_to_actions",
    "subject_to_action",
]

_NSE_BASE_URL = "https://www.nseindia.com"
_NSE_CORPORATE_ACTIONS_URL = "https://www.nseindia.com/api/corporates-corporateActions"
_NSE_REFERER_URL = "https://www.nseindia.com/option-chain"
_NSE_DATE_FORMAT = "%d-%b-%Y"

_BONUS_RE = re.compile(r"^Bonus\s+(\d+):(\d+)", re.IGNORECASE)
_SPLIT_RE = re.compile(
    r"Face Value Split.*From Rs ([0-9.]+)/?.*To Re?s? ([0-9.]+)/?.*Per Share",
    re.IGNORECASE,
)
_DIVIDEND_PREFIX_RE = re.compile(
    r"^(?:Interim|Final|Special|1st|2nd|3rd|4th|Dividend)\b", re.IGNORECASE
)
_PER_SHARE_AMOUNT_RE = re.compile(r"R(?:s|e)?\s+([0-9.]+)(?:/-)?\s+Per Share", re.IGNORECASE)
_RIGHTS_RE = re.compile(
    r"^Rights\s+(\d+):(\d+)\s*(?:@\s*Premium\s+R(?:s|e)?\s*([0-9.]+))?",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class CorporateActionRow:
    symbol: str
    company: str
    ex_date: date
    record_date: date | None
    face_value: Decimal | None
    subject: str
    isin: str | None


def parse_corporate_actions(text: str) -> list[CorporateActionRow]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("corporate actions data is not valid JSON") from exc

    rows: list[CorporateActionRow] = []
    for item in payload:
        rows.append(
            CorporateActionRow(
                symbol=item["symbol"],
                company=item["comp"],
                ex_date=datetime.strptime(item["exDate"], _NSE_DATE_FORMAT).date(),
                record_date=_parse_date_or_none(item.get("recDate")),
                face_value=_parse_face_value_or_none(item.get("faceVal")),
                subject=item["subject"],
                isin=item.get("isin"),
            )
        )
    return rows


def subject_to_action(row: CorporateActionRow, exchange: str = "NSE") -> CorporateAction | None:
    subject = row.subject.strip()
    instrument = f"{exchange}:{row.symbol}-EQ"

    bonus_match = _BONUS_RE.match(subject)
    if bonus_match is not None:
        held = int(bonus_match.group(1))
        bonus_shares = int(bonus_match.group(2))
        return Bonus(instrument, row.ex_date, held, held + bonus_shares)

    split_match = _SPLIT_RE.match(subject)
    if split_match is not None:
        return _split_from_faces(
            split_match.group(1), split_match.group(2), instrument, row.ex_date
        )

    if _DIVIDEND_PREFIX_RE.match(subject) is not None:
        amount_match = _PER_SHARE_AMOUNT_RE.search(subject)
        if amount_match is None:
            return None
        return Dividend(instrument, row.ex_date, Decimal(amount_match.group(1)), "INR")

    rights_match = _RIGHTS_RE.match(subject)
    if rights_match is not None:
        old_shares = int(rights_match.group(1))
        new_shares = int(rights_match.group(2))
        premium = rights_match.group(3)
        if premium is None:
            return None
        face_value = row.face_value if row.face_value is not None else Decimal(0)
        return Rights(
            instrument,
            row.ex_date,
            old_shares,
            new_shares,
            face_value + Decimal(premium),
        )

    return None


def rows_to_actions(rows: list[CorporateActionRow], exchange: str = "NSE") -> list[CorporateAction]:
    actions: list[CorporateAction] = []
    for row in rows:
        action = subject_to_action(row, exchange)
        if action is not None:
            actions.append(action)
    return actions


def fetch_corporate_actions(from_date: str, to_date: str) -> list[CorporateAction]:
    from curl_cffi import requests as curl_requests

    session: Any = curl_requests.Session(impersonate="chrome124")
    session.get(_NSE_BASE_URL, timeout=20)
    response: Any = session.get(
        _NSE_CORPORATE_ACTIONS_URL,
        params={"index": "equities", "from_date": from_date, "to_date": to_date},
        headers={
            "Accept": "application/json",
            "Referer": _NSE_REFERER_URL,
        },
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"NSE corporate actions request failed with status {response.status_code}"
        )
    return rows_to_actions(parse_corporate_actions(response.text))


def _parse_date_or_none(value: object) -> date | None:
    if not isinstance(value, str) or value in ("", "-"):
        return None
    return datetime.strptime(value, _NSE_DATE_FORMAT).date()


def _parse_face_value_or_none(value: object) -> Decimal | None:
    if not isinstance(value, str) or value in ("", "-"):
        return None
    return Decimal(value)


def _split_from_faces(old_face: str, new_face: str, instrument: str, ex_date: date) -> Split:
    old_decimal = Decimal(old_face)
    new_decimal = Decimal(new_face)
    places = max(_decimal_places(old_decimal), _decimal_places(new_decimal))
    scale = 10**places
    old_scaled = int(old_decimal * scale)
    new_scaled = int(new_decimal * scale)
    divisor = math.gcd(old_scaled, new_scaled)
    # An old_face-valued share splits into old_face/new_face new shares.
    return Split(instrument, ex_date, new_scaled // divisor, old_scaled // divisor)


def _decimal_places(value: Decimal) -> int:
    text = format(value, "f")
    if "." in text:
        return len(text.split(".", 1)[1])
    return 0
