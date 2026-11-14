"""NSE FII/DII daily institutional net-flow ingestion (gap 0013).

Deterministic parser plus optional live fetcher for the NSE ``fiidiiTradeReact``
JSON feed. Zero-floats invariant: every value is parsed with exact
``decimal.Decimal`` arithmetic, never ``float``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from tantra_data.domain.institutional_flow import InstitutionalFlow

__all__ = [
    "InstitutionalFlowRow",
    "fetch_institutional_flows",
    "parse_institutional_flows",
    "read_institutional_flows",
    "rows_to_flows",
]

NSE_HOME_URL = "https://www.nseindia.com"
FII_DII_URL = f"{NSE_HOME_URL}/api/fiidiiTradeReact"
REFERER_URL = "https://www.nseindia.com/option-chain"
DATE_FORMAT = "%d-%b-%Y"
DEFAULT_SEGMENT = "cash"


@dataclass(frozen=True)
class InstitutionalFlowRow:
    """A single parsed FII/DII net-flow observation (one participant, one day)."""

    trade_date: date
    participant: str
    segment: str
    gross_buy: Decimal
    gross_sell: Decimal
    net: Decimal


def parse_institutional_flows(text: str) -> list[InstitutionalFlowRow]:
    """Parse the NSE ``fiidiiTradeReact`` JSON payload.

    The verified live payload is a JSON array of objects shaped like::

        [{"buyValue": "11745.95", "category": "DII", "date": "13-Nov-2026",
          "netValue": "-2253.58", "sellValue": "13999.53"}, ...]

    ``category`` ``"FII/FPI"`` is normalized to participant ``"FII"``.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("FII/DII data is not valid JSON") from exc
    if not isinstance(payload, list):
        raise ValueError("FII/DII data is not valid JSON")
    return [_parse_flow_element(element) for element in payload]


def _parse_flow_element(element: object) -> InstitutionalFlowRow:
    if not isinstance(element, dict):
        raise ValueError("FII/DII data is not valid JSON")
    category = element.get("category")
    if isinstance(category, str) and category.startswith("FII"):
        participant = "FII"
    elif category == "DII":
        participant = "DII"
    else:
        raise ValueError(f"Unknown FII/DII category: {category!r}")
    date_text = element.get("date")
    if not isinstance(date_text, str):
        raise ValueError(f"FII/DII row missing a date: {element!r}")
    trade_date = datetime.strptime(date_text.strip(), DATE_FORMAT).date()
    segment = element.get("segment", DEFAULT_SEGMENT)
    if not isinstance(segment, str):
        raise ValueError(f"FII/DII row has an invalid segment: {element!r}")
    try:
        gross_buy = Decimal(str(element["buyValue"]))
        gross_sell = Decimal(str(element["sellValue"]))
        net = Decimal(str(element["netValue"]))
    except (KeyError, InvalidOperation) as exc:
        raise ValueError(f"FII/DII row has invalid value fields: {element!r}") from exc
    return InstitutionalFlowRow(
        trade_date=trade_date,
        participant=participant,
        segment=segment,
        gross_buy=gross_buy,
        gross_sell=gross_sell,
        net=net,
    )


def read_institutional_flows(path: str | os.PathLike[str]) -> list[InstitutionalFlowRow]:
    """Read and parse FII/DII flow JSON from a local file at *path*."""
    with open(path, encoding="utf-8") as handle:
        return parse_institutional_flows(handle.read())


def rows_to_flows(
    rows: list[InstitutionalFlowRow],
    source: str = "nse",
) -> list[InstitutionalFlow]:
    """Map parsed rows onto the frozen :class:`InstitutionalFlow` domain type."""
    return [
        InstitutionalFlow(
            trade_date=row.trade_date,
            participant=row.participant,
            segment=row.segment,
            gross_buy=row.gross_buy,
            gross_sell=row.gross_sell,
            net=row.net,
            source=source,
        )
        for row in rows
    ]


def fetch_institutional_flows() -> list[InstitutionalFlow]:
    """Fetch the latest FII/DII cash-market net flows from NSE.

    ``curl_cffi`` with Chrome impersonation is required because the bare
    ``www.nseindia.com`` endpoint blocks plain ``requests``/``curl``; it is
    imported lazily so this module imports without it.
    """
    from curl_cffi import requests

    session: requests.Session = requests.Session(impersonate="chrome124")
    session.get(NSE_HOME_URL, timeout=20)
    response = session.get(
        FII_DII_URL,
        headers={"Accept": "application/json", "Referer": REFERER_URL},
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f"FII/DII fetch failed with HTTP {response.status_code}")
    return rows_to_flows(parse_institutional_flows(response.text))
