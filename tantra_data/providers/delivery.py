"""NSE per-symbol daily delivery percentage parser and fetcher.

Tolerant of both the live NSE bhavcopy header (``DELIV_QTY`` / ``DELIV_PER`` /
``TTL_TRD_QNTY`` / ``DATE1``) and the legacy header (``DELIVERABLE_QTY`` /
``% DLVY TO QTY`` / ``TOTAL_TRADED_QUANTITY`` / ``DATE``), plus ``-``
placeholders for missing delivery values. Delivery percentages are computed
with exact :class:`decimal.Decimal` arithmetic, never floats.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from tantra_data.domain.delivery import DeliveryRecord

__all__ = [
    "DeliveryRow",
    "fetch_delivery",
    "parse_delivery",
    "read_delivery",
    "rows_to_records",
]

_BHAVCOPY_URL = (
    "https://archives.nseindia.com/products/content/"
    "sec_bhavdata_full_{date}.csv"
)
_DATE_FORMAT = "%d-%b-%Y"

_COLUMN_ALIASES = {
    "date": ("DATE1", "DATE"),
    "traded": ("TTL_TRD_QNTY", "TOTAL_TRADED_QUANTITY"),
    "deliverable": ("DELIV_QTY", "DELIVERABLE_QTY"),
    "pct": ("DELIV_PER", "% DLVY TO QTY"),
}


@dataclass(frozen=True)
class DeliveryRow:
    """One per-symbol delivery row parsed from an NSE bhavcopy CSV."""

    trade_date: date
    instrument: str
    traded_quantity: int
    deliverable_quantity: int | None
    delivery_pct: Decimal | None
    exchange: str = "NSE"


def _clean(value: object) -> str:
    """Return a stripped, BOM-free string for the given cell value."""
    if value is None:
        return ""
    return str(value).strip().lstrip("\ufeff")


def _parse_int(value: object) -> int | None:
    """Parse an integer cell, returning None for ``-`` or empty values."""
    text = _clean(value)
    if not text or text == "-":
        return None
    return int(text)


def _parse_decimal(value: object) -> Decimal | None:
    """Parse a decimal cell exactly, returning None for ``-`` or empty values."""
    text = _clean(value)
    if not text or text == "-":
        return None
    return Decimal(text)


def _pick_columns(fieldnames: list[str]) -> dict[str, str]:
    """Map canonical roles (date/traded/deliverable/pct) to header names."""
    columns: dict[str, str] = {}
    for role, aliases in _COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in fieldnames:
                columns[role] = alias
                break
    return columns


def parse_delivery(text: str) -> list[DeliveryRow]:
    """Parse NSE bhavcopy delivery CSV text into :class:`DeliveryRow` values."""
    lines = text.splitlines()
    if not lines:
        raise ValueError("delivery data is empty or missing a header")
    header = next(csv.reader([lines[0]]))
    if not header:
        raise ValueError("delivery data is empty or missing a header")
    fieldnames = [_clean(name) for name in header]
    columns = _pick_columns(fieldnames)
    missing = [role for role in ("date", "traded") if role not in columns]
    if missing:
        raise ValueError(
            f"delivery data is missing required columns: {', '.join(missing)}"
        )

    rows: list[DeliveryRow] = []
    for record in csv.DictReader(lines[1:], fieldnames=fieldnames):
        symbol = _clean(record.get("SYMBOL"))
        if not symbol:
            continue
        trade_date = datetime.strptime(
            _clean(record.get(columns["date"])), _DATE_FORMAT
        ).date()
        traded_quantity = int(_clean(record.get(columns["traded"])))
        deliverable = (
            _parse_int(record.get(columns["deliverable"]))
            if "deliverable" in columns
            else None
        )
        pct = (
            _parse_decimal(record.get(columns["pct"]))
            if "pct" in columns
            else None
        )
        rows.append(
            DeliveryRow(
                trade_date=trade_date,
                instrument=f"NSE:{symbol}-EQ",
                exchange="NSE",
                traded_quantity=traded_quantity,
                deliverable_quantity=deliverable,
                delivery_pct=pct,
            )
        )
    return rows


def read_delivery(path: str) -> list[DeliveryRow]:
    """Read and parse delivery rows from a bhavcopy CSV file."""
    with open(path, newline="", encoding="utf-8-sig") as handle:
        return parse_delivery(handle.read())


def rows_to_records(
    rows: list[DeliveryRow], exchange: str = "NSE"
) -> list[DeliveryRecord]:
    """Convert parsed delivery rows into canonical :class:`DeliveryRecord` values."""
    records: list[DeliveryRecord] = []
    for row in rows:
        deliverable_quantity = row.deliverable_quantity or 0
        if row.delivery_pct is not None:
            delivery_pct = row.delivery_pct
        elif row.traded_quantity > 0:
            delivery_pct = (
                Decimal(deliverable_quantity) / Decimal(row.traded_quantity) * 100
            )
        else:
            delivery_pct = Decimal(0)
        records.append(
            DeliveryRecord(
                trade_date=row.trade_date,
                instrument=row.instrument,
                exchange=exchange,
                traded_quantity=row.traded_quantity,
                deliverable_quantity=deliverable_quantity,
                delivery_pct=delivery_pct,
            )
        )
    return records


def fetch_delivery(date_text: str) -> list[DeliveryRecord]:
    """Fetch NSE delivery records for the given ``YYYY-MM-DD`` trade date."""
    import requests

    trade_date = datetime.strptime(date_text, "%Y-%m-%d").date()
    url = _BHAVCOPY_URL.format(date=trade_date.strftime("%d%m%Y"))
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"failed to fetch delivery data from {url}: HTTP {response.status_code}"
        )
    return rows_to_records(parse_delivery(response.text))
