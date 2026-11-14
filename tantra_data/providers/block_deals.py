"""Deterministic NSE block/bulk deal disclosure parser.

Parses the official NSE daily disclosure CSVs (``block.csv`` / ``bulk.csv``)
into typed rows with exact ``decimal.Decimal`` prices and maps them onto
canonical :class:`~tantra_data.domain.block_deals.BlockDeal` values.

Columns (official NSE header)::

    Date,Symbol,Security Name,Client Name,Buy/Sell,Quantity Traded,
    Trade Price / Wght. Avg. Price
    (bulk.csv additionally ends with ,Remarks)

Zero-floats invariant: every price and quantity is an exact ``Decimal`` —
never floating point. Fetching is optional; ``requests`` is imported lazily so
the module stays import-safe without network dependencies.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from tantra_data.domain.block_deals import BlockDeal

_BLOCK_DEALS_URL = "https://archives.nseindia.com/content/equities/block.csv"
_BULK_DEALS_URL = "https://archives.nseindia.com/content/equities/bulk.csv"

_PRICE_COLUMN = "Trade Price / Wght. Avg. Price"


@dataclass(frozen=True, slots=True)
class BlockDealRow:
    """One NSE block/bulk disclosure line with an exact ``Decimal`` price."""

    trade_date: date
    symbol: str
    security_name: str
    client_name: str
    side: str
    quantity: int
    price: Decimal
    remarks: str | None = None


def _decimal(text: str, field: str) -> Decimal:
    try:
        return Decimal(text.strip())
    except Exception as exc:
        raise ValueError(f"invalid {field!r} value in block/bulk deals: {text!r}") from exc


def _quantity(text: str, field: str) -> int:
    try:
        return int(text.strip())
    except Exception as exc:
        raise ValueError(f"invalid {field!r} value in block/bulk deals: {text!r}") from exc


def _date(text: str, field: str) -> date:
    try:
        return datetime.strptime(text.strip(), "%d-%b-%Y").date()
    except Exception as exc:
        raise ValueError(f"invalid {field!r} date in block/bulk deals: {text!r}") from exc


def parse_block_deals(text: str) -> list[BlockDealRow]:
    """Parse raw NSE block-deal CSV text into typed rows (header is mandatory)."""
    reader = csv.DictReader(line for line in text.splitlines() if line.strip())
    if reader.fieldnames is None or not reader.fieldnames:
        raise ValueError("block deals data is empty or missing a header")
    rows: list[BlockDealRow] = []
    for raw in reader:
        if not any(raw.values()):
            continue
        rows.append(
            BlockDealRow(
                trade_date=_date(raw["Date"], "Date"),
                symbol=raw["Symbol"].strip(),
                security_name=raw["Security Name"].strip(),
                client_name=raw["Client Name"].strip(),
                side=raw["Buy/Sell"].strip(),
                quantity=_quantity(raw["Quantity Traded"], "Quantity Traded"),
                price=_decimal(raw[_PRICE_COLUMN], _PRICE_COLUMN),
            )
        )
    return rows


def parse_bulk_deals(text: str) -> list[BlockDealRow]:
    """Parse raw NSE bulk-deal CSV text into typed rows (header is mandatory).

    The optional ``Remarks`` column is stripped; empty remarks become ``None``.
    """
    reader = csv.DictReader(line for line in text.splitlines() if line.strip())
    if reader.fieldnames is None or not reader.fieldnames:
        raise ValueError("bulk deals data is empty or missing a header")
    rows: list[BlockDealRow] = []
    for raw in reader:
        if not any(raw.values()):
            continue
        remarks = raw.get("Remarks", "").strip() or None
        rows.append(
            BlockDealRow(
                trade_date=_date(raw["Date"], "Date"),
                symbol=raw["Symbol"].strip(),
                security_name=raw["Security Name"].strip(),
                client_name=raw["Client Name"].strip(),
                side=raw["Buy/Sell"].strip(),
                quantity=_quantity(raw["Quantity Traded"], "Quantity Traded"),
                price=_decimal(raw[_PRICE_COLUMN], _PRICE_COLUMN),
                remarks=remarks,
            )
        )
    return rows


def read_block_deals(path: str | Path) -> list[BlockDealRow]:
    """Read and parse an NSE block-deal CSV file from disk."""
    with open(path, encoding="utf-8") as handle:
        return parse_block_deals(handle.read())


def read_bulk_deals(path: str | Path) -> list[BlockDealRow]:
    """Read and parse an NSE bulk-deal CSV file from disk."""
    with open(path, encoding="utf-8") as handle:
        return parse_bulk_deals(handle.read())


def rows_to_deals(
    rows: list[BlockDealRow],
    exchange: str = "NSE",
    category: str = "block",
) -> list[BlockDeal]:
    """Map parsed disclosure rows onto canonical block/bulk deal values.

    Instruments use the canonical ``{exchange}:{symbol}-EQ`` shape; remarks are
    carried over for ``bulk`` disclosures only.
    """
    deals: list[BlockDeal] = []
    for row in rows:
        deals.append(
            BlockDeal(
                trade_date=row.trade_date,
                exchange=exchange,
                instrument=f"{exchange}:{row.symbol}-EQ",
                security_name=row.security_name,
                client_name=row.client_name,
                side=row.side,
                category=category,
                quantity=Decimal(str(row.quantity)),
                price=row.price,
                remarks=row.remarks if category == "bulk" else None,
            )
        )
    return deals


def _download(url: str) -> str:
    import requests

    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.text


def fetch_block_deals() -> list[BlockDeal]:
    """Fetch and parse the latest NSE block-deal disclosure CSV."""
    return rows_to_deals(parse_block_deals(_download(_BLOCK_DEALS_URL)), category="block")


def fetch_bulk_deals() -> list[BlockDeal]:
    """Fetch and parse the latest NSE bulk-deal disclosure CSV."""
    return rows_to_deals(parse_bulk_deals(_download(_BULK_DEALS_URL)), category="bulk")


__all__ = [
    "BlockDealRow",
    "fetch_block_deals",
    "fetch_bulk_deals",
    "parse_block_deals",
    "parse_bulk_deals",
    "read_block_deals",
    "read_bulk_deals",
    "rows_to_deals",
]
