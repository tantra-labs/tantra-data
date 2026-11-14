"""NSE block/bulk deal disclosure value objects.

A block deal is a single large trade above an exchange minimum executed in the
separate block-deal window; a bulk deal is any single trade on the regular
order book whose quantity exceeds 0.5% of the company's listed shares. NSE and
BSE publish their own daily disclosures for both categories.

Zero-floats invariant: every price and quantity is an exact
``decimal.Decimal``. Value objects are frozen, self-validating dataclasses that
compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from tantra_data._compat import D, to_decimal

_VALID_CATEGORIES = ("block", "bulk")
_VALID_SIDES = ("BUY", "SELL")


def _non_empty(value: str, field: str) -> str:
    if not value:
        raise ValueError(f"{field} must be non-empty")
    return value


@dataclass(frozen=True, slots=True)
class BlockDeal:
    """One disclosed block or bulk deal on a trade date."""

    trade_date: date
    """Deal date (the exchange disclosure's trade date)."""

    exchange: str
    """Exchange that published the disclosure (e.g. ``"NSE"``, ``"BSE"``)."""

    instrument: str
    """Security identifier (e.g. ``"NSE:RELIANCE-EQ"`` or the raw symbol)."""

    security_name: str
    """Human-readable security name from the disclosure."""

    client_name: str
    """Client/counterparty name from the disclosure."""

    side: str
    """Deal side: ``"BUY"`` or ``"SELL"``."""

    category: str
    """Deal category: ``"block"`` or ``"bulk"``."""

    quantity: Decimal
    """Traded quantity (exact ``Decimal``)."""

    price: Decimal
    """Trade / weighted-average price (exact ``Decimal``)."""

    remarks: str | None = None
    """Optional remarks column (present on NSE's bulk CSV)."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "trade_date", self.trade_date)
        object.__setattr__(self, "exchange", _non_empty(self.exchange, "exchange"))
        object.__setattr__(self, "instrument", _non_empty(self.instrument, "instrument"))
        object.__setattr__(
            self, "security_name", _non_empty(self.security_name, "security_name")
        )
        object.__setattr__(self, "client_name", _non_empty(self.client_name, "client_name"))
        side = self.side.upper()
        if side not in _VALID_SIDES:
            raise ValueError(f"invalid side {self.side!r}; must be one of {_VALID_SIDES}")
        object.__setattr__(self, "side", side)
        category = self.category.lower()
        if category not in _VALID_CATEGORIES:
            raise ValueError(
                f"invalid category {self.category!r}; must be one of {_VALID_CATEGORIES}"
            )
        object.__setattr__(self, "category", category)
        quantity = to_decimal(self.quantity)
        if quantity <= 0:
            raise ValueError(f"quantity must be positive, got {quantity}")
        object.__setattr__(self, "quantity", quantity)
        price = to_decimal(self.price)
        if price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        object.__setattr__(self, "price", price)
        remarks = self.remarks
        if remarks is not None:
            remarks = remarks.strip() or None
            object.__setattr__(self, "remarks", remarks)

    @property
    def value(self) -> Decimal:
        """Notional value of the deal: ``quantity * price`` (exact Decimal)."""
        return self.quantity * self.price


__all__ = ["BlockDeal", "D"]
