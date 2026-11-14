"""Per-symbol daily delivery records.

The NSE Bhavcopy "Deliverable Quantity / Traded Quantity" report publishes per-
symbol traded quantity and deliverable quantity; delivery percentage is
``deliverable_qty / traded_qty * 100`` computed as exact ``Decimal`` (never a
float ratio). Delivery percentage is the standard mechanism for distinguishing
real accumulation/distribution from intraday churn.

Zero-floats invariant: every value is an exact ``decimal.Decimal``. Value
objects are frozen, self-validating dataclasses that compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from tantra_data._compat import D, to_decimal


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    """One symbol's daily delivery position."""

    trade_date: date
    """Trade date of the delivery position."""

    instrument: str
    """Security identifier (e.g. ``"NSE:RELIANCE-EQ"`` or the raw symbol)."""

    traded_quantity: int
    """Total traded quantity for the day."""

    deliverable_quantity: int
    """Deliverable quantity for the day."""

    delivery_pct: Decimal
    """Delivery percentage = ``deliverable_qty / traded_qty * 100`` (exact
    ``Decimal``)."""

    exchange: str = "NSE"
    """Exchange that published the record."""

    def __post_init__(self) -> None:
        if self.trade_date is None:
            raise ValueError("trade_date must be set")
        if not self.instrument:
            raise ValueError("instrument must be non-empty")
        if self.traded_quantity < 0:
            raise ValueError("traded_quantity must be non-negative")
        if self.deliverable_quantity < 0:
            raise ValueError("deliverable_quantity must be non-negative")
        if not self.exchange:
            raise ValueError("exchange must be non-empty")
        object.__setattr__(self, "delivery_pct", to_decimal(self.delivery_pct))

    @property
    def delivery_ratio(self) -> Decimal:
        """Deliverable-to-traded ratio as an exact ``Decimal`` in ``[0,1]``."""
        if self.traded_quantity == 0:
            return D(0)
        return D(self.deliverable_quantity) / D(self.traded_quantity)


__all__ = ["DeliveryRecord"]
