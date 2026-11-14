"""Daily FII/DII institutional net-flow value objects.

FII (Foreign Institutional Investor) and DII (Domestic Institutional Investor)
daily cash-market net buy/sell figures (and, separately, F&O net positioning)
are published by NSE/SEBI. Each record carries gross buy value, gross sell
value, and net value per participant per segment.

Zero-floats invariant: every value is an exact ``decimal.Decimal``. Value
objects are frozen, self-validating dataclasses that compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from tantra_data._compat import to_decimal

_VALID_PARTICIPANTS = ("FII", "DII")
_VALID_SEGMENTS = (
    "cash",
    "index_futures",
    "stock_futures",
    "index_options",
    "stock_options",
)


@dataclass(frozen=True, slots=True)
class InstitutionalFlow:
    """One participant's daily net-flow record for one segment."""

    trade_date: date
    """Trade date of the flow record."""

    participant: str
    """Participant category: ``"FII"`` or ``"DII"``."""

    segment: str
    """Segment: ``cash``, ``index_futures``, ``stock_futures``,
    ``index_options``, ``stock_options``. Cash is the primary signal."""

    gross_buy: Decimal
    """Gross buy value (exact ``Decimal``)."""

    gross_sell: Decimal
    """Gross sell value (exact ``Decimal``)."""

    net: Decimal
    """Net value (exact ``Decimal``)."""

    source: str = "nse"
    """Publisher of the record (e.g. ``"nse"``)."""

    def __post_init__(self) -> None:
        if self.trade_date is None:
            raise ValueError("trade_date must be set")
        participant = self.participant.upper()
        if participant not in _VALID_PARTICIPANTS:
            raise ValueError(
                f"invalid participant {self.participant!r}; must be one of {_VALID_PARTICIPANTS}"
            )
        object.__setattr__(self, "participant", participant)
        segment = self.segment.lower()
        if segment not in _VALID_SEGMENTS:
            raise ValueError(
                f"invalid segment {self.segment!r}; must be one of {_VALID_SEGMENTS}"
            )
        object.__setattr__(self, "segment", segment)
        object.__setattr__(self, "gross_buy", to_decimal(self.gross_buy))
        object.__setattr__(self, "gross_sell", to_decimal(self.gross_sell))
        object.__setattr__(self, "net", to_decimal(self.net))
        if not self.source:
            raise ValueError("source must be non-empty")


__all__ = ["InstitutionalFlow"]
