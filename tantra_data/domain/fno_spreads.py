"""F&O spot-futures basis and option bid-ask spread value objects (exact ``Decimal``).

Tracks point-in-time futures calendar/spot basis and top-of-book option/futures
bid-ask spreads for Multi-Asset Experimental Analysis (F&O Spread & Rollover Basis
modeling). Zero floats anywhere: every financial quantity is ``decimal.Decimal``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

from tantra_data._compat import to_decimal

_OPTION_TYPES = ("CE", "PE")
_FOUR_DP = Decimal("0.0001")
_TEN_K = Decimal("10000")
_TWO = Decimal("2")

__all__ = [
    "FuturesBasisSnapshot",
    "OptionSpreadSnapshot",
]


def _require_decimal(value: Decimal | int | str, field_name: str) -> Decimal:
    if isinstance(value, float):
        raise TypeError(f"{field_name} must be Decimal/int/str, never float")
    dec = to_decimal(value)
    if not dec.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return dec


@dataclass(frozen=True, slots=True)
class FuturesBasisSnapshot:
    """Point-in-time spot vs near-month & far-month futures basis snapshot."""

    as_of: datetime
    underlying: str
    spot_price: Decimal
    near_expiry: date
    near_futures_price: Decimal
    far_expiry: date | None = None
    far_futures_price: Decimal | None = None
    source: str = "nse"

    def __post_init__(self) -> None:
        if self.as_of is None or self.as_of.tzinfo is None:
            raise ValueError("as_of must be a timezone-aware datetime (UTC)")
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))

        underlying = self.underlying.strip().upper()
        if not underlying:
            raise ValueError("underlying must be non-empty")
        object.__setattr__(self, "underlying", underlying)

        spot = _require_decimal(self.spot_price, "spot_price")
        near_px = _require_decimal(self.near_futures_price, "near_futures_price")
        if spot <= 0:
            raise ValueError("spot_price must be positive")
        if near_px <= 0:
            raise ValueError("near_futures_price must be positive")
        object.__setattr__(self, "spot_price", spot)
        object.__setattr__(self, "near_futures_price", near_px)

        if self.near_expiry is None:
            raise ValueError("near_expiry must be set")

        if self.far_futures_price is not None:
            far_px = _require_decimal(self.far_futures_price, "far_futures_price")
            if far_px <= 0:
                raise ValueError("far_futures_price must be positive when set")
            object.__setattr__(self, "far_futures_price", far_px)

        if not self.source or not self.source.strip():
            raise ValueError("source must be non-empty")

    @property
    def spot_basis_abs(self) -> Decimal:
        """``near_futures_price - spot_price`` rounded to 4 decimal places."""
        return (self.near_futures_price - self.spot_price).quantize(_FOUR_DP)

    @property
    def spot_basis_bps(self) -> Decimal:
        """Basis in basis points ``(near - spot) / spot * 10000``."""
        return ((self.near_futures_price - self.spot_price) / self.spot_price * _TEN_K).quantize(
            _FOUR_DP
        )

    @property
    def calendar_spread_abs(self) -> Decimal | None:
        """``far_futures_price - near_futures_price`` if far contract is present."""
        if self.far_futures_price is None:
            return None
        return (self.far_futures_price - self.near_futures_price).quantize(_FOUR_DP)

    @property
    def calendar_spread_bps(self) -> Decimal | None:
        """Rollover spread in bps ``(far - near) / near * 10000``."""
        if self.far_futures_price is None:
            return None
        return (
            (self.far_futures_price - self.near_futures_price) / self.near_futures_price * _TEN_K
        ).quantize(_FOUR_DP)


@dataclass(frozen=True, slots=True)
class OptionSpreadSnapshot:
    """Point-in-time option strike top-of-book bid-ask spread observation."""

    as_of: datetime
    underlying: str
    expiry: date
    strike: Decimal
    option_type: str
    bid: Decimal
    ask: Decimal
    implied_volatility: Decimal = Decimal("0")
    open_interest: Decimal = Decimal("0")
    source: str = "nse"

    def __post_init__(self) -> None:
        if self.as_of is None or self.as_of.tzinfo is None:
            raise ValueError("as_of must be a timezone-aware datetime (UTC)")
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))

        underlying = self.underlying.strip().upper()
        if not underlying:
            raise ValueError("underlying must be non-empty")
        object.__setattr__(self, "underlying", underlying)

        if self.expiry is None:
            raise ValueError("expiry must be set")

        opt_type = self.option_type.strip().upper()
        if opt_type not in _OPTION_TYPES:
            raise ValueError(f"invalid option_type {self.option_type!r}; expected {_OPTION_TYPES}")
        object.__setattr__(self, "option_type", opt_type)

        strike = _require_decimal(self.strike, "strike")
        bid = _require_decimal(self.bid, "bid")
        ask = _require_decimal(self.ask, "ask")
        iv = _require_decimal(self.implied_volatility, "implied_volatility")
        oi = _require_decimal(self.open_interest, "open_interest")

        if strike <= 0:
            raise ValueError("strike must be positive")
        if bid < 0:
            raise ValueError("bid must be non-negative")
        if ask < bid:
            raise ValueError("ask must be >= bid")
        if oi < 0:
            raise ValueError("open_interest must be non-negative")

        object.__setattr__(self, "strike", strike)
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(self, "implied_volatility", iv)
        object.__setattr__(self, "open_interest", oi)

        if not self.source or not self.source.strip():
            raise ValueError("source must be non-empty")

    @property
    def mid(self) -> Decimal:
        """Exact mid price ``(bid + ask) / 2``."""
        return ((self.bid + self.ask) / _TWO).quantize(_FOUR_DP)

    @property
    def spread_abs(self) -> Decimal:
        """Absolute bid-ask spread ``ask - bid``."""
        return (self.ask - self.bid).quantize(_FOUR_DP)

    @property
    def spread_bps(self) -> Decimal:
        """Relative bid-ask spread in bps ``(ask - bid) / mid * 10000``."""
        mid_px = (self.bid + self.ask) / _TWO
        if mid_px <= 0:
            return Decimal("0.0000")
        return (((self.ask - self.bid) / mid_px) * _TEN_K).quantize(_FOUR_DP)
