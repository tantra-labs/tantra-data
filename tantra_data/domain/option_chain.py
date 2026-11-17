"""Intraday option-chain snapshot value objects and gamma-exposure math.

Closes docs/gaps/0016-*.md: the lakehouse needs point-in-time, strike-wise
option positioning (open interest, OI change, IV, delta, gamma) per expiry at
intraday timestamps, plus a derived gamma-exposure (GEX) / gamma-flip series
for the Elephant Edge (EE+) backtest regime gate.

The GEX model is a Black-76-style approximation computed with exact
``Decimal`` arithmetic:

    per-strike GEX = gamma * open_interest * lot_size * spot

``net`` is the sum over every strike; ``flip`` is the strike where the
cumulative GEX (walking strikes from the lowest upward) changes sign — the
dealer long/short-gamma inflection level — or ``None`` when it never crosses
zero.

Zero-floats invariant: every value is an exact ``decimal.Decimal`` (never
``float``). Value objects are frozen, self-validating dataclasses that compare
by value.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal

from tantra_data._compat import to_decimal

_OPTION_TYPES = ("CE", "PE")

__all__ = [
    "GammaExposure",
    "OptionChainSnapshot",
    "StrikeGamma",
    "StrikePositioning",
    "compute_gamma_exposure",
]


@dataclass(frozen=True, slots=True)
class StrikePositioning:
    """One strike's call or put positioning within a chain snapshot."""

    strike: Decimal
    """Strike price of the option (exact ``Decimal``)."""

    option_type: str
    """``"CE"`` (call) or ``"PE"`` (put)."""

    open_interest: Decimal
    """Open interest in contracts (exact ``Decimal``)."""

    oi_change: Decimal
    """Change in open interest versus the prior observation (exact ``Decimal``)."""

    implied_volatility: Decimal
    """Implied volatility (exact ``Decimal``)."""

    delta: Decimal
    """Option delta (exact ``Decimal``)."""

    gamma: Decimal
    """Option gamma (exact ``Decimal``; negative when dealers are short)."""

    def __post_init__(self) -> None:
        option_type = self.option_type.upper()
        if option_type not in _OPTION_TYPES:
            raise ValueError(
                f"invalid option_type {self.option_type!r}; must be one of {_OPTION_TYPES}"
            )
        object.__setattr__(self, "option_type", option_type)
        object.__setattr__(self, "strike", to_decimal(self.strike))
        object.__setattr__(self, "open_interest", to_decimal(self.open_interest))
        object.__setattr__(self, "oi_change", to_decimal(self.oi_change))
        object.__setattr__(self, "implied_volatility", to_decimal(self.implied_volatility))
        object.__setattr__(self, "delta", to_decimal(self.delta))
        object.__setattr__(self, "gamma", to_decimal(self.gamma))
        if self.open_interest < 0:
            raise ValueError("open_interest must be non-negative")


@dataclass(frozen=True, slots=True)
class OptionChainSnapshot:
    """An intraday snapshot of the full option chain for one underlying/expiry."""

    as_of: datetime
    """Observation timestamp, normalized to UTC (tz-aware)."""

    underlying: str
    """Underlying symbol (e.g. ``"NIFTY"``)."""

    expiry: date
    """Contract expiry date of the snapshot's option series."""

    strikes: list[StrikePositioning] = field(default_factory=list)
    """Strike-wise call/put positioning rows for this snapshot."""

    source: str = "nse"
    """Publisher of the snapshot (``"nse"`` live, ``"fixture"`` for the
    offline recorded fallback — never fabricated as live data)."""

    def __post_init__(self) -> None:
        if self.as_of is None or self.as_of.tzinfo is None:
            raise ValueError("as_of must be a timezone-aware datetime (UTC)")
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))
        if not self.underlying:
            raise ValueError("underlying must be non-empty")
        if self.expiry is None:
            raise ValueError("expiry must be set")
        if not self.source:
            raise ValueError("source must be non-empty")


@dataclass(frozen=True, slots=True)
class StrikeGamma:
    """Per-strike gamma exposure of a snapshot."""

    strike: Decimal
    """Strike price (exact ``Decimal``)."""

    gamma: Decimal
    """Gamma used for the exposure (exact ``Decimal``)."""

    open_interest: Decimal
    """Open interest used for the exposure (exact ``Decimal``)."""

    gex: Decimal
    """Gamma exposure = ``gamma * open_interest * lot_size * spot``."""


@dataclass(frozen=True, slots=True)
class GammaExposure:
    """The full GEX profile of one snapshot."""

    per_strike: list[StrikeGamma]
    """Per-strike GEX rows, ordered by ascending strike (then option type)."""

    net: Decimal
    """Net (summed) gamma exposure of the snapshot (exact ``Decimal``)."""

    flip: Decimal | None
    """Strike where cumulative GEX changes sign, or ``None`` if it never
    crosses zero."""


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def compute_gamma_exposure(
    snapshot: OptionChainSnapshot,
    lot_size: Decimal,
    spot: Decimal,
) -> GammaExposure:
    """Compute the gamma-exposure profile of ``snapshot`` (exact ``Decimal``).

    ``lot_size`` is the exchange lot size in contracts and ``spot`` the
    underlying spot price. Per-strike GEX is ``gamma * open_interest * lot *
    spot``; ``net`` is the exact sum over every row; ``flip`` is the strike
    (walking strikes ascending) where the cumulative GEX changes sign, or
    ``None`` when the cumulative GEX never crosses zero.
    """
    lot = to_decimal(lot_size)
    spot_value = to_decimal(spot)
    if lot <= 0:
        raise ValueError("lot_size must be positive")
    if spot_value <= 0:
        raise ValueError("spot must be positive")

    per_strike: list[StrikeGamma] = []
    cumulative = Decimal("0")
    flip: Decimal | None = None
    for positioning in sorted(snapshot.strikes, key=lambda s: (s.strike, s.option_type)):
        gex = positioning.gamma * positioning.open_interest * lot * spot_value
        per_strike.append(
            StrikeGamma(
                strike=positioning.strike,
                gamma=positioning.gamma,
                open_interest=positioning.open_interest,
                gex=gex,
            )
        )
        new_cumulative = cumulative + gex
        if flip is None:
            previous_sign = _sign(cumulative)
            current_sign = _sign(new_cumulative)
            if previous_sign != 0 and current_sign != 0 and previous_sign != current_sign:
                flip = positioning.strike
        cumulative = new_cumulative
    return GammaExposure(per_strike=per_strike, net=cumulative, flip=flip)
