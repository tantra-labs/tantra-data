"""Primary-market IPO issue and subscription value objects.

Gap 0022 adds the NSE primary-market feed as the vendor: issue metadata
(:class:`IpoIssue`: symbol / company / open-close dates / price band / lot
size / issue size — exact ``Decimal`` for the band and size) and live
subscription snapshots (:class:`IpoSubscription`: QIB / NII / b-NII / s-NII /
RII / total multiples — exact ``Decimal``).

Zero-floats invariant: every price band, lot size, issue size, and subscription
multiple is an exact ``decimal.Decimal``. Value objects are frozen,
self-validating dataclasses that compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from tantra_data._compat import to_decimal

_DEFAULT_SOURCE = "nse"

_VALID_BOARDS = ("EQ", "SME")
_VALID_STATUSES = ("open", "upcoming", "listed")
_SUBSCRIPTION_FIELDS = ("qib", "nii", "bnii", "snii", "rii", "total")


def _non_empty(value: str, field: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError(f"{field} must be non-empty")
    return text


def _optional_decimal(value: Decimal | int | str | None) -> Decimal | None:
    if value is None:
        return None
    decimal_value = to_decimal(value)
    if decimal_value < 0:
        raise ValueError(f"value must be non-negative, got {decimal_value}")
    return decimal_value


def _require_positive_decimal(value: Decimal | int | str, field: str) -> Decimal:
    decimal_value = to_decimal(value)
    if decimal_value <= 0:
        raise ValueError(f"{field} must be positive, got {decimal_value}")
    return decimal_value


@dataclass(frozen=True, slots=True)
class IpoIssue:
    """One primary-market issue's metadata as of a snapshot date.

    ``as_of_date`` is the date the issue record was observed (point-in-time
    semantics: a query on date ``D`` sees the issue exactly as it was
    disclosed as of ``D``). ``price_band_low`` / ``price_band_high`` and
    ``issue_size_cr`` are exact ``Decimal``; ``issue_price`` is the final
    offer price for listed issues (``None`` while the book is open).
    """

    symbol: str
    """Exchange symbol of the issue (e.g. ``"VPFL"``)."""

    company_name: str
    """Company name of the issuer."""

    board: str
    """Board: ``"EQ"`` (mainboard) or ``"SME"``."""

    status: str
    """Lifecycle status: ``"open"`` | ``"upcoming"`` | ``"listed"``."""

    open_date: date
    """Issue opening date (bidding start)."""

    close_date: date
    """Issue closing date (bidding end)."""

    listing_date: date | None
    """Listing date, or ``None`` before the issue lists."""

    price_band_low: Decimal
    """Lower end of the price band (exact ``Decimal``)."""

    price_band_high: Decimal
    """Upper end of the price band (exact ``Decimal``)."""

    issue_price: Decimal | None
    """Final offer price for listed issues, else ``None``."""

    lot_size: Decimal | None
    """Bid lot size (exact ``Decimal``), or ``None`` when not disclosed."""

    issue_size_cr: Decimal | None
    """Issue size in INR crores (exact ``Decimal``), or ``None``."""

    as_of_date: date
    """Snapshot date the issue record was observed."""

    source: str = _DEFAULT_SOURCE
    """Publisher of the record (e.g. ``"nse"``)."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _non_empty(self.symbol, "symbol").upper())
        object.__setattr__(self, "company_name", _non_empty(self.company_name, "company_name"))
        board = self.board.upper()
        if board not in _VALID_BOARDS:
            raise ValueError(f"invalid board {self.board!r}; must be one of {_VALID_BOARDS}")
        object.__setattr__(self, "board", board)
        status = self.status.lower()
        if status not in _VALID_STATUSES:
            raise ValueError(f"invalid status {self.status!r}; must be one of {_VALID_STATUSES}")
        object.__setattr__(self, "status", status)
        if self.open_date > self.close_date:
            raise ValueError(f"open_date {self.open_date} must be <= close_date {self.close_date}")
        low = _require_positive_decimal(self.price_band_low, "price_band_low")
        high = _require_positive_decimal(self.price_band_high, "price_band_high")
        if high < low:
            raise ValueError(f"price_band_high {high} must be >= price_band_low {low}")
        object.__setattr__(self, "price_band_low", low)
        object.__setattr__(self, "price_band_high", high)
        object.__setattr__(self, "issue_price", _optional_decimal(self.issue_price))
        lot_size = _optional_decimal(self.lot_size)
        if lot_size is not None and lot_size <= 0:
            raise ValueError(f"lot_size must be positive, got {lot_size}")
        object.__setattr__(self, "lot_size", lot_size)
        issue_size = _optional_decimal(self.issue_size_cr)
        if issue_size is not None and issue_size <= 0:
            raise ValueError(f"issue_size_cr must be positive, got {issue_size}")
        object.__setattr__(self, "issue_size_cr", issue_size)
        if not self.source:
            raise ValueError("source must be non-empty")


@dataclass(frozen=True, slots=True)
class IpoSubscription:
    """A snapshot of an issue's category-wise subscription multiples.

    Multiples are times-subscribed figures as reported by the exchange
    (e.g. ``"44.12"`` = 44.12x), stored as exact ``Decimal``; ``None`` when the
    exchange had not yet reported the category (never fabricated).
    """

    symbol: str
    """Exchange symbol of the issue."""

    as_of_date: date
    """Snapshot date the subscription figures were observed."""

    qib: Decimal | None
    """Qualified Institutional Buyers multiple (exact ``Decimal``)."""

    nii: Decimal | None
    """Non-Institutional Investors multiple (exact ``Decimal``)."""

    bnii: Decimal | None
    """Big NII (bids > 10 lakh) multiple (exact ``Decimal``)."""

    snii: Decimal | None
    """Small NII (bids 2-10 lakh) multiple (exact ``Decimal``)."""

    rii: Decimal | None
    """Retail Individual Investors multiple (exact ``Decimal``)."""

    total: Decimal | None
    """Overall subscription multiple (exact ``Decimal``)."""

    source: str = _DEFAULT_SOURCE
    """Publisher of the record (e.g. ``"nse"``)."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _non_empty(self.symbol, "symbol").upper())
        for name in _SUBSCRIPTION_FIELDS:
            object.__setattr__(self, name, _optional_decimal(getattr(self, name)))
        if not self.source:
            raise ValueError("source must be non-empty")


__all__ = ["IpoIssue", "IpoSubscription"]
