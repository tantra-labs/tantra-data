"""Shareholding-pattern, promoter-pledge and insider-disclosure value objects.

Closes docs/gaps/0024-shareholding-sast-estimates.md: the lakehouse needs three
governance/disclosure datasets that block the Shareholding / Forecaster and
Red-Flag screen categories and insider-deal alerts:

* :class:`ShareholdingPattern` — one SEBI shareholding-pattern category
  (promoter & promoter group, FII/FPI, DII, mutual funds, public, …) for one
  company and quarter, point-in-time by the date the filing became public
  (``filing_date``). Source: BSE LODR Reg 31 iXBRL filings (verified live).
* :class:`PledgeRecord` — the promoter-pledge / encumbrance snapshot for one
  company and quarter (shares pledged, pledged % of total shares, promoter
  holding %). Source: NSE ``corporate-pledgedata`` (verified live).
* :class:`InsiderDisclosure` — one SEBI PIT (Prohibition of Insider Trading)
  disclosure by an insider (promoter/director/KMP/relative): traded dates,
  buy/sell quantities and values, and before/after holdings. Source: NSE
  ``corporates-pit`` (verified live).

Zero-floats invariant: every percentage and money value is an exact
``decimal.Decimal`` — never ``float``. Value objects are frozen,
self-validating dataclasses that compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from tantra_data._compat import D, to_decimal

__all__ = [
    "InsiderDisclosure",
    "PledgeRecord",
    "ShareholdingPattern",
]


@dataclass(frozen=True, slots=True)
class ShareholdingPattern:
    """One category's shareholding for one company and quarter (SEBI format).

    ``filing_date`` is the date the filing became public (the exchange
    broadcast), so a consumer filtering ``filing_date <= as_of`` sees exactly
    the pattern known at ``as_of`` — no look-ahead. ``shareholding_pct`` is
    the exact ``Decimal`` percentage of total issued shares; ``no_of_shares``
    the exact share count.
    """

    instrument: str
    """Company identifier in the source's terms (BSE scripcode for BSE)."""

    company_name: str
    """Company legal name (e.g. ``"Reliance Industries Ltd"``)."""

    exchange: str
    """Exchange that published the filing (e.g. ``"BSE"``)."""

    filing_date: date
    """Date the shareholding-pattern filing became public."""

    quarter_end: date
    """End date of the quarter the pattern refers to."""

    category: str
    """Normalized SEBI shareholding category (e.g. ``"promoter_promoter_group"``,
    ``"fii_fpi"``, ``"mutual_funds"``, ``"public"``, ``"total"``)."""

    no_of_shares: int
    """Number of fully paid-up equity shares held in this category (exact int)."""

    shareholding_pct: Decimal
    """Shareholding as a percentage of total shares (exact ``Decimal``)."""

    source: str = "bse"
    """Publisher of the record (``"bse"`` live, ``"fixture"`` for the offline
    recorded fallback — never fabricated as live data)."""

    def __post_init__(self) -> None:
        if not self.instrument:
            raise ValueError("instrument must be non-empty")
        if not self.company_name:
            raise ValueError("company_name must be non-empty")
        if not self.exchange:
            raise ValueError("exchange must be non-empty")
        if self.filing_date is None:
            raise ValueError("filing_date must be set")
        if self.quarter_end is None:
            raise ValueError("quarter_end must be set")
        if not self.category:
            raise ValueError("category must be non-empty")
        if self.no_of_shares < 0:
            raise ValueError("no_of_shares must be non-negative")
        object.__setattr__(self, "shareholding_pct", to_decimal(self.shareholding_pct))
        if self.shareholding_pct < 0:
            raise ValueError("shareholding_pct must be non-negative")
        if not self.source:
            raise ValueError("source must be non-empty")


@dataclass(frozen=True, slots=True)
class PledgeRecord:
    """Promoter-pledge / encumbrance snapshot for one company and quarter.

    ``quarter_end`` (NSE ``shp``) is the shareholding-pattern quarter the
    pledge refers to; ``broadcast_date`` (NSE ``broadcastDt``) is when the
    snapshot became public. ``pledged_pct`` is the pledged quantity as a
    percentage of *total* issued shares (NSE ``percSharesPledged``); all
    percentages are exact ``Decimal``.
    """

    instrument: str
    """Company identifier in the source's terms (NSE company name)."""

    company_name: str
    """Company name (e.g. ``"20 Microns Limited"``)."""

    quarter_end: date
    """Shareholding-pattern quarter end the pledge refers to."""

    broadcast_date: date
    """Date the pledge snapshot became public."""

    promoter_holding_pct: Decimal
    """Promoter & promoter-group holding as a percentage of total shares."""

    pledged_shares: int
    """Number of promoter shares pledged / encumbered (exact int)."""

    pledged_pct: Decimal
    """Pledged shares as a percentage of total issued shares (exact ``Decimal``)."""

    promoter_shares: int
    """Total promoter & promoter-group shares (exact int)."""

    total_issued_shares: int
    """Total issued shares of the company (exact int)."""

    source: str = "nse"
    """Publisher of the record (``"nse"`` live, ``"fixture"`` for the offline
    recorded fallback — never fabricated as live data)."""

    def __post_init__(self) -> None:
        if not self.instrument:
            raise ValueError("instrument must be non-empty")
        if not self.company_name:
            raise ValueError("company_name must be non-empty")
        if self.quarter_end is None:
            raise ValueError("quarter_end must be set")
        if self.broadcast_date is None:
            raise ValueError("broadcast_date must be set")
        for name in ("pledged_shares", "promoter_shares", "total_issued_shares"):
            value = getattr(self, name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        object.__setattr__(self, "promoter_holding_pct", to_decimal(self.promoter_holding_pct))
        object.__setattr__(self, "pledged_pct", to_decimal(self.pledged_pct))
        if self.promoter_holding_pct < 0:
            raise ValueError("promoter_holding_pct must be non-negative")
        if self.pledged_pct < 0:
            raise ValueError("pledged_pct must be non-negative")
        if not self.source:
            raise ValueError("source must be non-empty")


@dataclass(frozen=True, slots=True)
class InsiderDisclosure:
    """One SEBI PIT insider-trading disclosure (Reg 7(2) / 6(2) format).

    ``trade_date`` is the date the insider traded (the acquisition/sale
    window start); ``intimation_date`` is when the disclosure was intimated to
    the exchange. Quantities are exact ints; ``value`` and the before/after
    percentages are exact ``Decimal``.
    """

    instrument: str
    """Exchange symbol (e.g. ``"RELIANCE"``)."""

    company_name: str
    """Company name (e.g. ``"Reliance Industries Limited"``)."""

    disclosure_id: int
    """Row id of the disclosure in the exchange feed (unique per row)."""

    trade_date: date
    """Date the insider trade(s) covered by the disclosure occurred."""

    intimation_date: date
    """Date the disclosure was intimated to the exchange."""

    name: str
    """Name of the disclosing insider."""

    person_category: str
    """Insider category (e.g. ``"Promoter"``, ``"Director"``, ``"KMP"``,
    ``"Immediate relative"``, ``"Other"``)."""

    transaction_type: str
    """Normalized transaction type: ``"BUY"``, ``"SELL"`` or ``"PLEDGE"``
    (pledge creation / revocation)."""

    acq_mode: str
    """Raw acquisition mode from the exchange feed (e.g. ``"Market Purchase"``,
    ``"Off Market"``, ``"Pledge Creation"``, ``"Gift"``)."""

    acquired_shares: int
    """Net shares acquired/sold in the covered window (exact int; negative
    indicates a net sale)."""

    buy_quantity: int
    """Gross shares bought in the covered window (exact int)."""

    sell_quantity: int
    """Gross shares sold in the covered window (exact int)."""

    value: Decimal
    """Transaction value of the covered trades (exact ``Decimal``)."""

    before_shares: int
    """Shares held by the insider before the trade(s)."""

    before_pct: Decimal
    """Holding percentage before the trade(s) (exact ``Decimal``)."""

    after_shares: int
    """Shares held by the insider after the trade(s)."""

    after_pct: Decimal
    """Holding percentage after the trade(s) (exact ``Decimal``)."""

    source: str = "nse"
    """Publisher of the record (``"nse"`` live, ``"fixture"`` for the offline
    recorded fallback — never fabricated as live data)."""

    def __post_init__(self) -> None:
        if not self.instrument:
            raise ValueError("instrument must be non-empty")
        if not self.company_name:
            raise ValueError("company_name must be non-empty")
        if self.trade_date is None:
            raise ValueError("trade_date must be set")
        if self.intimation_date is None:
            raise ValueError("intimation_date must be set")
        if not self.name:
            raise ValueError("name must be non-empty")
        if not self.person_category:
            raise ValueError("person_category must be non-empty")
        if not self.acq_mode:
            raise ValueError("acq_mode must be non-empty")
        transaction_type = _normalize_transaction_type(self.transaction_type)
        object.__setattr__(self, "transaction_type", transaction_type)
        for name in (
            "buy_quantity",
            "sell_quantity",
            "before_shares",
            "after_shares",
        ):
            value = getattr(self, name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        object.__setattr__(self, "value", to_decimal(self.value))
        object.__setattr__(self, "before_pct", to_decimal(self.before_pct))
        object.__setattr__(self, "after_pct", to_decimal(self.after_pct))
        if self.value < 0:
            raise ValueError("value must be non-negative")
        if self.before_pct < 0 or self.after_pct < 0:
            raise ValueError("before_pct and after_pct must be non-negative")
        if not self.source:
            raise ValueError("source must be non-empty")


def pledged_pct_of_promoter(record: PledgeRecord) -> Decimal:
    """Pledged shares as a percentage of promoter holding (exact ``Decimal``).

    ``0`` when the promoter holds no shares. This is the investor-relevant
    red-flag ratio (a high fraction of the promoter holding encumbered).
    """
    if record.promoter_shares <= 0:
        return D(0)
    return D(record.pledged_shares) / D(record.promoter_shares) * D(100)


def _normalize_transaction_type(raw: str) -> str:
    """Normalize a raw NSE transaction-type to ``BUY`` / ``SELL`` / ``PLEDGE``."""
    value = raw.strip().lower()
    if value.startswith("buy"):
        return "BUY"
    if value.startswith("sell"):
        return "SELL"
    if "pledge" in value:
        return "PLEDGE"
    raise ValueError(f"invalid transaction_type {raw!r}; expected Buy, Sell or Pledge")
