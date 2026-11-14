"""Mutual-fund scheme metadata, NAV history, and derived return value objects.

Gap 0021 adds the AMFI daily NAV dump (``NAVAll.txt``) as the vendor: scheme
metadata (:class:`MutualFundMeta`: scheme code / name / category / ISIN / AMC),
per-scheme daily NAV history (:class:`MutualFundNav`: scheme code, nav date,
exact-``Decimal`` NAV, repurchase/sale price where the vendor supplies it), and
derived return metrics (:class:`MutualFundReturn`: exact-``Decimal`` CAGR, and
expense ratio where the vendor supplies it).

Zero-floats invariant: every NAV, price, and derived return is an exact
``decimal.Decimal``. Value objects are frozen, self-validating dataclasses that
compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from tantra_data._compat import to_decimal

_DEFAULT_SOURCE = "amfi"

_OPTIONAL_FIELDS = ("category", "isin_growth", "isin_div_reinvestment", "plan", "option")


def _non_empty(value: str, field: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError(f"{field} must be non-empty")
    return text


def _optional_decimal(value: Decimal | int | str | None) -> Decimal | None:
    """Coerce an optional money/ratio value to an exact ``Decimal`` (or ``None``)."""
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
class MutualFundMeta:
    """Scheme-level metadata observed in one AMFI NAV dump.

    ``as_of_date`` is the date of the NAV dump the metadata was observed in
    (point-in-time semantics: a query on date ``D`` sees the scheme exactly as
    it was disclosed as of ``D``).
    """

    scheme_code: str
    """AMFI scheme code (numeric string, e.g. ``"135762"``)."""

    scheme_name: str
    """Scheme name as disclosed by the vendor (e.g. ``"Axis Children's Fund"``)."""

    amc: str
    """Asset-management company that operates the scheme (e.g. ``"Axis Mutual Fund"``)."""

    category: str | None
    """Fund category from the AMFI section header when parseable (e.g.
    ``"Children's Fund - Childrens' Fund"``), else ``None``."""

    isin_growth: str | None
    """ISIN of the growth / dividend-payout option, or ``None`` when the vendor
    reports ``-``."""

    isin_div_reinvestment: str | None
    """ISIN of the dividend-reinvestment option, or ``None`` when the vendor
    reports ``-``."""

    plan: str | None
    """Plan (``"Direct Plan"`` / ``"Regular Plan"``), or ``None`` when blank."""

    option: str | None
    """Option (``"Growth Option"`` / ``"IDCW Option"`` / ...), or ``None`` when
    blank."""

    as_of_date: date
    """Date of the NAV dump the metadata was observed in."""

    source: str = _DEFAULT_SOURCE
    """Publisher of the record (e.g. ``"amfi"``)."""

    def __post_init__(self) -> None:
        code = _non_empty(self.scheme_code, "scheme_code")
        if not code.isdigit():
            raise ValueError(f"scheme_code must be numeric digits, got {self.scheme_code!r}")
        object.__setattr__(self, "scheme_code", code)
        object.__setattr__(self, "scheme_name", _non_empty(self.scheme_name, "scheme_name"))
        object.__setattr__(self, "amc", _non_empty(self.amc, "amc"))
        for name in _OPTIONAL_FIELDS:
            value = getattr(self, name)
            if value is not None:
                cleaned = value.strip()
                object.__setattr__(self, name, cleaned or None)
        if not self.source:
            raise ValueError("source must be non-empty")


@dataclass(frozen=True, slots=True)
class MutualFundNav:
    """One scheme's NAV observation on a date (exact ``Decimal``).

    ``repurchase_price`` / ``sale_price`` are carried only where the vendor
    supplies them (older AMFI dumps included them; the current daily dump does
    not) and are ``None`` otherwise — never fabricated.
    """

    scheme_code: str
    """AMFI scheme code."""

    nav_date: date
    """Date the NAV was disclosed."""

    nav: Decimal
    """Net asset value per unit (exact ``Decimal``)."""

    repurchase_price: Decimal | None
    """Repurchase price when the vendor supplies it, else ``None``."""

    sale_price: Decimal | None
    """Sale price when the vendor supplies it, else ``None``."""

    source: str = _DEFAULT_SOURCE
    """Publisher of the record (e.g. ``"amfi"``)."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "scheme_code", _non_empty(self.scheme_code, "scheme_code"))
        object.__setattr__(self, "nav", _require_positive_decimal(self.nav, "nav"))
        object.__setattr__(self, "repurchase_price", _optional_decimal(self.repurchase_price))
        object.__setattr__(self, "sale_price", _optional_decimal(self.sale_price))
        if not self.source:
            raise ValueError("source must be non-empty")


@dataclass(frozen=True, slots=True)
class MutualFundReturn:
    """Derived return metrics for a scheme as of a date.

    ``cagr_1y`` is the exact-``Decimal`` annualized growth rate between the
    earliest and latest observed NAVs (e.g. ``0.1234`` = 12.34%), or ``None``
    when history is insufficient to derive it (a single NAV point cannot
    produce a rate — returns are never fabricated). ``expense_ratio`` is
    carried from the vendor where supplied and is ``None`` otherwise.
    """

    scheme_code: str
    """AMFI scheme code."""

    as_of_date: date
    """Date the derived return was computed (the ingest snapshot date)."""

    cagr_1y: Decimal | None
    """Annualized (CAGR) growth over the observed NAV span, or ``None``."""

    expense_ratio: Decimal | None
    """Annual expense ratio where the vendor supplies it, else ``None``."""

    source: str = _DEFAULT_SOURCE
    """Publisher of the underlying records (e.g. ``"amfi"``)."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "scheme_code", _non_empty(self.scheme_code, "scheme_code"))
        object.__setattr__(self, "cagr_1y", _optional_decimal(self.cagr_1y))
        object.__setattr__(self, "expense_ratio", _optional_decimal(self.expense_ratio))
        if not self.source:
            raise ValueError("source must be non-empty")


__all__ = ["MutualFundMeta", "MutualFundNav", "MutualFundReturn"]
