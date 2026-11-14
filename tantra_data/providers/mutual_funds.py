"""AMFI daily NAV dump mutual-fund provider (gap 0021).

Fetches the official, free, machine-readable AMFI daily NAV dump
(``https://www.amfiindia.com/spages/NAVAll.txt``) and maps it onto canonical
:class:`~tantra_data.domain.mutual_funds.MutualFundMeta` /
:class:`~tantra_data.domain.mutual_funds.MutualFundNav` /
:class:`~tantra_data.domain.mutual_funds.MutualFundReturn` rows.

Verified live (2026-11-14): the dump is reachable over plain HTTPS (no cookie
challenge) and is a semicolon-delimited file whose data rows carry exactly
eight fields::

    Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;
    Plan;Option;Net Asset Value;Date

Section headers delimit the fund category (``Open Ended Schemes(Children's
Fund - Childrens' Fund)``) and the AMC (``Axis Mutual Fund``). The current
dump does not carry repurchase/sale price columns (older dumps did) and does
not carry expense ratios — those fields are parsed where the vendor supplies
them and are ``None`` otherwise (never fabricated). Holdings are NOT part of
this provider: free machine-readable holdings sources did not verify live
during this task, so gap 0021 stays OPEN on the holdings slice.

Zero-floats invariant: every NAV and derived CAGR is an exact
``decimal.Decimal`` (``Decimal(str(value))`` at the parse boundary) — never
``float``.
"""

from __future__ import annotations

import re
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Protocol

from tantra_data.domain.mutual_funds import MutualFundMeta, MutualFundNav, MutualFundReturn

__all__ = [
    "MutualFundRow",
    "ParsedMutualFunds",
    "derive_cagr",
    "derive_returns",
    "fetch_mutual_funds",
    "ingest_mutual_funds",
    "parse_nav_all",
    "rows_to_meta",
    "rows_to_navs",
]

NAV_ALL_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
SOURCE = "amfi"
_DATE_FORMAT = "%d-%b-%Y"
_HEADER_PREFIX = "Scheme Code;"
_CATEGORY_RE = re.compile(r"^(?:Open|Close) Ended Schemes\s*(?:\((.*)\))?\s*$")
_QUANT = Decimal("0.0001")
_DAYS_PER_YEAR = Decimal(365)

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


@dataclass(frozen=True)
class MutualFundRow:
    """One parsed AMFI NAV row (vendor-shaped, before domain mapping)."""

    scheme_code: str
    isin_growth: str | None
    isin_div_reinvestment: str | None
    scheme_name: str
    plan: str | None
    option: str | None
    nav: Decimal
    nav_date: date
    repurchase_price: Decimal | None
    sale_price: Decimal | None
    amc: str
    category: str | None


@dataclass(frozen=True)
class ParsedMutualFunds:
    """Canonical parsed rows from one AMFI NAV dump."""

    as_of_date: date
    rows: list[MutualFundRow]


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text == "-":
        return None
    return text


def _to_decimal(value: str | None, field: str) -> Decimal | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid {field!r} value in AMFI NAV dump: {value!r}") from exc


def _to_date(value: str | None, field: str) -> date:
    if value is None:
        raise ValueError(f"missing {field!r} date in AMFI NAV dump")
    try:
        return datetime.strptime(value.strip(), _DATE_FORMAT).date()
    except ValueError as exc:
        raise ValueError(f"invalid {field!r} date in AMFI NAV dump: {value!r}") from exc


def _mode_date(dates: list[date]) -> date:
    """Most common date in the dump (ties break toward the later date)."""
    counts: dict[date, int] = {}
    for day in dates:
        counts[day] = counts.get(day, 0) + 1
    most_common = max(counts.values())
    candidates = [day for day, count in counts.items() if count == most_common]
    return max(candidates)


def parse_nav_all(text: str) -> ParsedMutualFunds:
    """Deterministically parse an AMFI ``NAVAll.txt`` dump.

    Blank lines and the mandatory header row are skipped. Lines without a
    semicolon are section headers: a ``* Ended Schemes(...)`` line sets the
    fund category, any other non-data line sets the AMC. Data rows carry eight
    fields in the current format (scheme code, growth ISIN, div-reinvestment
    ISIN, scheme name, plan, option, NAV, date); a seven-field legacy format
    (scheme code, ISIN, scheme name, NAV, repurchase price, sale price, date)
    is also accepted. Rows whose scheme code is not numeric are skipped
    (never fabricated).
    """
    current_category: str | None = None
    current_amc: str | None = None
    rows: list[MutualFundRow] = []
    nav_dates: list[date] = []

    for line in text.splitlines():
        if not line.strip():
            continue
        if line.startswith(_HEADER_PREFIX):
            continue
        if ";" not in line:
            category_match = _CATEGORY_RE.match(line.strip())
            if category_match is not None:
                current_category = category_match.group(1) or line.strip()
            else:
                current_amc = line.strip()
            continue
        fields = [field.strip() for field in line.split(";")]
        row = _parse_data_row(fields, current_amc, current_category)
        if row is None:
            continue
        rows.append(row)
        nav_dates.append(row.nav_date)

    if not rows:
        raise ValueError("AMFI NAV dump contains no parseable scheme rows")
    return ParsedMutualFunds(as_of_date=_mode_date(nav_dates), rows=rows)


def _parse_data_row(
    fields: list[str],
    amc: str | None,
    category: str | None,
) -> MutualFundRow | None:
    """Parse one semicolon-split data row into a row, or ``None`` to skip."""
    scheme_code = fields[0]
    if not scheme_code.isdigit():
        return None
    if len(fields) >= 8:
        isin_growth = _clean(fields[1])
        isin_div = _clean(fields[2])
        scheme_name = fields[3]
        plan = _clean(fields[4])
        option = _clean(fields[5])
        nav = _to_decimal(fields[6], "Net Asset Value")
        nav_date = _to_date(fields[7], "Date")
        repurchase_price: Decimal | None = None
        sale_price: Decimal | None = None
    elif len(fields) >= 7:  # legacy: code;ISIN;name;nav;repurchase;sale;date
        isin_growth = _clean(fields[1])
        isin_div = None
        scheme_name = fields[2]
        plan = None
        option = None
        nav = _to_decimal(fields[3], "Net Asset Value")
        nav_date = _to_date(fields[6], "Date")
        repurchase_price = _to_decimal(fields[4], "Repurchase Price")
        sale_price = _to_decimal(fields[5], "Sale Price")
    else:
        return None
    if nav is None or nav <= 0:
        return None
    return MutualFundRow(
        scheme_code=scheme_code,
        isin_growth=isin_growth,
        isin_div_reinvestment=isin_div,
        scheme_name=scheme_name,
        plan=plan,
        option=option,
        nav=nav,
        nav_date=nav_date,
        repurchase_price=repurchase_price,
        sale_price=sale_price,
        amc=amc or "Unknown AMC",
        category=category,
    )


def rows_to_meta(row: MutualFundRow, as_of: date) -> MutualFundMeta:
    """Map one parsed row onto the canonical scheme-metadata value."""
    return MutualFundMeta(
        scheme_code=row.scheme_code,
        scheme_name=row.scheme_name,
        amc=row.amc,
        category=row.category,
        isin_growth=row.isin_growth,
        isin_div_reinvestment=row.isin_div_reinvestment,
        plan=row.plan,
        option=row.option,
        as_of_date=as_of,
        source=SOURCE,
    )


def rows_to_navs(rows: list[MutualFundRow]) -> list[MutualFundNav]:
    """Map parsed rows onto canonical NAV-history values."""
    return [
        MutualFundNav(
            scheme_code=row.scheme_code,
            nav_date=row.nav_date,
            nav=row.nav,
            repurchase_price=row.repurchase_price,
            sale_price=row.sale_price,
            source=SOURCE,
        )
        for row in rows
    ]


def derive_cagr(navs: list[MutualFundNav]) -> Decimal | None:
    """Annualized growth (CAGR) over the observed NAV span, exact ``Decimal``.

    ``(end_nav / start_nav) ** (1 / years) - 1`` computed with exact
    ``Decimal`` arithmetic at high context precision and quantized to 4dp.
    Returns ``None`` when history is insufficient (fewer than two points,
    zero span, or a non-positive base) — a rate is never fabricated.
    """
    if len(navs) < 2:
        return None
    sorted_navs = sorted(navs, key=lambda row: row.nav_date)
    earliest = sorted_navs[0]
    latest = sorted_navs[-1]
    days = (latest.nav_date - earliest.nav_date).days
    if days <= 0 or earliest.nav <= 0:
        return None
    years = Decimal(days) / _DAYS_PER_YEAR
    growth = latest.nav / earliest.nav
    if growth <= 0:
        return None
    with localcontext() as ctx:
        ctx.prec = 28
        cagr = (growth ** (Decimal(1) / years)) - Decimal(1)
    return cagr.quantize(_QUANT)


def derive_returns(
    navs: list[MutualFundNav],
    as_of: date,
    expense_ratio: Decimal | None = None,
) -> MutualFundReturn:
    """Build a derived-return record for one scheme's NAV history."""
    return MutualFundReturn(
        scheme_code=navs[0].scheme_code,
        as_of_date=as_of,
        cagr_1y=derive_cagr(navs),
        expense_ratio=expense_ratio,
        source=SOURCE,
    )


def fetch_mutual_funds() -> ParsedMutualFunds:
    """Fetch and parse the latest AMFI daily NAV dump."""
    request = urllib.request.Request(NAV_ALL_URL, headers={"User-Agent": _UA})
    with urllib.request.urlopen(request, timeout=60) as response:
        text = response.read().decode("utf-8", errors="replace")
    return parse_nav_all(text)


class _MetaStore(Protocol):
    """Minimal structural contract for a scheme-metadata store."""

    def upsert(self, meta: MutualFundMeta) -> None: ...


class _NavStore(Protocol):
    """Minimal structural contract for a NAV-history store."""

    def upsert(self, nav: MutualFundNav) -> None: ...

    def query(
        self,
        scheme_code: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MutualFundNav]: ...


class _ReturnStore(Protocol):
    """Minimal structural contract for a derived-return store."""

    def upsert(self, returns: MutualFundReturn) -> None: ...


def ingest_mutual_funds(
    meta_store: _MetaStore,
    nav_store: _NavStore,
    return_store: _ReturnStore,
) -> tuple[int, int, int]:
    """Fetch the latest AMFI dump and upsert metas, NAVs, and returns.

    Returns ``(meta_count, nav_count, return_count)``. Returns are derived
    from the accumulated NAV history already in ``nav_store`` (old + new rows)
    so CAGR becomes available as daily dumps accumulate.
    """
    parsed = fetch_mutual_funds()
    metas = [rows_to_meta(row, parsed.as_of_date) for row in parsed.rows]
    navs = rows_to_navs(parsed.rows)
    for meta in metas:
        meta_store.upsert(meta)
    for nav in navs:
        nav_store.upsert(nav)

    by_scheme: dict[str, list[MutualFundNav]] = {}
    for nav in nav_store.query():
        by_scheme.setdefault(nav.scheme_code, []).append(nav)
    return_count = 0
    for scheme_navs in by_scheme.values():
        return_store.upsert(derive_returns(scheme_navs, parsed.as_of_date))
        return_count += 1
    return len(metas), len(navs), return_count
