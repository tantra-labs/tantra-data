"""tests/test_loaders.py: tantra_data.loaders + domain.quantity surfaces (ex 0003).

The loaders are wheel-optional: everything except bar construction is asserted directly,
and the ``Bar``-producing tests skip when the optional ``tantra`` wheel is not installed
(``pip install tantra[native]``), keeping the suite green in a no-wheel environment.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest

from tantra_data import parse_quantity
from tantra_data.domain.quantity import Quantity, QuantityError
from tantra_data.loaders.amfi import (
    EXCHANGE,
    AmfiNavLoader,
    NavRow,
    NavSkip,
    nav_bars,
    parse_navall,
)
from tantra_data.loaders.nse import parse_bhavcopy_csv
from tantra_data.loaders.yfinance import YFinanceProvider

_HAS_TANTRA = importlib.util.find_spec("tantra") is not None
_REQUIRES_TANTRA = pytest.mark.skipif(
    not _HAS_TANTRA,
    reason="requires the optional `tantra` wheel (pip install tantra[native])",
)

NAVALL = """Applic Date: 02-Jun-2025

100001;Sample Flexi Cap Fund - Growth;123.4567;02-Jun-2025
100002;Sample Index Fund - Direct Plan;89.1011;02-Jun-2025
100003;Sample Debt Fund - Growth;-;02-Jun-2025
"""

UDIFF_CSV = """TradDt;TckrSymb;SctySrs;OpnPric;HghPric;LwPric;ClsPric;TtlTradQty
2025-06-02;RELIANCE;EQ;2500.10;2540.00;2495.20;2530.50;123456
2025-06-02;HDFCBANK;BE;1700.00;1720.00;1690.00;1710.00;80000
2025-06-02;SOMEETF;N1;100.00;101.00;99.00;100.50;50000
"""

SEC_BHAVDATA_CSV = """DATE1;SYMBOL;SERIES;OPEN_PRICE;HIGH_PRICE;LOW_PRICE;CLOSE_PRICE;TTL_TRD_QNTY
02-06-2025;RELIANCE;EQ;2500.10;2540.00;2495.20;2530.50;123456
02-06-2025;HDFCBANK;BZ;1700.00;1720.00;1690.00;1710.00;80000
"""

LEGACY_CSV = """TIMESTAMP;SYMBOL;SERIES;OPEN;HIGH;LOW;CLOSE;TOTTRDQTY
02-06-2025;RELIANCE;EQ;2500.10;2540.00;2495.20;2530.50;123456
02-06-2025;HDFCBANK;BE;1700.00;1720.00;1690.00;1710.00;80000
"""


# ---------------------------------------------------------------- parse_navall / nav_bars


def test_parse_navall_parses_well_formed_rows_and_skips_header_and_blanks():
    rows = parse_navall(NAVALL)
    assert [row.code for row in rows] == ["100001", "100002"]
    assert rows[0] == NavRow(
        code="100001",
        name="Sample Flexi Cap Fund - Growth",
        nav=123.4567,
        date=dt.date(2025, 6, 2),
    )
    assert rows[1].nav == 89.1011


def test_parse_navall_missing_nav_is_skipped_with_line_code_and_reason():
    skipped: list[NavSkip] = []
    rows = parse_navall(NAVALL, skipped=skipped)
    assert len(rows) == 2
    assert len(skipped) == 1
    (skip,) = skipped
    assert skip.line == 5
    assert skip.code == "100003"
    assert "'-'" in skip.reason


@pytest.mark.parametrize("nav", ["-", "", "0", "-1.5", "nan", "inf"])
def test_parse_navall_skips_any_nav_that_is_not_positive_finite(nav):
    skipped: list[NavSkip] = []
    rows = parse_navall(f"Applic Date: 02-Jun-2025\n100004;Any Fund;{nav};02-Jun-2025\n", skipped)
    assert rows == []
    assert [skip.code for skip in skipped] == ["100004"]
    assert skipped[0].line == 2
    assert nav in skipped[0].reason


def test_parse_navall_structural_damage_raises_naming_the_offending_line():
    text = "Applic Date: 02-Jun-2025\n100001;Any Fund;1.0;02-Jun-2025\n100002;Any Fund;1.0\n"
    with pytest.raises(ValueError, match="line 3"):
        parse_navall(text)


@_REQUIRES_TANTRA
def test_nav_bars_are_decimal_and_stamped_at_the_session_open():
    rows = parse_navall(NAVALL)
    bars = nav_bars(rows)
    assert len(bars) == len(rows)
    first = bars[0]
    assert (first.open, first.high, first.low, first.close) == (
        Decimal("123.4567"),
        Decimal("123.4567"),
        Decimal("123.4567"),
        Decimal("123.4567"),
    )
    assert first.volume == Decimal("0")
    # 2025-06-02 03:45 UTC == 09:15 IST, the daily-bar stamp the store uses.
    assert first.ts == 1_748_835_900_000_000_000
    assert first.instrument_id.exchange == EXCHANGE


# ---------------------------------------------------------------- AmfiNavLoader basic flow


def test_amfi_nav_loader_basic_flow(tmp_path: Path):
    """Construction needs neither the wheel nor any filesystem writes."""
    loader = AmfiNavLoader(tmp_path)
    assert loader.load_category("equity") == {}
    assert AmfiNavLoader().load_category("equity") == {}


# ---------------------------------------------------------------- parse_bhavcopy_csv dialects


@_REQUIRES_TANTRA
def test_parse_bhavcopy_csv_udiff_dialect_keeps_eq_and_clamps_ohlcv():
    bars = parse_bhavcopy_csv(UDIFF_CSV)
    assert set(bars) == {"RELIANCE", "HDFCBANK"}  # the N1 series is dropped
    reliance = bars["RELIANCE"]
    assert reliance.open == Decimal("2500.10")
    assert reliance.high == Decimal("2540.00")
    assert reliance.low == Decimal("2495.20")
    assert reliance.close == Decimal("2530.50")
    assert reliance.volume == Decimal("123456")
    assert reliance.instrument_id.exchange == "NSE"
    assert reliance.low <= min(reliance.open, reliance.close)
    assert reliance.high >= max(reliance.open, reliance.close)


@_REQUIRES_TANTRA
def test_parse_bhavcopy_csv_sec_bhavdata_dialect_keeps_be_and_bz():
    bars = parse_bhavcopy_csv(SEC_BHAVDATA_CSV)
    assert set(bars) == {"RELIANCE", "HDFCBANK"}
    assert bars["HDFCBANK"].close == Decimal("1710.00")
    assert bars["RELIANCE"].ts == bars["HDFCBANK"].ts


@_REQUIRES_TANTRA
def test_parse_bhavcopy_csv_legacy_dialect_keeps_eq_and_be():
    bars = parse_bhavcopy_csv(LEGACY_CSV)
    assert set(bars) == {"RELIANCE", "HDFCBANK"}
    assert bars["RELIANCE"].volume == Decimal("123456")


@_REQUIRES_TANTRA
def test_parse_bhavcopy_csv_unrecognised_header_returns_empty_dict():
    assert parse_bhavcopy_csv("Foo;Bar\n1;2\n") == {}


# ---------------------------------------------------------------- parse_quantity


def test_parse_quantity_crore():
    qty = parse_quantity("2.5 cr")
    assert qty.value == Decimal("25000000")
    assert qty == Quantity(Decimal("25000000"), Decimal("2.5"), "cr")
    assert tuple(qty) == (Decimal("25000000"), "cr")


def test_parse_quantity_lakh_and_thousand():
    assert parse_quantity("50 lakh").value == Decimal("5000000")
    assert parse_quantity("10k").value == Decimal("10000")
    assert parse_quantity("1,00,000").value == Decimal("100000")


def test_parse_quantity_percent():
    qty = parse_quantity("5 percent")
    assert qty.value == Decimal("0.05")
    assert qty.unit_suffix == "percent"


def test_parse_quantity_plain_number_gets_shares_suffix():
    qty = parse_quantity("5000")
    assert qty.value == Decimal("5000")
    assert tuple(qty) == (Decimal("5000"), "shares")


@pytest.mark.parametrize("bad", ["", "   ", "abc", "5 widgets", "cr", "-"])
def test_parse_quantity_errors(bad):
    with pytest.raises(QuantityError):
        parse_quantity(bad)


# ---------------------------------------------------------------- YFinanceProvider


def test_yfinance_provider_raises_clear_error_when_yfinance_unavailable(monkeypatch):
    monkeypatch.setattr("tantra_data.loaders.yfinance.yfinance_available", lambda: False)
    provider = YFinanceProvider()
    with pytest.raises(RuntimeError, match="yfinance is not installed"):
        provider.fetch(None, "1D", None)  # raises before touching inst/interval; no network
