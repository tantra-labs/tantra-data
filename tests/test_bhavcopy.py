"""NSE bhavcopy provider parsing."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl

from tantra_data.providers.bhavcopy import parse_bhavcopy, rows_to_bars

SAMPLE = (
    "SYMBOL,SERIES,DATE,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,LAST_PRICE,CLOSE_PRICE,"
    "AVERAGE_PRICE,TOTAL_TRADED_QUANTITY,TURNOVER_LACS,NO_OF_TRADES,DELIVERABLE_QTY,% DLVY TO QTY\n"
    "RELIANCE,EQ,02-Jan-2024,1250.00,1252.00,1265.00,1248.00,1260.00,1260.00,1256.75,"
    "4823112,6061.12,15234,3012000,62.45\n"
    "TATAMOTORS,EQ,02-Jan-2024,780.50,781.00,792.40,775.10,789.30,788.90,785.20,"
    "9123456,7163.99,22001,5040000,\n"
)

# Live NSE archive layout: ", " separators, renamed columns (DATE1/AVG_PRICE/
# TTL_TRD_QNTY/DELIV_QTY/DELIV_PER), and "-" delivery placeholders.
LIVE_SAMPLE = (
    "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, CLOSE_PRICE,"
    " AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER\n"
    "20MICRONS, EQ, 06-Nov-2026, 180.26, 181.00, 185.81, 177.00, 183.00, 183.22, 183.19, 82090,"
    " 150.38, 2891, 39593, 48.23\n"
    "21STCENMGM, BE, 06-Nov-2026, 25.00, 25.50, 26.00, 24.80, 25.75, 25.70, 25.60, 1000, 2.56,"
    " 10, -, -\n"
)


def test_parse_bhavcopy_parses_exact_decimals() -> None:
    rows = parse_bhavcopy(SAMPLE)
    assert len(rows) == 2

    reliance = rows[0]
    assert reliance.symbol == "RELIANCE"
    assert reliance.trade_date == date(2024, 1, 2)
    assert reliance.close == Decimal("1260.00")
    assert reliance.average_price == Decimal("1256.75")
    assert reliance.total_traded_quantity == 4_823_112
    assert reliance.deliverable_pct == Decimal("62.45")

    tata = rows[1]
    assert tata.deliverable_pct is None


def test_rows_to_bars_maps_to_canonical_schema() -> None:
    bars = rows_to_bars(parse_bhavcopy(SAMPLE))
    assert bars.columns == ["ts", "open", "high", "low", "close", "volume"]
    assert bars.height == 2
    assert bars["close"].dtype == pl.Decimal
    assert bars["close"].to_list() == [Decimal("1260.0000"), Decimal("788.9000")]
    assert bars["volume"].to_list() == [4_823_112, 9_123_456]


def test_empty_bhavcopy_returns_empty_frame() -> None:
    bars = rows_to_bars([])
    assert bars.is_empty()
    assert bars.columns == ["ts", "open", "high", "low", "close", "volume"]


def test_parse_bhavcopy_accepts_live_header() -> None:
    rows = parse_bhavcopy(LIVE_SAMPLE)
    assert len(rows) == 2

    microns = rows[0]
    assert microns.symbol == "20MICRONS"
    assert microns.series == "EQ"
    assert microns.trade_date == date(2026, 11, 6)
    assert microns.close == Decimal("183.22")
    assert microns.average_price == Decimal("183.19")
    assert microns.total_traded_quantity == 82_090
    assert microns.deliverable_qty == 39_593
    assert microns.deliverable_pct == Decimal("48.23")

    be_row = rows[1]
    assert be_row.series == "BE"
    assert be_row.deliverable_qty == 0
    assert be_row.deliverable_pct is None


def test_rows_to_bars_live_header_integration() -> None:
    bars = rows_to_bars(parse_bhavcopy(LIVE_SAMPLE))
    assert bars.columns == ["ts", "open", "high", "low", "close", "volume"]
    assert bars.height == 2
    assert bars["close"].dtype == pl.Decimal
    assert bars["close"].to_list() == [Decimal("183.2200"), Decimal("25.7000")]
