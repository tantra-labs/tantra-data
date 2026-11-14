"""Tests for NSE delivery parsing, DuckDB storage, and Polars reading."""

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from tantra_data.adapters.delivery_store import DuckDbDeliveryStore
from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.ports.data_feeds import DeliveryReader
from tantra_data.ports.repositories import DeliveryRepository
from tantra_data.providers.delivery import (
    DeliveryRow,
    parse_delivery,
    rows_to_records,
)
from tantra_data.service.delivery_reader import PythonDeliveryReader

LIVE_SAMPLE = (
    "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, "
    "LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, "
    "NO_OF_TRADES, DELIV_QTY, DELIV_PER\n"
    "20MICRONS, EQ, 06-Nov-2026, 180.26, 181.00, 185.81, 177.00, 183.00, "
    "183.22, 183.19, 82090, 150.38, 2891, 39593, 48.23\n"
    "21STCENMGM, BE, 06-Nov-2026, 57.54, 54.67, 54.67, 54.67, 54.67, 54.67, "
    "54.67, 2663, 1.46, 22, -, -\n"
)

LEGACY_SAMPLE = (
    "SYMBOL,SERIES,DATE,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,"
    "LAST_PRICE,CLOSE_PRICE,AVG_PRICE,TOTAL_TRADED_QUANTITY,TURNOVER_LACS,"
    "NO_OF_TRADES,DELIVERABLE_QTY,% DLVY TO QTY\n"
    "20MICRONS,EQ,06-Nov-2026,180.26,181.00,185.81,177.00,183.00,"
    "183.22,183.19,82090,150.38,2891,39593,48.23\n"
)


def test_parse_delivery_parses_live_header() -> None:
    rows = parse_delivery(LIVE_SAMPLE)

    assert len(rows) == 2

    populated = rows[0]
    assert populated.trade_date == date(2026, 11, 6)
    assert populated.instrument == "NSE:20MICRONS-EQ"
    assert populated.traded_quantity == 82090
    assert populated.deliverable_quantity == 39593
    assert populated.delivery_pct == Decimal("48.23")

    no_delivery = rows[1]
    assert no_delivery.trade_date == date(2026, 11, 6)
    assert no_delivery.instrument == "NSE:21STCENMGM-EQ"
    assert no_delivery.traded_quantity == 2663
    assert no_delivery.deliverable_quantity is None
    assert no_delivery.delivery_pct is None


def test_parse_delivery_parses_legacy_header() -> None:
    rows = parse_delivery(LEGACY_SAMPLE)

    assert len(rows) == 1
    row = rows[0]
    assert row.trade_date == date(2026, 11, 6)
    assert row.instrument == "NSE:20MICRONS-EQ"
    assert row.traded_quantity == 82090
    assert row.deliverable_quantity == 39593
    assert row.delivery_pct == Decimal("48.23")


def test_parse_delivery_rejects_missing_header() -> None:
    with pytest.raises(ValueError, match="delivery data is empty or missing a header"):
        parse_delivery("")


def test_rows_to_records_builds_canonical_records() -> None:
    records = rows_to_records(parse_delivery(LIVE_SAMPLE))

    populated = records[0]
    assert populated.instrument == "NSE:20MICRONS-EQ"
    assert populated.exchange == "NSE"
    assert populated.traded_quantity == 82090
    assert populated.deliverable_quantity == 39593
    assert populated.delivery_pct == Decimal("48.23")

    no_delivery = records[1]
    assert no_delivery.deliverable_quantity == 0
    assert no_delivery.delivery_pct == Decimal(0)


def test_rows_to_records_computes_pct_when_missing() -> None:
    rows = [
        DeliveryRow(
            trade_date=date(2026, 11, 6),
            instrument="NSE:SAMPLE-EQ",
            traded_quantity=200,
            deliverable_quantity=50,
            delivery_pct=None,
        )
    ]
    records = rows_to_records(rows)

    assert records[0].delivery_pct == Decimal("25")
    assert records[0].delivery_ratio == Decimal("0.25")


def test_duck_db_delivery_store_upsert_and_query_round_trip() -> None:
    store = DuckDbDeliveryStore()
    try:
        record = DeliveryRecord(
            trade_date=date(2026, 11, 6),
            instrument="NSE:20MICRONS-EQ",
            traded_quantity=82090,
            deliverable_quantity=39593,
            delivery_pct=Decimal("48.2300"),
        )
        store.upsert(record)
        store.upsert(record)

        assert store.query() == [record]

        other = DeliveryRecord(
            trade_date=date(2026, 11, 6),
            instrument="NSE:21STCENMGM-EQ",
            traded_quantity=2663,
            deliverable_quantity=0,
            delivery_pct=Decimal("0"),
        )
        store.upsert(other)
        assert len(store.query()) == 2
        assert store.query(instrument="NSE:20MICRONS-EQ") == [record]
        assert store.query(trade_date=date(2026, 11, 6), instrument="NSE:21STCENMGM-EQ") == [other]
    finally:
        store.close()


def test_python_delivery_reader_returns_canonical_frame() -> None:
    store = DuckDbDeliveryStore()
    try:
        store.upsert(
            DeliveryRecord(
                trade_date=date(2026, 11, 6),
                instrument="NSE:20MICRONS-EQ",
                traded_quantity=82090,
                deliverable_quantity=39593,
                delivery_pct=Decimal("48.2300"),
            )
        )
        reader = PythonDeliveryReader(store)
        frame = reader.get_delivery()

        assert frame.schema["trade_date"] == pl.Date
        assert frame.schema["instrument"] == pl.String
        assert frame.schema["exchange"] == pl.String
        assert frame.schema["traded_quantity"] == pl.Int64
        assert frame.schema["deliverable_quantity"] == pl.Int64
        assert frame.schema["delivery_pct"] == pl.Decimal(18, 4)
        assert frame["trade_date"][0] == date(2026, 11, 6)
        assert frame["instrument"][0] == "NSE:20MICRONS-EQ"
        assert frame["delivery_pct"][0] == Decimal("48.2300")
    finally:
        store.close()


def test_python_delivery_reader_returns_empty_canonical_frame() -> None:
    store = DuckDbDeliveryStore()
    try:
        reader = PythonDeliveryReader(store)
        frame = reader.get_delivery(instrument="NSE:NOPE-EQ")

        assert frame.is_empty()
        assert frame.columns == [
            "trade_date",
            "instrument",
            "exchange",
            "traded_quantity",
            "deliverable_quantity",
            "delivery_pct",
        ]
        assert frame.schema["delivery_pct"] == pl.Decimal(18, 4)
    finally:
        store.close()


def test_protocol_conformance() -> None:
    store = DuckDbDeliveryStore()
    try:
        reader = PythonDeliveryReader(store)
        assert isinstance(store, DeliveryRepository)
        assert isinstance(reader, DeliveryReader)
    finally:
        store.close()
