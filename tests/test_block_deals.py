"""NSE block/bulk deal disclosure provider, store, and reader."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl

from tantra_data.adapters.block_deals_store import DuckDbBlockDealsStore
from tantra_data.domain.block_deals import BlockDeal
from tantra_data.ports.data_feeds import BlockDealsReader
from tantra_data.ports.repositories import BlockDealsRepository
from tantra_data.providers.block_deals import (
    parse_block_deals,
    parse_bulk_deals,
    rows_to_deals,
)
from tantra_data.service.block_deals_reader import PythonBlockDealsReader

BLOCK_SAMPLE = (
    "Date,Symbol,Security Name,Client Name,Buy/Sell,Quantity Traded,"
    "Trade Price / Wght. Avg. Price\n"
    "13-NOV-2026,RELIANCE,RELIANCE INDUSTRIES LTD,HDFC MUTUAL FUND,BUY,250000,1250.00\n"
    "13-NOV-2026,TATAMOTORS,TATA MOTORS LTD,ICICI PRUDENTIAL LIFE INSURANCE,SELL,400000,780.50\n"
)

BULK_SAMPLE = (
    "Date,Symbol,Security Name,Client Name,Buy/Sell,Quantity Traded,"
    "Trade Price / Wght. Avg. Price,Remarks\n"
    "13-NOV-2026,HDFCBANK,HDFC BANK LTD,ADITYA BIRLA SUN LIFE AMC,BUY,5000000,1675.25,Bulk deal\n"
    "13-NOV-2026,INFY,INFOSYS LTD,SBI MUTUAL FUND,SELL,3000000,1440.10,\n"
)


def test_parse_block_deals_parses_exact_decimals() -> None:
    rows = parse_block_deals(BLOCK_SAMPLE)
    assert len(rows) == 2

    reliance = rows[0]
    assert reliance.trade_date == date(2026, 11, 13)
    assert reliance.symbol == "RELIANCE"
    assert reliance.security_name == "RELIANCE INDUSTRIES LTD"
    assert reliance.client_name == "HDFC MUTUAL FUND"
    assert reliance.side == "BUY"
    assert reliance.quantity == 250_000
    assert reliance.price == Decimal("1250.00")
    assert reliance.remarks is None

    tata = rows[1]
    assert tata.side == "SELL"
    assert tata.quantity == 400_000
    assert tata.price == Decimal("780.50")


def test_parse_bulk_deals_parses_remarks() -> None:
    rows = parse_bulk_deals(BULK_SAMPLE)
    assert len(rows) == 2

    hdfc = rows[0]
    assert hdfc.symbol == "HDFCBANK"
    assert hdfc.quantity == 5_000_000
    assert hdfc.price == Decimal("1675.25")
    assert hdfc.remarks == "Bulk deal"

    infy = rows[1]
    assert infy.side == "SELL"
    assert infy.remarks is None


def test_rows_to_deals_maps_to_canonical_instruments() -> None:
    block = rows_to_deals(parse_block_deals(BLOCK_SAMPLE), category="block")
    assert len(block) == 2
    assert all(isinstance(deal, BlockDeal) for deal in block)

    first = block[0]
    assert first.exchange == "NSE"
    assert first.instrument == "NSE:RELIANCE-EQ"
    assert first.category == "block"
    assert first.side == "BUY"
    assert first.quantity == Decimal("250000")
    assert first.price == Decimal("1250.00")
    assert first.value == Decimal("312500000.00")
    assert first.remarks is None

    second = block[1]
    assert second.instrument == "NSE:TATAMOTORS-EQ"
    assert second.side == "SELL"

    bulk = rows_to_deals(parse_bulk_deals(BULK_SAMPLE), category="bulk")
    assert bulk[0].instrument == "NSE:HDFCBANK-EQ"
    assert bulk[0].category == "bulk"
    assert bulk[0].remarks == "Bulk deal"
    assert bulk[1].remarks is None


def test_store_upsert_query_roundtrip() -> None:
    store = DuckDbBlockDealsStore()
    try:
        block = rows_to_deals(parse_block_deals(BLOCK_SAMPLE), category="block")
        bulk = rows_to_deals(parse_bulk_deals(BULK_SAMPLE), category="bulk")
        for deal in block + bulk:
            store.upsert(deal)

        # Idempotent re-upsert on the same key replaces, never duplicates.
        store.upsert(block[0])
        assert len(store.query()) == 4

        # Date filter.
        dated = store.query(trade_date=date(2026, 11, 13))
        assert len(dated) == 4

        # Instrument filter preserves exact Decimals.
        reliance = store.query(instrument="NSE:RELIANCE-EQ")
        assert len(reliance) == 1
        assert reliance[0].trade_date == date(2026, 11, 13)
        assert reliance[0].exchange == "NSE"
        assert reliance[0].category == "block"
        assert reliance[0].quantity == Decimal("250000")
        assert reliance[0].price == Decimal("1250.00")
        assert reliance[0].remarks is None

        # Category filter.
        bulk_only = store.query(category="bulk")
        assert len(bulk_only) == 2
        assert all(deal.category == "bulk" for deal in bulk_only)

        # No-match query.
        assert store.query(trade_date=date(2020, 1, 1)) == []
    finally:
        store.close()


def test_reader_list_deals_returns_exact_decimal_frame() -> None:
    store = DuckDbBlockDealsStore()
    try:
        for deal in rows_to_deals(parse_block_deals(BLOCK_SAMPLE), category="block"):
            store.upsert(deal)
        reader = PythonBlockDealsReader(store)

        expected_columns = [
            "trade_date",
            "exchange",
            "instrument",
            "security_name",
            "client_name",
            "side",
            "category",
            "quantity",
            "price",
            "remarks",
        ]
        frame = reader.list_deals()
        assert frame.columns == expected_columns
        assert frame.height == 2
        assert frame["quantity"].dtype == pl.Decimal
        assert frame["price"].dtype == pl.Decimal
        assert frame["trade_date"].to_list() == [date(2026, 11, 13), date(2026, 11, 13)]
        assert frame["quantity"].to_list() == [Decimal("250000.0000"), Decimal("400000.0000")]
        assert frame["price"].to_list() == [Decimal("1250.0000"), Decimal("780.5000")]

        empty = reader.list_deals(trade_date=date(2020, 1, 1))
        assert empty.is_empty()
        assert empty.columns == expected_columns
        assert empty["quantity"].dtype == pl.Decimal
        assert empty["price"].dtype == pl.Decimal
    finally:
        store.close()


def test_protocol_checks() -> None:
    store = DuckDbBlockDealsStore()
    try:
        reader = PythonBlockDealsReader(store)
        assert isinstance(store, BlockDealsRepository)
        assert isinstance(reader, BlockDealsReader)
    finally:
        store.close()
