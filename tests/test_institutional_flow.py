"""Tests for FII/DII institutional net-flow ingestion (T5.9 / gap 0013)."""

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from tantra_data.adapters.institutional_flow_store import DuckDbInstitutionalFlowStore
from tantra_data.domain.institutional_flow import InstitutionalFlow
from tantra_data.ports.data_feeds import InstitutionalFlowReader
from tantra_data.ports.repositories import InstitutionalFlowRepository
from tantra_data.providers.institutional_flow import (
    InstitutionalFlowRow,
    parse_institutional_flows,
    rows_to_flows,
)
from tantra_data.service.institutional_flow_reader import PythonInstitutionalFlowReader

SAMPLE = (
    '[{"buyValue": "11745.95", "category": "DII", "date": "13-Nov-2026", '
    '"netValue": "-2253.58", "sellValue": "13999.53"}, '
    '{"buyValue": "18339.57", "category": "FII/FPI", "date": "13-Nov-2026", '
    '"netValue": "5736.55", "sellValue": "12603.02"}]'
)


def test_parse_institutional_flows() -> None:
    rows = parse_institutional_flows(SAMPLE)
    assert len(rows) == 2
    dii, fii = rows
    assert isinstance(dii, InstitutionalFlowRow)
    assert dii.trade_date == date(2026, 11, 13)
    assert dii.participant == "DII"
    assert dii.segment == "cash"
    assert dii.gross_buy == Decimal("11745.95")
    assert dii.gross_sell == Decimal("13999.53")
    assert dii.net == Decimal("-2253.58")
    assert isinstance(fii, InstitutionalFlowRow)
    assert fii.participant == "FII"
    assert fii.trade_date == date(2026, 11, 13)
    assert fii.gross_buy == Decimal("18339.57")
    assert fii.gross_sell == Decimal("12603.02")
    assert fii.net == Decimal("5736.55")


def test_parse_rejects_non_json() -> None:
    with pytest.raises(ValueError, match="not valid JSON"):
        parse_institutional_flows("<html>definitely not json</html>")


def test_parse_rejects_unknown_category() -> None:
    payload = (
        '[{"buyValue": "1", "category": "HNI", "date": "13-Nov-2026", '
        '"netValue": "1", "sellValue": "2"}]'
    )
    with pytest.raises(ValueError, match="Unknown FII/DII category"):
        parse_institutional_flows(payload)


def test_rows_to_flows_builds_domain_flows() -> None:
    flows = rows_to_flows(parse_institutional_flows(SAMPLE))
    assert len(flows) == 2
    dii, fii = flows
    assert isinstance(dii, InstitutionalFlow)
    assert isinstance(fii, InstitutionalFlow)
    assert dii.participant == "DII"
    assert dii.segment == "cash"
    assert dii.source == "nse"
    assert dii.gross_buy == Decimal("11745.95")
    assert fii.participant == "FII"
    assert fii.source == "nse"
    archived = rows_to_flows(parse_institutional_flows(SAMPLE), source="archive")
    assert all(flow.source == "archive" for flow in archived)


def test_store_round_trip_filters_and_idempotent_upsert() -> None:
    store = DuckDbInstitutionalFlowStore()
    flows = rows_to_flows(parse_institutional_flows(SAMPLE))
    for flow in flows:
        store.upsert(flow)
    store.upsert(flows[0])
    assert len(store.query()) == 2
    dii = store.query(participant="DII")
    assert len(dii) == 1
    assert dii[0].gross_buy == Decimal("11745.95")
    assert len(store.query(segment="cash")) == 2
    assert len(store.query(trade_date=date(2026, 11, 13))) == 2
    assert store.query(participant="NOPE") == []
    store.close()


def test_reader_returns_exact_decimal_frame_and_canonical_empty() -> None:
    store = DuckDbInstitutionalFlowStore()
    reader = PythonInstitutionalFlowReader(store)
    try:
        for flow in rows_to_flows(parse_institutional_flows(SAMPLE)):
            store.upsert(flow)
        frame = reader.get_flows()
        assert frame.height == 2
        expected_columns = [
            "trade_date",
            "participant",
            "segment",
            "gross_buy",
            "gross_sell",
            "net",
            "source",
        ]
        assert frame.columns == expected_columns
        assert frame["trade_date"].dtype == pl.Date
        assert frame["participant"].dtype == pl.String
        assert frame["segment"].dtype == pl.String
        assert frame["gross_buy"].dtype == pl.Decimal(18, 4)
        assert frame["gross_sell"].dtype == pl.Decimal(18, 4)
        assert frame["net"].dtype == pl.Decimal(18, 4)
        assert frame["source"].dtype == pl.String
        dii = frame.filter(pl.col("participant") == "DII")
        assert dii["gross_buy"][0] == Decimal("11745.95")
        assert dii["gross_sell"][0] == Decimal("13999.53")
        assert dii["net"][0] == Decimal("-2253.58")
        fii = frame.filter(pl.col("participant") == "FII")
        assert fii["net"][0] == Decimal("5736.55")
        empty = reader.get_flows(participant="NOT_A_PARTICIPANT")
        assert empty.height == 0
        assert empty.columns == expected_columns
        assert empty["trade_date"].dtype == pl.Date
        assert empty["gross_buy"].dtype == pl.Decimal(18, 4)
    finally:
        store.close()


def test_protocol_conformance() -> None:
    store = DuckDbInstitutionalFlowStore()
    try:
        reader = PythonInstitutionalFlowReader(store)
        assert isinstance(store, InstitutionalFlowRepository)
        assert isinstance(reader, InstitutionalFlowReader)
    finally:
        store.close()
