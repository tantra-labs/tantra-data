"""Tests for the option-chain Parquet bridge export (gap 0016).

Covers the per-snapshot ``option_chain/{expiry}/YYYY-MM-DD-HHMM.parquet``
files, the ``chain_as_of.parquet`` point-in-time projection (round-trips
against the DuckDB ``chain_as_of`` read) and the derived GEX series file —
all with exact ``Decimal128(18, 4)`` columns and no floats.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.domain.option_chain import OptionChainSnapshot, StrikePositioning
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter
from tantra_data.store.option_chain import DuckDbOptionChainStore

EXPIRY = date(2026, 11, 26)


def _pos(
    strike: str,
    option_type: str,
    oi: str,
    gamma: str = "0.000100",
    oi_change: str = "10",
) -> StrikePositioning:
    return StrikePositioning(
        strike=Decimal(strike),
        option_type=option_type,
        open_interest=Decimal(oi),
        oi_change=Decimal(oi_change),
        implied_volatility=Decimal("15.00"),
        delta=Decimal("0.50"),
        gamma=Decimal(gamma),
    )


def _snapshot(
    as_of: datetime,
    strikes: list[StrikePositioning] | None = None,
    expiry: date = EXPIRY,
) -> OptionChainSnapshot:
    return OptionChainSnapshot(
        as_of=as_of, underlying="NIFTY", expiry=expiry, strikes=strikes or []
    )


def _exporter(store: DuckDbOptionChainStore) -> LakehouseBridgeExporter:
    return LakehouseBridgeExporter(None, None, option_chain=store)


def _chain_from_projection(
    rows: list[dict[str, Any]],
    reference_ts: datetime,
) -> list[dict[str, Any]]:
    """Replicate ``chain_as_of`` semantics over the projection file rows."""
    latest: dict[tuple[Decimal, str], dict[str, Any]] = {}
    for row in rows:
        as_of: datetime = row["as_of"]
        if as_of > reference_ts:
            continue
        key = (Decimal(str(row["strike"])), str(row["option_type"]))
        if key not in latest or as_of > latest[key]["as_of"]:
            latest[key] = row
    return sorted(latest.values(), key=lambda row: (row["strike"], row["option_type"]))


def test_export_option_chain_writes_snapshot_files(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path / "db")
    t1 = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=5)
    store.upsert_snapshot(_snapshot(t1, strikes=[_pos("25000", "CE", "1000")]))
    store.upsert_snapshot(
        _snapshot(t2, strikes=[_pos("25000", "CE", "1100"), _pos("25100", "PE", "500")])
    )
    exporter = _exporter(store)
    try:
        assert exporter.export_option_chain(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted(
        (tmp_path / "catalog" / "option_chain" / "2026-11-26").glob("2026-11-16-*.parquet")
    )
    assert [file.name for file in files] == ["2026-11-16-0445.parquet", "2026-11-16-0450.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == [
        "as_of",
        "underlying",
        "expiry",
        "strike",
        "option_type",
        "open_interest",
        "oi_change",
        "implied_volatility",
        "delta",
        "gamma",
    ]
    assert table.schema.field("as_of").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("expiry").type == pa.date32()
    for column in ("open_interest", "oi_change", "implied_volatility", "delta", "gamma"):
        assert table.schema.field(column).type == pa.decimal128(18, 4)
    data = table.to_pydict()
    assert data["underlying"] == ["NIFTY"]
    assert data["strike"] == [Decimal("25000.0000")]
    assert data["option_type"] == ["CE"]


def test_export_option_chain_projection_round_trips_duckdb(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path / "db")
    t1 = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=5)
    store.upsert_snapshot(_snapshot(t1, strikes=[_pos("25000", "CE", "1000")]))
    store.upsert_snapshot(
        _snapshot(t2, strikes=[_pos("25000", "CE", "1100"), _pos("25100", "PE", "500")])
    )
    exporter = _exporter(store)
    try:
        exporter.export_option_chain(tmp_path / "catalog")
    finally:
        store.close()

    projection = pq.read_table(
        tmp_path / "catalog" / "option_chain" / "2026-11-26" / "chain_as_of.parquet"
    ).to_pylist()

    store2 = DuckDbOptionChainStore(tmp_path / "db")
    try:
        # at t1 the projection equals the t1 chain (no look-ahead from t2)
        at_t1 = _chain_from_projection(projection, t1)
        duck_t1 = store2.chain_as_of("NIFTY", EXPIRY, t1)
        assert _rows_equal(at_t1, duck_t1)
        # at t2 both the 25000 update and the carried-forward 25100 are visible
        at_t2 = _chain_from_projection(projection, t2)
        duck_t2 = store2.chain_as_of("NIFTY", EXPIRY, t2)
        assert _rows_equal(at_t2, duck_t2)
        assert {row["strike"] for row in at_t2} == {Decimal("25000"), Decimal("25100")}
    finally:
        store2.close()


def test_export_option_chain_gex_writes_decimal_series(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path / "db")
    as_of = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    store.upsert_snapshot(
        _snapshot(
            as_of,
            strikes=[
                _pos("24000", "CE", "1000", gamma="-0.000100"),
                _pos("24500", "CE", "1000", gamma="0.000200"),
                _pos("25000", "CE", "1000", gamma="0.000100"),
            ],
        )
    )
    exporter = _exporter(store)
    try:
        assert (
            exporter.export_option_chain_gex(tmp_path / "catalog", Decimal("75"), Decimal("25000"))
            == 1
        )
    finally:
        store.close()

    gex_path = tmp_path / "catalog" / "option_chain" / "2026-11-26" / "gex_series.parquet"
    assert gex_path.exists()
    table = pq.read_table(gex_path)
    assert table.column_names == [
        "as_of",
        "expiry",
        "strike",
        "gamma",
        "open_interest",
        "gex",
        "net",
        "flip",
    ]
    assert table.schema.field("as_of").type == pa.timestamp("us", tz="UTC")
    for column in ("strike", "gamma", "open_interest", "gex", "net", "flip"):
        assert table.schema.field(column).type == pa.decimal128(18, 4)
    assert sorted(table.column("gex").to_pylist()) == [
        Decimal("-187500.0000"),
        Decimal("187500.0000"),
        Decimal("375000.0000"),
    ]
    assert table.column("net").to_pylist() == [Decimal("375000.0000")] * 3
    assert table.column("flip").to_pylist() == [Decimal("24500.0000")] * 3


def test_export_option_chain_missing_store_writes_nothing(tmp_path) -> None:
    exporter = LakehouseBridgeExporter(None, None)
    assert exporter.export_option_chain(tmp_path / "catalog") == 0
    assert (
        exporter.export_option_chain_gex(tmp_path / "catalog", Decimal("75"), Decimal("25000")) == 0
    )
    assert not (tmp_path / "catalog" / "option_chain").exists()


def _rows_equal(rows: list[dict[str, object]], strikes: list[StrikePositioning]) -> bool:
    projected = [(row["strike"], row["option_type"], row["open_interest"]) for row in rows]
    duck = [
        (positioning.strike, positioning.option_type, positioning.open_interest)
        for positioning in strikes
    ]
    return projected == duck
