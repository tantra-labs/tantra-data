"""Tests for the DuckDB intraday option-chain store (gap 0016)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import duckdb

from tantra_data.domain.option_chain import OptionChainSnapshot, StrikePositioning
from tantra_data.store.option_chain import DuckDbOptionChainStore

NEAR_EXPIRY = date(2026, 11, 26)
FAR_EXPIRY = date(2026, 12, 31)


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
    expiry: date = NEAR_EXPIRY,
    strikes: list[StrikePositioning] | None = None,
    underlying: str = "NIFTY",
) -> OptionChainSnapshot:
    return OptionChainSnapshot(
        as_of=as_of, underlying=underlying, expiry=expiry, strikes=strikes or []
    )


def test_upsert_snapshot_is_idempotent(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path)
    as_of = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    snapshot = _snapshot(as_of, strikes=[_pos("25000", "CE", "1000"), _pos("25000", "PE", "2000")])
    try:
        store.upsert_snapshot(snapshot)
        store.upsert_snapshot(snapshot)  # same key -> replace, never duplicate
        chain = store.chain_as_of("NIFTY", NEAR_EXPIRY, as_of + timedelta(hours=1))
        assert len(chain) == 2
    finally:
        store.close()
    connection = duckdb.connect(str(tmp_path / "option_chain.duckdb"))
    try:
        row = connection.execute("SELECT COUNT(*) FROM option_chain_snapshots").fetchone()
        assert row is not None
        assert row[0] == 2
    finally:
        connection.close()


def test_chain_as_of_is_point_in_time_no_lookahead(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path)
    t1 = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=5)
    try:
        store.upsert_snapshot(_snapshot(t1, strikes=[_pos("25000", "CE", "1000")]))
        store.upsert_snapshot(_snapshot(t2, strikes=[_pos("25000", "CE", "5000")]))
        # the later snapshot must not leak into an earlier reference_ts
        assert store.chain_as_of("NIFTY", NEAR_EXPIRY, t1)[0].open_interest == Decimal("1000")
        # a reference strictly between snapshots still sees the earlier one
        mid = t1 + timedelta(minutes=2)
        assert store.chain_as_of("NIFTY", NEAR_EXPIRY, mid)[0].open_interest == Decimal("1000")
        # at t2 and beyond the later value is visible
        assert store.chain_as_of("NIFTY", NEAR_EXPIRY, t2)[0].open_interest == Decimal("5000")
        assert store.chain_as_of("NIFTY", NEAR_EXPIRY, t2 + timedelta(hours=1))[
            0
        ].open_interest == Decimal("5000")
    finally:
        store.close()


def test_chain_as_of_carries_forward_dropped_strikes(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path)
    t1 = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=5)
    try:
        store.upsert_snapshot(
            _snapshot(t1, strikes=[_pos("25000", "CE", "1000"), _pos("25100", "CE", "2000")])
        )
        # the 25100 strike is dropped from the later snapshot
        store.upsert_snapshot(_snapshot(t2, strikes=[_pos("25000", "CE", "5000")]))
        chain = store.chain_as_of("NIFTY", NEAR_EXPIRY, t2)
        strikes = {positioning.strike: positioning for positioning in chain}
        assert strikes[Decimal("25000")].open_interest == Decimal("5000")
        assert strikes[Decimal("25100")].open_interest == Decimal("2000")
    finally:
        store.close()


def test_chain_as_of_is_scoped_by_expiry_rollover(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path)
    as_of = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    try:
        store.upsert_snapshot(
            _snapshot(as_of, expiry=NEAR_EXPIRY, strikes=[_pos("25000", "CE", "1000")])
        )
        store.upsert_snapshot(
            _snapshot(as_of, expiry=FAR_EXPIRY, strikes=[_pos("25000", "CE", "9999")])
        )
        near = store.chain_as_of("NIFTY", NEAR_EXPIRY, as_of + timedelta(hours=1))
        far = store.chain_as_of("NIFTY", FAR_EXPIRY, as_of + timedelta(hours=1))
        assert [positioning.open_interest for positioning in near] == [Decimal("1000")]
        assert [positioning.open_interest for positioning in far] == [Decimal("9999")]
    finally:
        store.close()


def test_snapshots_between_filters_and_groups_by_as_of(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path)
    t1 = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=5)
    try:
        store.upsert_snapshot(_snapshot(t1, strikes=[_pos("25000", "CE", "1000")]))
        store.upsert_snapshot(
            _snapshot(t2, strikes=[_pos("25000", "CE", "1100"), _pos("25100", "PE", "500")])
        )
        all_snapshots = store.snapshots_between()
        assert len(all_snapshots) == 2
        assert {snapshot.as_of for snapshot in all_snapshots} == {t1, t2}
        assert len(store.snapshots_between(expiry=NEAR_EXPIRY)) == 2
        assert store.snapshots_between(expiry=FAR_EXPIRY) == []
        windowed = store.snapshots_between(start=t2)
        assert [snapshot.as_of for snapshot in windowed] == [t2]
        scoped = store.snapshots_between(underlying="BANKNIFTY")
        assert scoped == []
    finally:
        store.close()


def test_gamma_exposure_as_of_returns_none_without_data(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path)
    try:
        exposure = store.gamma_exposure_as_of(
            "NIFTY",
            NEAR_EXPIRY,
            datetime(2026, 11, 16, 4, 45, tzinfo=UTC),
            Decimal("75"),
            Decimal("25000"),
        )
        assert exposure is None
    finally:
        store.close()


def test_gamma_exposure_as_of_matches_domain_math(tmp_path) -> None:
    store = DuckDbOptionChainStore(tmp_path)
    t1 = datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    try:
        store.upsert_snapshot(
            _snapshot(
                t1,
                strikes=[
                    _pos("24000", "CE", "1000", gamma="-0.000100"),
                    _pos("24500", "CE", "1000", gamma="0.000200"),
                    _pos("25000", "CE", "1000", gamma="0.000100"),
                ],
            )
        )
        exposure = store.gamma_exposure_as_of(
            "NIFTY", NEAR_EXPIRY, t1, Decimal("75"), Decimal("25000")
        )
        assert exposure is not None
        assert exposure.net == Decimal("375000")
        assert exposure.flip == Decimal("24500")
    finally:
        store.close()
