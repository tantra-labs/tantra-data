"""Tests for F&O spot-futures basis and option bid-ask spread domain & store."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import duckdb
import pytest

from tantra_data.domain.fno_spreads import FuturesBasisSnapshot, OptionSpreadSnapshot
from tantra_data.store.fno_spreads import DuckDbFnoSpreadStore

NEAR_EXPIRY = date(2026, 11, 26)
FAR_EXPIRY = date(2026, 12, 31)


def test_futures_basis_exact_decimal_math_and_float_rejection() -> None:
    as_of = datetime(2026, 11, 18, 9, 30, tzinfo=UTC)
    snap = FuturesBasisSnapshot(
        as_of=as_of,
        underlying="nifty",
        spot_price=Decimal("24000.0000"),
        near_expiry=NEAR_EXPIRY,
        near_futures_price=Decimal("24072.0000"),
        far_expiry=FAR_EXPIRY,
        far_futures_price=Decimal("24192.3600"),
    )
    assert snap.underlying == "NIFTY"
    assert isinstance(snap.spot_basis_abs, Decimal)
    assert snap.spot_basis_abs == Decimal("72.0000")
    assert snap.spot_basis_bps == Decimal("30.0000")
    assert snap.calendar_spread_abs == Decimal("120.3600")
    assert snap.calendar_spread_bps == Decimal("50.0000")

    with pytest.raises(TypeError, match="never float"):
        FuturesBasisSnapshot(
            as_of=as_of,
            underlying="NIFTY",
            spot_price=24000.0,  # type: ignore[arg-type]
            near_expiry=NEAR_EXPIRY,
            near_futures_price=Decimal("24050"),
        )


def test_option_spread_validation_and_bps() -> None:
    as_of = datetime(2026, 11, 18, 9, 30, tzinfo=UTC)
    snap = OptionSpreadSnapshot(
        as_of=as_of,
        underlying="banknifty",
        expiry=NEAR_EXPIRY,
        strike=Decimal("52000"),
        option_type="ce",
        bid=Decimal("199.5000"),
        ask=Decimal("200.5000"),
        implied_volatility=Decimal("14.2500"),
        open_interest=Decimal("250000"),
    )
    assert snap.underlying == "BANKNIFTY"
    assert snap.option_type == "CE"
    assert snap.mid == Decimal("200.0000")
    assert snap.spread_abs == Decimal("1.0000")
    assert snap.spread_bps == Decimal("50.0000")

    with pytest.raises(ValueError, match="ask must be >= bid"):
        OptionSpreadSnapshot(
            as_of=as_of,
            underlying="BANKNIFTY",
            expiry=NEAR_EXPIRY,
            strike=Decimal("52000"),
            option_type="PE",
            bid=Decimal("210"),
            ask=Decimal("200"),
        )


def test_duckdb_fno_spread_store_idempotent_and_point_in_time(tmp_path) -> None:
    store = DuckDbFnoSpreadStore(tmp_path)
    t1 = datetime(2026, 11, 18, 4, 0, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=15)

    try:
        b1 = FuturesBasisSnapshot(
            as_of=t1,
            underlying="NIFTY",
            spot_price=Decimal("24000"),
            near_expiry=NEAR_EXPIRY,
            near_futures_price=Decimal("24048"),
            far_expiry=FAR_EXPIRY,
            far_futures_price=Decimal("24120"),
        )
        b2 = FuturesBasisSnapshot(
            as_of=t2,
            underlying="NIFTY",
            spot_price=Decimal("24050"),
            near_expiry=NEAR_EXPIRY,
            near_futures_price=Decimal("24110"),
            far_expiry=FAR_EXPIRY,
            far_futures_price=Decimal("24190"),
        )
        store.upsert_futures_basis(b1)
        store.upsert_futures_basis(b1)  # idempotent
        store.upsert_futures_basis(b2)

        # No look-ahead at t1 or t1 + 5m
        at_t1 = store.futures_basis_as_of("NIFTY", NEAR_EXPIRY, t1 + timedelta(minutes=5))
        assert at_t1 is not None
        assert at_t1.spot_basis_bps == Decimal("20.0000")

        # At t2 sees b2
        at_t2 = store.futures_basis_as_of("NIFTY", NEAR_EXPIRY, t2)
        assert at_t2 is not None
        assert at_t2.near_futures_price == Decimal("24110.0000")

        series = store.futures_basis_series("NIFTY", near_expiry=NEAR_EXPIRY)
        assert [s.as_of for s in series] == [t1, t2]

        # Option spreads + exact median bps
        store.upsert_option_spreads(
            [
                OptionSpreadSnapshot(
                    as_of=t1,
                    underlying="NIFTY",
                    expiry=NEAR_EXPIRY,
                    strike=Decimal("24000"),
                    option_type="CE",
                    bid=Decimal("99.5"),
                    ask=Decimal("100.5"),
                ),
                OptionSpreadSnapshot(
                    as_of=t1,
                    underlying="NIFTY",
                    expiry=NEAR_EXPIRY,
                    strike=Decimal("24100"),
                    option_type="CE",
                    bid=Decimal("49.0"),
                    ask=Decimal("51.0"),
                ),
                OptionSpreadSnapshot(
                    as_of=t2,
                    underlying="NIFTY",
                    expiry=NEAR_EXPIRY,
                    strike=Decimal("24000"),
                    option_type="CE",
                    bid=Decimal("100.0"),
                    ask=Decimal("104.0"),
                ),
            ]
        )
        # Re-upserting t1 strike preserves idempotency
        store.upsert_option_spreads(
            [
                OptionSpreadSnapshot(
                    as_of=t1,
                    underlying="NIFTY",
                    expiry=NEAR_EXPIRY,
                    strike=Decimal("24000"),
                    option_type="CE",
                    bid=Decimal("99.5"),
                    ask=Decimal("100.5"),
                )
            ]
        )

        # Point-in-time before t2 sees 100 bps (24000 CE) and 400 bps (24100 CE) -> median 250 bps
        med_t1 = store.median_spread_bps_as_of(
            "NIFTY", NEAR_EXPIRY, t1 + timedelta(minutes=1), option_type="CE"
        )
        assert med_t1 == Decimal("250.0000")
    finally:
        store.close()

    conn = duckdb.connect(str(tmp_path / "fno_spreads.duckdb"))
    try:
        fut_count = conn.execute("SELECT COUNT(*) FROM futures_basis_snapshots").fetchone()[0]
        opt_count = conn.execute("SELECT COUNT(*) FROM option_spread_snapshots").fetchone()[0]
        assert fut_count == 2
        assert opt_count == 3
    finally:
        conn.close()
