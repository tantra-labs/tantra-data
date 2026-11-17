"""Gamma-exposure (GEX) golden-vector tests (gap 0016).

Per-strike GEX is ``gamma * open_interest * lot_size * spot`` computed with
exact ``Decimal``; ``net`` is the exact sum; ``flip`` is the strike where the
cumulative GEX (walking strikes ascending) changes sign. All vectors are
hand-computed with ``lot_size = 75`` and ``spot = 25000``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from tantra_data.domain.option_chain import (
    GammaExposure,
    OptionChainSnapshot,
    StrikeGamma,
    StrikePositioning,
    compute_gamma_exposure,
)

LOT_SIZE = Decimal("75")
SPOT = Decimal("25000")


def _pos(strike: str, option_type: str, oi: str, gamma: str) -> StrikePositioning:
    return StrikePositioning(
        strike=Decimal(strike),
        option_type=option_type,
        open_interest=Decimal(oi),
        oi_change=Decimal("0"),
        implied_volatility=Decimal("15.00"),
        delta=Decimal("0.50"),
        gamma=Decimal(gamma),
    )


def _snapshot(strikes: list[StrikePositioning]) -> OptionChainSnapshot:
    return OptionChainSnapshot(
        as_of=datetime(2026, 11, 16, 4, 45, tzinfo=UTC),
        underlying="NIFTY",
        expiry=date(2026, 11, 26),
        strikes=strikes,
    )


def test_single_strike_gex_is_gamma_times_oi_times_lot_times_spot() -> None:
    exposure = compute_gamma_exposure(
        _snapshot([_pos("25000", "CE", "1000", "0.000100")]), LOT_SIZE, SPOT
    )
    assert exposure.per_strike == [
        StrikeGamma(
            strike=Decimal("25000"),
            gamma=Decimal("0.000100"),
            open_interest=Decimal("1000"),
            gex=Decimal("187500"),
        )
    ]
    assert exposure.net == Decimal("187500")
    assert exposure.flip is None


def test_gamma_exposure_golden_vector_with_flip() -> None:
    strikes = [
        _pos("24000", "CE", "1000", "-0.000100"),
        _pos("24500", "CE", "1000", "0.000200"),
        _pos("25000", "CE", "1000", "0.000100"),
    ]
    exposure = compute_gamma_exposure(_snapshot(strikes), LOT_SIZE, SPOT)
    # -0.000100*1000*75*25000 = -187500; 0.000200*1000*75*25000 = 375000;
    # 0.000100*1000*75*25000 = 187500
    assert [(per.strike, per.gex) for per in exposure.per_strike] == [
        (Decimal("24000"), Decimal("-187500")),
        (Decimal("24500"), Decimal("375000")),
        (Decimal("25000"), Decimal("187500")),
    ]
    # cumulative: -187500 -> +187500 changes sign at 24500
    assert exposure.net == Decimal("375000")
    assert exposure.flip == Decimal("24500")


def test_gamma_exposure_flip_is_none_when_cumulative_never_crosses_zero() -> None:
    strikes = [
        _pos("25000", "CE", "1000", "0.000100"),
        _pos("25500", "CE", "2000", "0.000100"),
        _pos("26000", "CE", "1000", "0.000100"),
    ]
    exposure = compute_gamma_exposure(_snapshot(strikes), LOT_SIZE, SPOT)
    # 187500 + 375000 + 187500 = 750000; cumulative stays positive
    assert exposure.net == Decimal("750000")
    assert exposure.flip is None


def test_gamma_exposure_is_exact_decimal_never_float() -> None:
    exposure = compute_gamma_exposure(
        _snapshot(
            [
                _pos("25000", "CE", "3", "0.333333"),
                _pos("25500", "PE", "7", "0.111111"),
            ]
        ),
        Decimal("75"),
        Decimal("25000"),
    )
    assert isinstance(exposure, GammaExposure)
    assert all(isinstance(per.gex, Decimal) for per in exposure.per_strike)
    assert isinstance(exposure.net, Decimal)
    # 0.333333*3*75*25000 = 1874998.125; 0.111111*7*75*25000 = 1458331.875
    assert exposure.per_strike[0].gex == Decimal("1874998.125")
    assert exposure.per_strike[1].gex == Decimal("1458331.875")
    assert exposure.net == Decimal("3333330.000")


def test_compute_gamma_exposure_rejects_non_positive_lot_or_spot() -> None:
    snapshot = _snapshot([_pos("25000", "CE", "1000", "0.000100")])
    try:
        compute_gamma_exposure(snapshot, Decimal("0"), SPOT)
    except ValueError:
        pass
    else:
        raise AssertionError("lot_size of zero must be rejected")
    try:
        compute_gamma_exposure(snapshot, LOT_SIZE, Decimal("-1"))
    except ValueError:
        pass
    else:
        raise AssertionError("non-positive spot must be rejected")
