"""Ingestion tests for the NSE corporate-actions provider (gap 0009)."""

from datetime import date
from decimal import Decimal

import pytest

from tantra_data.adapters.duckdb_store import DuckDbCorporateActionsStore
from tantra_data.domain.corporate_actions import Bonus, Dividend, Rights, Split
from tantra_data.providers.corporate_actions import (
    CorporateActionRow,
    parse_corporate_actions,
    rows_to_actions,
    subject_to_action,
)

CANNED_CORPORATE_ACTIONS_JSON = """[
  {
    "bcEndDate": "-",
    "bcStartDate": "-",
    "caBroadcastDate": null,
    "comp": "Colgate Palmolive (India) Limited",
    "exDate": "02-Nov-2026",
    "faceVal": "1",
    "ind": "-",
    "isin": "INE259A01022",
    "ndEndDate": "-",
    "ndStartDate": "-",
    "recDate": "02-Nov-2026",
    "series": "EQ",
    "subject": "Interim Dividend - Rs 24 Per Share",
    "symbol": "COLPAL"
  },
  {
    "bcEndDate": "-",
    "bcStartDate": "-",
    "caBroadcastDate": null,
    "comp": "Tata Motors Limited",
    "exDate": "04-Nov-2026",
    "faceVal": "2",
    "ind": "-",
    "isin": "INE155A01022",
    "ndEndDate": "-",
    "ndStartDate": "-",
    "recDate": "05-Nov-2026",
    "series": "EQ",
    "subject": "Bonus 1:1",
    "symbol": "TATAMOTORS"
  },
  {
    "bcEndDate": "-",
    "bcStartDate": "-",
    "caBroadcastDate": null,
    "comp": "Reliance Industries Limited",
    "exDate": "06-Nov-2026",
    "faceVal": "10",
    "ind": "-",
    "isin": "INE002A01018",
    "ndEndDate": "-",
    "ndStartDate": "-",
    "recDate": "07-Nov-2026",
    "series": "EQ",
    "subject": "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share",
    "symbol": "RELIANCE"
  }
]
"""


def _row(
    subject: str,
    symbol: str = "COLPAL",
    face_value: str | None = None,
) -> CorporateActionRow:
    return CorporateActionRow(
        symbol=symbol,
        company="Company Limited",
        ex_date=date(2026, 11, 2),
        record_date=date(2026, 11, 2),
        face_value=Decimal(face_value) if face_value is not None else None,
        subject=subject,
        isin="INE259A01022",
    )


def test_parse_corporate_actions_parses_verified_shape() -> None:
    rows = parse_corporate_actions(CANNED_CORPORATE_ACTIONS_JSON)
    assert len(rows) == 3

    colpal = rows[0]
    assert colpal.symbol == "COLPAL"
    assert colpal.company == "Colgate Palmolive (India) Limited"
    assert colpal.ex_date == date(2026, 11, 2)
    assert colpal.record_date == date(2026, 11, 2)
    assert colpal.face_value == Decimal("1")
    assert colpal.subject == "Interim Dividend - Rs 24 Per Share"
    assert colpal.isin == "INE259A01022"

    assert rows[1].symbol == "TATAMOTORS"
    assert rows[1].ex_date == date(2026, 11, 4)
    assert rows[1].face_value == Decimal("2")
    assert rows[1].subject == "Bonus 1:1"

    assert rows[2].symbol == "RELIANCE"
    assert rows[2].ex_date == date(2026, 11, 6)
    assert rows[2].face_value == Decimal("10")


def test_subject_to_action_maps_observed_patterns() -> None:
    ex_date = date(2026, 11, 2)
    assert subject_to_action(_row("Bonus 1:1")) == Bonus(
        "NSE:COLPAL-EQ", ex_date, 1, 2
    )
    assert subject_to_action(_row("Bonus 3:2")) == Bonus(
        "NSE:COLPAL-EQ", ex_date, 3, 5
    )
    split_subject = (
        "Face Value Split (Sub-Division) - From Rs 10/- Per Share "
        "To Re 1/- Per Share"
    )
    assert subject_to_action(_row(split_subject)) == Split(
        "NSE:COLPAL-EQ", ex_date, 1, 10
    )
    assert subject_to_action(
        _row("Face Value Split (Sub-Division) - From Rs 5/- Per Share To Re 1/- Per Share")
    ) == Split("NSE:COLPAL-EQ", ex_date, 1, 5)
    assert subject_to_action(
        _row("Interim Dividend - Rs 24 Per Share")
    ) == Dividend("NSE:COLPAL-EQ", ex_date, Decimal("24"))
    assert subject_to_action(
        _row("Final Dividend - Rs 5 Per Share")
    ) == Dividend("NSE:COLPAL-EQ", ex_date, Decimal("5"))
    compound = (
        "Special Dividend - Rs 1.50 Per Share/ Interim Dividend - Rs 1.50 Per Share"
    )
    assert subject_to_action(_row(compound)) == Dividend(
        "NSE:COLPAL-EQ", ex_date, Decimal("1.50")
    )
    assert subject_to_action(
        _row("Rights 3:8 @ Premium Re 0.50/-", face_value="1")
    ) == Rights("NSE:COLPAL-EQ", ex_date, 3, 8, Decimal("1.50"))
    assert subject_to_action(
        _row("Rights 5:12 @ Premium Rs 855/-", face_value="10")
    ) == Rights("NSE:COLPAL-EQ", ex_date, 5, 12, Decimal("865"))


def test_subject_to_action_skips_unsupported() -> None:
    assert subject_to_action(_row("Interest Payment")) is None
    assert subject_to_action(_row("Buy Back")) is None
    assert subject_to_action(_row("Distribution - Rs 5 Per Unit")) is None
    assert subject_to_action(_row("Rights 3:8")) is None


def test_rows_to_actions_drops_unsupported_and_keeps_supported() -> None:
    rows = [
        _row("Bonus 1:1"),
        _row("Interest Payment"),
        _row("Interim Dividend - Rs 24 Per Share", symbol="HDFCBANK"),
        _row("Buy Back"),
    ]
    actions = rows_to_actions(rows)
    ex_date = date(2026, 11, 2)
    assert actions == [
        Bonus("NSE:COLPAL-EQ", ex_date, 1, 2),
        Dividend("NSE:HDFCBANK-EQ", ex_date, Decimal("24")),
    ]


def test_store_ingestion_end_to_end() -> None:
    store = DuckDbCorporateActionsStore(path=":memory:")
    actions = rows_to_actions(parse_corporate_actions(CANNED_CORPORATE_ACTIONS_JSON))
    assert len(actions) == 3

    for action in actions:
        store.add(action)
    listed = store.list()
    assert len(listed) == len(actions)
    assert set(type(a) for a in listed) == {Bonus, Dividend, Split}

    for action in actions:
        store.add(action)
    assert len(store.list()) == len(actions)


def test_parse_corporate_actions_rejects_non_json() -> None:
    with pytest.raises(ValueError, match="not valid JSON"):
        parse_corporate_actions("this is definitely not json")
