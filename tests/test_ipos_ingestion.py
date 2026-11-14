"""Tests for IPO calendar + subscription ingestion (gap 0022).

Covers the NSE provider (deterministic parse of the documented payload shape
with exact-``Decimal`` bands/sizes/multiples, plus the verified ``activeCat``
srNo mapping), the DuckDB stores, the Polars reader, the ingest CLI and the
bridge exporter.
"""

from datetime import date
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tantra_data.adapters.ipo_store import DuckDbIpoStore
from tantra_data.adapters.ipo_subscription_store import DuckDbIpoSubscriptionStore
from tantra_data.cli.ingest_ipos import main as ingest_cli_main
from tantra_data.domain.ipos import IpoIssue, IpoSubscription
from tantra_data.providers import ipos as provider
from tantra_data.providers.ipos import (
    IpoIssueRow,
    parse_ipo_issues,
    rows_to_issues,
    rows_to_subscriptions,
)
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter
from tantra_data.service.ipos_reader import (
    IPO_SCHEMA,
    IPO_SUBSCRIPTION_SCHEMA,
    PythonIpoReader,
)

PAYLOAD = {
    "as_of_date": "2026-11-14",
    "issues": [
        {
            "symbol": "VPFL",
            "company_name": "Vedant Poly Fabs Limited",
            "board": "SME",
            "status": "listed",
            "open_date": "02-Nov-2026",
            "close_date": "04-Nov-2026",
            "listing_date": "09-Nov-2026",
            "price_band": "Rs.88 to Rs.94",
            "issue_price": "94",
        },
        {
            "symbol": "MAXENGS",
            "company_name": "Maxwell Engineering Solutions Limited",
            "board": "SME",
            "status": "listed",
            "open_date": "2026-10-30",
            "close_date": "2026-11-03",
            "listing_date": "2026-11-06",
            "price_band_low": "150",
            "price_band_high": "158",
            "issue_price": "158",
            "lot_size": "800",
            "issue_size_cr": "20.72",
        },
        {
            "symbol": "BCSSL",
            "company_name": "Bombay Coated and Special Steels Limited",
            "board": "EQ",
            "status": "listed",
            "open_date": "29-Oct-2026",
            "close_date": "02-Nov-2026",
            "listing_date": "05-Nov-2026",
            "price_band": "Rs.78 to Rs.82",
            "issue_price": "82",
        },
    ],
    "subscriptions": [
        {
            "symbol": "MAXENGS",
            "qib": "0.00",
            "nii": "0.00",
            "bnii": "0.00",
            "snii": "0.00",
            "rii": "0.00",
            "total": "0.00",
        }
    ],
}


def _sample_rows() -> list[IpoIssueRow]:
    return parse_ipo_issues(PAYLOAD).issue_rows


def _make_exporter(issues, subscriptions) -> LakehouseBridgeExporter:
    return LakehouseBridgeExporter(None, None, ipos=issues, ipo_subscriptions=subscriptions)


# ---------------------------------------------------------------------------
# Provider: deterministic parse of the documented NSE IPO payload
# ---------------------------------------------------------------------------


def test_parse_issues_exact_decimal_band_dates_and_board() -> None:
    parsed = parse_ipo_issues(PAYLOAD)
    assert parsed.as_of_date == date(2026, 11, 14)
    assert len(parsed.issue_rows) == 3

    vpfl = parsed.issue_rows[0]
    assert isinstance(vpfl, IpoIssueRow)
    assert vpfl.symbol == "VPFL"
    assert vpfl.board == "SME"
    assert vpfl.status == "listed"
    assert vpfl.open_date == date(2026, 11, 2)
    assert vpfl.close_date == date(2026, 11, 4)
    assert vpfl.listing_date == date(2026, 11, 9)
    assert vpfl.price_band_low == Decimal("88")
    assert vpfl.price_band_high == Decimal("94")
    assert vpfl.issue_price == Decimal("94")
    assert isinstance(vpfl.price_band_low, Decimal)

    maxengs = parsed.issue_rows[1]
    assert maxengs.price_band_low == Decimal("150")  # explicit low/high win
    assert maxengs.price_band_high == Decimal("158")
    assert maxengs.lot_size == Decimal("800")
    assert maxengs.issue_size_cr == Decimal("20.72")

    bcssl = parsed.issue_rows[2]
    assert bcssl.board == "EQ"
    assert bcssl.price_band_low == Decimal("78")
    assert bcssl.price_band_high == Decimal("82")

    assert all(
        isinstance(row.price_band_low, Decimal) and isinstance(row.price_band_high, Decimal)
        for row in parsed.issue_rows
    )


def test_parse_single_number_price_band_and_skips_bad_rows() -> None:
    payload = {
        "as_of_date": "2026-11-14",
        "issues": [
            {
                "symbol": "ONE",
                "company_name": "Single Band Co",
                "board": "EQ",
                "status": "upcoming",
                "open_date": "2026-12-01",
                "close_date": "2026-12-03",
                "price_band": "120",
            },
            {"company_name": "No Symbol Co", "open_date": "2026-12-01", "close_date": "2026-12-03"},
            {
                "symbol": "NOCOMPANY",
                "open_date": "2026-12-01",
                "close_date": "2026-12-03",
                "price_band": "Rs.10 to Rs.12",
            },
            {
                "symbol": "NOBAND",
                "company_name": "No Band Co",
                "open_date": "2026-12-01",
                "close_date": "2026-12-03",
            },
        ],
    }
    parsed = parse_ipo_issues(payload)
    assert len(parsed.issue_rows) == 1
    row = parsed.issue_rows[0]
    assert row.symbol == "ONE"
    assert row.price_band_low == Decimal("120")
    assert row.price_band_high == Decimal("120")


def test_parse_subscriptions_exact_decimal_multiples() -> None:
    parsed = parse_ipo_issues(PAYLOAD)
    assert len(parsed.subscription_rows) == 1
    sub = parsed.subscription_rows[0]
    assert sub.symbol == "MAXENGS"
    assert sub.as_of_date == date(2026, 11, 14)
    assert sub.qib == Decimal("0.00")
    assert sub.nii == Decimal("0.00")
    assert sub.bnii == Decimal("0.00")
    assert sub.snii == Decimal("0.00")
    assert sub.rii == Decimal("0.00")
    assert sub.total == Decimal("0.00")
    assert all(isinstance(value, Decimal) for value in (sub.qib, sub.nii, sub.total))


def test_parse_rejects_non_dict_payload() -> None:
    with pytest.raises(ValueError, match="must be a dict"):
        parse_ipo_issues("not a dict")


def test_rows_map_onto_domain_records() -> None:
    rows = _sample_rows()
    issues = rows_to_issues(rows)
    assert all(isinstance(issue, IpoIssue) for issue in issues)
    assert issues[0].symbol == "VPFL"
    assert issues[0].source == "nse"
    assert issues[0].as_of_date == date(2026, 11, 14)

    subscriptions = rows_to_subscriptions(parse_ipo_issues(PAYLOAD).subscription_rows)
    assert all(isinstance(sub, IpoSubscription) for sub in subscriptions)
    assert subscriptions[0].source == "nse"


def test_domain_validation_rejects_bad_values() -> None:
    with pytest.raises(ValueError, match="close_date"):
        IpoIssue(
            symbol="X",
            company_name="X Co",
            board="EQ",
            status="upcoming",
            open_date=date(2026, 12, 3),
            close_date=date(2026, 12, 1),
            listing_date=None,
            price_band_low=Decimal("10"),
            price_band_high=Decimal("12"),
            issue_price=None,
            lot_size=None,
            issue_size_cr=None,
            as_of_date=date(2026, 11, 14),
        )
    with pytest.raises(ValueError, match="price_band_high"):
        IpoIssue(
            symbol="X",
            company_name="X Co",
            board="EQ",
            status="upcoming",
            open_date=date(2026, 12, 1),
            close_date=date(2026, 12, 3),
            listing_date=None,
            price_band_low=Decimal("15"),
            price_band_high=Decimal("12"),
            issue_price=None,
            lot_size=None,
            issue_size_cr=None,
            as_of_date=date(2026, 11, 14),
        )
    with pytest.raises(ValueError, match="invalid board"):
        IpoIssue(
            symbol="X",
            company_name="X Co",
            board="ETF",
            status="upcoming",
            open_date=date(2026, 12, 1),
            close_date=date(2026, 12, 3),
            listing_date=None,
            price_band_low=Decimal("10"),
            price_band_high=Decimal("12"),
            issue_price=None,
            lot_size=None,
            issue_size_cr=None,
            as_of_date=date(2026, 11, 14),
        )


def test_detail_subscription_maps_verified_active_cat_rows() -> None:
    detail = {
        "activeCat": {
            "dataList": [
                {
                    "category": "Category",
                    "noOfTotalMeant": "No. of times of total meant for the category",
                    "srNo": "Sr.No.",
                },
                {
                    "category": "Qualified Institutional Buyers(QIBs)",
                    "noOfTotalMeant": "44.12",
                    "srNo": "1",
                },
                {
                    "category": "Non Institutional Investors",
                    "noOfTotalMeant": "66.99",
                    "srNo": "2",
                },
                {
                    "category": "Non Institutional Investors(bid > 10 lakh)",
                    "noOfTotalMeant": "71.20",
                    "srNo": "2.1",
                },
                {
                    "category": "Non Institutional Investors(bid 2-10 lakh)",
                    "noOfTotalMeant": "51.30",
                    "srNo": "2.2",
                },
                {"category": "Retail", "noOfTotalMeant": "9.91", "srNo": "3"},
                {"category": "Total", "noOfTotalMeant": "33.44", "srNo": None},
            ]
        }
    }
    sub = provider._detail_subscription(detail, "NOVA", date(2026, 11, 14))
    assert sub is not None
    assert sub["qib"] == "44.12"
    assert sub["nii"] == "66.99"
    assert sub["bnii"] == "71.20"
    assert sub["snii"] == "51.30"
    assert sub["rii"] == "9.91"
    assert sub["total"] == "33.44"

    assert provider._detail_subscription(None, "NOVA", date(2026, 11, 14)) is None


# ---------------------------------------------------------------------------
# Stores: DuckDB round trips with idempotent upserts
# ---------------------------------------------------------------------------


def _sample_issues():
    return rows_to_issues(_sample_rows())


def _sample_subscriptions():
    return rows_to_subscriptions(parse_ipo_issues(PAYLOAD).subscription_rows)


def test_ipo_store_round_trip_and_idempotent_upsert() -> None:
    store = DuckDbIpoStore(":memory:")
    try:
        issues = _sample_issues()
        for issue in issues:
            store.upsert(issue)
        store.upsert(issues[0])  # same key -> replace
        assert len(store.query()) == 3

        listed = store.query(status="listed")
        assert len(listed) == 3

        one = store.query(symbol="VPFL")
        assert len(one) == 1
        assert one[0].company_name == "Vedant Poly Fabs Limited"
        assert one[0].price_band_low == Decimal("88")
        assert one[0].lot_size is None
        assert one[0].source == "nse"

        assert store.query(symbol="NONEXIST") == []
        assert store.query(as_of=date(2026, 11, 13)) == []
        assert len(store.query(as_of=date(2026, 11, 14))) == 3
    finally:
        store.close()


def test_subscription_store_round_trip_with_none_columns() -> None:
    store = DuckDbIpoSubscriptionStore(":memory:")
    try:
        subscriptions = _sample_subscriptions()
        for subscription in subscriptions:
            store.upsert(subscription)
        store.upsert(subscriptions[0])  # idempotent
        assert len(store.query()) == 1
        stored = store.query(symbol="MAXENGS")[0]
        assert stored.qib == Decimal("0.00")
        assert stored.total == Decimal("0.00")
        assert stored.as_of_date == date(2026, 11, 14)

        sparse = IpoSubscription(
            symbol="NEWIPO",
            as_of_date=date(2026, 11, 14),
            qib=None,
            nii=None,
            bnii=None,
            snii=None,
            rii=None,
            total=None,
            source="nse",
        )
        store.upsert(sparse)
        assert len(store.query()) == 2
        new = store.query(symbol="NEWIPO")[0]
        assert new.qib is None
        assert new.total is None
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Reader: canonical Polars frames with exact Decimal(18, 4)
# ---------------------------------------------------------------------------


def test_reader_returns_exact_decimal_frames_and_canonical_empty() -> None:
    issues = DuckDbIpoStore(":memory:")
    subscriptions = DuckDbIpoSubscriptionStore(":memory:")
    reader = PythonIpoReader(issues, subscriptions)
    try:
        for issue in _sample_issues():
            issues.upsert(issue)
        for subscription in _sample_subscriptions():
            subscriptions.upsert(subscription)

        frame = reader.get_issues()
        assert frame.height == 3
        assert frame.columns == list(IPO_SCHEMA)
        assert frame["price_band_low"].dtype == pl.Decimal(18, 4)
        assert frame["open_date"].dtype == pl.Date
        vpfl = frame.filter(pl.col("symbol") == "VPFL")
        assert vpfl["price_band_low"][0] == Decimal("88.0000")
        assert vpfl["price_band_high"][0] == Decimal("94.0000")

        listed = reader.get_issues(status="listed")
        assert listed.height == 3

        subs = reader.get_subscriptions()
        assert subs.height == 1
        assert subs.columns == list(IPO_SUBSCRIPTION_SCHEMA)
        assert subs["qib"].dtype == pl.Decimal(18, 4)
        assert subs["qib"][0] == Decimal("0.0000")

        empty = reader.get_issues(status="open")
        assert empty.height == 0
        assert empty.columns == list(IPO_SCHEMA)
        assert empty["price_band_low"].dtype == pl.Decimal(18, 4)
        empty_subs = reader.get_subscriptions(symbol="NONEXIST")
        assert empty_subs.height == 0
        assert empty_subs.columns == list(IPO_SUBSCRIPTION_SCHEMA)
    finally:
        issues.close()
        subscriptions.close()


# ---------------------------------------------------------------------------
# CLI: ingest IPO data into the DuckDB stores
# ---------------------------------------------------------------------------


def test_ingest_cli_upserts_into_stores(tmp_path, monkeypatch) -> None:
    parsed = parse_ipo_issues(PAYLOAD)
    monkeypatch.setattr(provider, "fetch_ipo_issues", lambda as_of=None: parsed)

    db = str(tmp_path / "ipos.duckdb")
    rc = ingest_cli_main(["--db", db, "--as-of", "2026-11-14"])
    assert rc == 0

    issues = DuckDbIpoStore(db)
    subscriptions = DuckDbIpoSubscriptionStore(db)
    try:
        assert len(issues.query()) == 3
        assert len(subscriptions.query()) == 1
        assert issues.query(symbol="MAXENGS")[0].lot_size == Decimal("800")
        assert subscriptions.query()[0].symbol == "MAXENGS"
    finally:
        issues.close()
        subscriptions.close()


# ---------------------------------------------------------------------------
# Bridge exporter: per-date Parquet projections (exact Decimal128)
# ---------------------------------------------------------------------------


def _seed_stores(issues, subscriptions) -> None:
    for issue in _sample_issues():
        issues.upsert(issue)
    for subscription in _sample_subscriptions():
        subscriptions.upsert(subscription)


def test_export_ipos_and_subscriptions_write_parquet(tmp_path):
    issues = DuckDbIpoStore(":memory:")
    subscriptions = DuckDbIpoSubscriptionStore(":memory:")
    exporter = _make_exporter(issues, subscriptions)
    try:
        _seed_stores(issues, subscriptions)
        assert exporter.export_ipos(tmp_path / "catalog") == 1
        assert exporter.export_ipo_subscriptions(tmp_path / "catalog") == 1
    finally:
        issues.close()
        subscriptions.close()

    files = sorted((tmp_path / "catalog" / "ipos").glob("*.parquet"))
    assert [file.name for file in files] == ["2026-11-14.parquet"]
    table = pq.read_table(files[0])
    assert table.column_names == [
        "as_of_date",
        "symbol",
        "company_name",
        "board",
        "status",
        "open_date",
        "close_date",
        "listing_date",
        "price_band_low",
        "price_band_high",
        "issue_price",
        "lot_size",
        "issue_size_cr",
        "source",
    ]
    assert table.schema.field("as_of_date").type == pa.date32()
    assert table.schema.field("price_band_low").type == pa.decimal128(18, 4)
    assert table.schema.field("lot_size").type == pa.decimal128(18, 4)
    data = table.to_pydict()
    vpfl_index = data["symbol"].index("VPFL")
    assert data["price_band_low"][vpfl_index] == Decimal("88.0000")
    assert data["price_band_high"][vpfl_index] == Decimal("94.0000")

    sub_files = sorted((tmp_path / "catalog" / "ipo_subscriptions").glob("*.parquet"))
    assert [file.name for file in sub_files] == ["2026-11-14.parquet"]
    sub_table = pq.read_table(sub_files[0])
    assert sub_table.column_names == [
        "as_of_date",
        "symbol",
        "qib",
        "nii",
        "bnii",
        "snii",
        "rii",
        "total",
        "source",
    ]
    assert sub_table.schema.field("qib").type == pa.decimal128(18, 4)
    assert sub_table.column("qib").to_pylist() == [Decimal("0.0000")]


def test_export_missing_stores_writes_nothing(tmp_path):
    exporter = _make_exporter(None, None)
    assert exporter.export_ipos(tmp_path / "catalog") == 0
    assert exporter.export_ipo_subscriptions(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "ipos").exists()
    assert not (tmp_path / "catalog" / "ipo_subscriptions").exists()


def test_export_bridge_cli_datasets(tmp_path):
    from tantra_data.cli.export_bridge import main as export_cli_main

    db = str(tmp_path / "ipos.duckdb")
    issues = DuckDbIpoStore(db)
    subscriptions = DuckDbIpoSubscriptionStore(db)
    try:
        _seed_stores(issues, subscriptions)
    finally:
        issues.close()
        subscriptions.close()

    catalog = str(tmp_path / "catalog")
    for dataset, folder in (
        ("ipos", "ipos"),
        ("ipo-subscriptions", "ipo_subscriptions"),
    ):
        rc = export_cli_main(["--dataset", dataset, "--db", db, "--catalog", catalog])
        assert rc == 0
        assert (tmp_path / "catalog" / folder / "2026-11-14.parquet").exists()

    rc = export_cli_main(["--dataset", "all", "--db", db, "--catalog", catalog])
    assert rc == 0
