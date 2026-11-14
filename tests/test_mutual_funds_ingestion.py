"""Tests for mutual-fund ingestion (gap 0021).

Covers the AMFI NAVAll.txt provider (deterministic parse of the documented
semicolon format + exact-``Decimal`` CAGR derivation), the DuckDB stores, the
Polars reader, the ingest CLI and the bridge exporter.
"""

from datetime import date
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tantra_data.adapters.mutual_fund_nav_store import DuckDbMutualFundNavStore
from tantra_data.adapters.mutual_fund_return_store import DuckDbMutualFundReturnStore
from tantra_data.adapters.mutual_fund_store import DuckDbMutualFundStore
from tantra_data.cli.ingest_mutual_funds import main as ingest_cli_main
from tantra_data.domain.mutual_funds import MutualFundMeta, MutualFundNav, MutualFundReturn
from tantra_data.providers import mutual_funds as provider
from tantra_data.providers.mutual_funds import (
    MutualFundRow,
    derive_cagr,
    derive_returns,
    parse_nav_all,
    rows_to_meta,
    rows_to_navs,
)
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter
from tantra_data.service.mutual_funds_reader import (
    MF_NAV_SCHEMA,
    MF_RETURN_SCHEMA,
    MUTUAL_FUND_SCHEMA,
    PythonMutualFundReader,
)

NAV_ALL_HEADER = (
    "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;"
    "Scheme Name;Plan;Option;Net Asset Value;Date"
)
NAV_ALL_TEXT = "\n".join(
    [
        NAV_ALL_HEADER,
        "",
        "Open Ended Schemes(Children\u2019s Fund - Childrens' Fund)",
        "",
        "Axis Mutual Fund",
        "",
        "135762;INF846K01WO1;-;Axis Children's Fund;Direct Plan;Growth Option;31.0814;13-Nov-2026",
        "135765;INF846K01WP8;-;Axis Children's Fund;Direct Plan;IDCW Option;28.6316;13-Nov-2026",
        "135759;INF846K01WJ1;-;Axis Children's Fund;Regular Plan;Growth Option;27.0280;13-Nov-2026",
        "",
        "Open Ended Schemes(Debt Scheme - Banking and PSU Fund)",
        "",
        "Franklin Templeton Mutual Fund",
        "",
        "129008;INF090I01KR8;-;"
        "Franklin India Banking & PSU Debt Fund;Direct Plan;Growth;25.4149;13-Nov-2026",
        "129009;INF090I01KS6;INF090I01KT4;"
        "Franklin India Banking & PSU Debt Fund;Direct Plan;IDCW;11.5005;13-Nov-2026",
        "",
    ]
)


def _sample_rows() -> list[MutualFundRow]:
    return parse_nav_all(NAV_ALL_TEXT).rows


def _sample_navs() -> list[MutualFundNav]:
    return rows_to_navs(_sample_rows())


def _make_exporter(metas, navs, returns) -> LakehouseBridgeExporter:
    return LakehouseBridgeExporter(
        None,
        None,
        mutual_funds=metas,
        mf_nav=navs,
        mf_returns=returns,
    )


# ---------------------------------------------------------------------------
# Provider: deterministic parse of the AMFI NAVAll.txt format
# ---------------------------------------------------------------------------


def test_parse_nav_all_exact_decimal_sections_and_mode_date() -> None:
    parsed = parse_nav_all(NAV_ALL_TEXT)
    assert parsed.as_of_date == date(2026, 11, 13)
    assert len(parsed.rows) == 5

    axis = parsed.rows[0]
    assert isinstance(axis, MutualFundRow)
    assert axis.scheme_code == "135762"
    assert axis.amc == "Axis Mutual Fund"
    assert axis.category == "Children\u2019s Fund - Childrens' Fund"
    assert axis.isin_growth == "INF846K01WO1"
    assert axis.isin_div_reinvestment is None
    assert axis.plan == "Direct Plan"
    assert axis.option == "Growth Option"
    assert axis.nav == Decimal("31.0814")
    assert isinstance(axis.nav, Decimal)
    assert axis.nav_date == date(2026, 11, 13)
    assert axis.repurchase_price is None
    assert axis.sale_price is None

    reinvest = parsed.rows[4]
    assert reinvest.isin_div_reinvestment == "INF090I01KT4"

    franklin = parsed.rows[3]
    assert franklin.amc == "Franklin Templeton Mutual Fund"
    assert franklin.category == "Debt Scheme - Banking and PSU Fund"

    assert all(isinstance(row.nav, Decimal) for row in parsed.rows)


def test_parse_nav_all_legacy_seven_field_repurchase_sale() -> None:
    text = "135762;INF846K01WO1;Axis Children's Fund;31.0814;30.5000;31.7500;13-Nov-2026\n"
    parsed = parse_nav_all(text)
    assert len(parsed.rows) == 1
    row = parsed.rows[0]
    assert row.repurchase_price == Decimal("30.5000")
    assert row.sale_price == Decimal("31.7500")
    assert row.isin_div_reinvestment is None


def test_parse_nav_all_skips_non_numeric_codes_and_raises_on_empty() -> None:
    text = "XYZ;ISIN;-;Not A Scheme;Plan;Option;10.00;13-Nov-2026\n"
    with pytest.raises(ValueError, match="no parseable scheme rows"):
        parse_nav_all(text)

    mixed = (
        "135762;INF846K01WO1;-;Axis Children's Fund;Direct Plan;Growth Option;31.0814;13-Nov-2026\n"
        "not-a-code;ISIN;-;Skip Me;Plan;Option;10.00;13-Nov-2026\n"
        "135766;INF846K01WM5;-;Axis Children's Fund;Direct Plan;Growth Option;0.0000;13-Nov-2026\n"
    )
    parsed = parse_nav_all(mixed)
    assert len(parsed.rows) == 1
    assert parsed.rows[0].scheme_code == "135762"


def test_parse_nav_all_mode_date_favors_most_common() -> None:
    text = (
        "135762;ISIN1;-;Scheme A;Plan;Option;10.00;13-Nov-2026\n"
        "135763;ISIN2;-;Scheme B;Plan;Option;11.00;12-Nov-2026\n"
        "135764;ISIN3;-;Scheme C;Plan;Option;12.00;13-Nov-2026\n"
    )
    parsed = parse_nav_all(text)
    assert parsed.as_of_date == date(2026, 11, 13)


def test_rows_map_onto_domain_records() -> None:
    rows = _sample_rows()
    as_of = date(2026, 11, 13)
    metas = [rows_to_meta(row, as_of) for row in rows]
    assert all(isinstance(meta, MutualFundMeta) for meta in metas)
    assert metas[0].scheme_code == "135762"
    assert metas[0].amc == "Axis Mutual Fund"
    assert metas[0].as_of_date == date(2026, 11, 13)
    assert metas[0].source == "amfi"

    navs = rows_to_navs(rows)
    assert all(isinstance(nav, MutualFundNav) for nav in navs)
    assert navs[0].nav == Decimal("31.0814")


def test_domain_validation_rejects_bad_values() -> None:
    with pytest.raises(ValueError, match="nav must be positive"):
        MutualFundNav(
            scheme_code="135762",
            nav_date=date(2026, 11, 13),
            nav="-1",
            repurchase_price=None,
            sale_price=None,
        )
    with pytest.raises(ValueError, match="scheme_code must be numeric"):
        MutualFundMeta(
            scheme_code="ABC",
            scheme_name="X",
            amc="Y",
            category=None,
            isin_growth=None,
            isin_div_reinvestment=None,
            plan=None,
            option=None,
            as_of_date=date(2026, 11, 13),
        )
    with pytest.raises(ValueError, match="value must be non-negative"):
        MutualFundReturn(
            scheme_code="135762",
            as_of_date=date(2026, 11, 13),
            cagr_1y=Decimal("0.10"),
            expense_ratio=Decimal("-0.5"),
        )


# ---------------------------------------------------------------------------
# Derived returns: exact-Decimal CAGR
# ---------------------------------------------------------------------------


def _history() -> list[MutualFundNav]:
    return [
        MutualFundNav(
            scheme_code="135762",
            nav_date=date(2025, 11, 13),
            nav=Decimal("25.0000"),
            repurchase_price=None,
            sale_price=None,
        ),
        MutualFundNav(
            scheme_code="135762",
            nav_date=date(2026, 11, 13),
            nav=Decimal("27.5000"),
            repurchase_price=None,
            sale_price=None,
        ),
    ]


def test_derive_cagr_exact_decimal() -> None:
    assert derive_cagr(_history()) == Decimal("0.1000")

    two_year = [
        MutualFundNav(
            scheme_code="1",
            nav_date=date(2024, 11, 13),
            nav=Decimal("100"),
            repurchase_price=None,
            sale_price=None,
        ),
        MutualFundNav(
            scheme_code="1",
            nav_date=date(2026, 11, 13),
            nav=Decimal("121"),
            repurchase_price=None,
            sale_price=None,
        ),
    ]
    assert derive_cagr(two_year) == Decimal("0.1000")


def test_derive_cagr_returns_none_without_history() -> None:
    assert derive_cagr([]) is None
    assert derive_cagr([_history()[0]]) is None
    same_day = _history()
    same_day[1] = MutualFundNav(
        scheme_code="135762",
        nav_date=date(2025, 11, 13),
        nav=Decimal("27.5000"),
        repurchase_price=None,
        sale_price=None,
    )
    assert derive_cagr(same_day) is None


def test_derive_returns_carries_none_when_insufficient() -> None:
    ret = derive_returns([_history()[0]], date(2026, 11, 13))
    assert isinstance(ret, MutualFundReturn)
    assert ret.cagr_1y is None
    assert ret.expense_ratio is None

    full = derive_returns(_history(), date(2026, 11, 13), expense_ratio=Decimal("0.65"))
    assert full.cagr_1y == Decimal("0.1000")
    assert full.expense_ratio == Decimal("0.65")


# ---------------------------------------------------------------------------
# Stores: DuckDB round trips with idempotent upserts
# ---------------------------------------------------------------------------


def test_meta_store_round_trip_and_idempotent_upsert() -> None:
    store = DuckDbMutualFundStore(":memory:")
    try:
        as_of = date(2026, 11, 13)
        for row in _sample_rows():
            store.upsert(rows_to_meta(row, as_of))
        store.upsert(rows_to_meta(_sample_rows()[0], as_of))  # same key -> replace
        assert len(store.query()) == 5

        axis = store.query(amc="Axis Mutual Fund")
        assert len(axis) == 3
        assert all(record.amc == "Axis Mutual Fund" for record in axis)

        one = store.query(scheme_code="135762")
        assert len(one) == 1
        assert one[0].category == "Children\u2019s Fund - Childrens' Fund"
        assert one[0].source == "amfi"

        assert store.query(scheme_code="999999") == []
        assert store.query(as_of=date(2026, 11, 12)) == []
        assert len(store.query(as_of=date(2026, 11, 13))) == 5
    finally:
        store.close()


def test_nav_store_round_trip_and_date_window() -> None:
    store = DuckDbMutualFundNavStore(":memory:")
    try:
        navs = _sample_navs()
        for nav in navs:
            store.upsert(nav)
        store.upsert(navs[0])  # idempotent
        assert len(store.query()) == len(navs)

        one = store.query(scheme_code="135762")
        assert len(one) == 1
        assert one[0].nav == Decimal("31.0814")
        assert one[0].source == "amfi"

        window = store.query(start_date=date(2026, 11, 1), end_date=date(2026, 11, 30))
        assert len(window) == len(navs)
        assert store.query(start_date=date(2027, 1, 1)) == []
    finally:
        store.close()


def test_return_store_round_trip_with_none_columns() -> None:
    store = DuckDbMutualFundReturnStore(":memory:")
    try:
        store.upsert(derive_returns(_history(), date(2026, 11, 13)))
        store.upsert(derive_returns(_history(), date(2026, 11, 13)))  # idempotent
        assert len(store.query()) == 1
        stored = store.query(scheme_code="135762")[0]
        assert stored.cagr_1y == Decimal("0.1000")
        assert stored.as_of_date == date(2026, 11, 13)

        single = MutualFundNav(
            scheme_code="135763",
            nav_date=date(2026, 11, 13),
            nav=Decimal("10.0000"),
            repurchase_price=None,
            sale_price=None,
        )
        store.upsert(derive_returns([single], date(2026, 11, 13)))
        assert len(store.query()) == 2
        sparse = store.query(scheme_code="135763")[0]
        assert sparse.cagr_1y is None
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Reader: canonical Polars frames with exact Decimal(18, 4)
# ---------------------------------------------------------------------------


def test_reader_returns_exact_decimal_frames_and_canonical_empty() -> None:
    metas = DuckDbMutualFundStore(":memory:")
    navs = DuckDbMutualFundNavStore(":memory:")
    returns = DuckDbMutualFundReturnStore(":memory:")
    reader = PythonMutualFundReader(metas, navs, returns)
    try:
        as_of = date(2026, 11, 13)
        for row in _sample_rows():
            metas.upsert(rows_to_meta(row, as_of))
            navs.upsert(rows_to_navs([row])[0])
        returns.upsert(derive_returns(_history(), as_of))

        schemes = reader.get_schemes()
        assert schemes.height == 5
        assert schemes.columns == list(MUTUAL_FUND_SCHEMA)
        assert schemes["scheme_code"].dtype == pl.String
        assert schemes["as_of_date"].dtype == pl.Date
        axis = reader.get_schemes(amc="Axis Mutual Fund")
        assert axis.height == 3

        nav_frame = reader.get_nav_history()
        assert nav_frame.height == 5
        assert nav_frame.columns == list(MF_NAV_SCHEMA)
        assert nav_frame["nav"].dtype == pl.Decimal(18, 4)
        one = reader.get_nav_history(scheme_code="135762")
        assert one.height == 1
        assert one["nav"][0] == Decimal("31.0814")

        ret_frame = reader.get_returns()
        assert ret_frame.height == 1
        assert ret_frame.columns == list(MF_RETURN_SCHEMA)
        assert ret_frame["cagr_1y"].dtype == pl.Decimal(18, 4)
        assert ret_frame["cagr_1y"][0] == Decimal("0.1000")

        empty = reader.get_schemes(amc="Nobody Mutual Fund")
        assert empty.height == 0
        assert empty.columns == list(MUTUAL_FUND_SCHEMA)
        empty_nav = reader.get_nav_history(scheme_code="999999")
        assert empty_nav.height == 0
        assert empty_nav["nav"].dtype == pl.Decimal(18, 4)
    finally:
        metas.close()
        navs.close()
        returns.close()


# ---------------------------------------------------------------------------
# CLI: ingest the AMFI dump into the DuckDB stores
# ---------------------------------------------------------------------------


def test_ingest_cli_upserts_into_stores(tmp_path, monkeypatch) -> None:
    parsed = parse_nav_all(NAV_ALL_TEXT)
    monkeypatch.setattr(provider, "fetch_mutual_funds", lambda: parsed)

    db = str(tmp_path / "mutual_funds.duckdb")
    rc = ingest_cli_main(["--db", db])
    assert rc == 0

    metas = DuckDbMutualFundStore(db)
    navs = DuckDbMutualFundNavStore(db)
    returns = DuckDbMutualFundReturnStore(db)
    try:
        assert len(metas.query()) == 5
        assert len(navs.query()) == 5
        assert len(returns.query()) == 5  # one derived-return record per scheme
        schemes = {record.scheme_code for record in returns.query()}
        assert schemes == {"129008", "129009", "135759", "135762", "135765"}
        assert all(record.cagr_1y is None for record in returns.query())  # single point
    finally:
        metas.close()
        navs.close()
        returns.close()


# ---------------------------------------------------------------------------
# Bridge exporter: per-date Parquet projections (exact Decimal128)
# ---------------------------------------------------------------------------


def _seed_stores(metas, navs, returns) -> None:
    as_of = date(2026, 11, 13)
    for row in _sample_rows():
        metas.upsert(rows_to_meta(row, as_of))
        navs.upsert(rows_to_navs([row])[0])
    returns.upsert(derive_returns(_history(), as_of))


def test_export_mutual_funds_and_nav_and_returns_write_parquet(tmp_path):
    metas = DuckDbMutualFundStore(":memory:")
    navs = DuckDbMutualFundNavStore(":memory:")
    returns = DuckDbMutualFundReturnStore(":memory:")
    exporter = _make_exporter(metas, navs, returns)
    try:
        _seed_stores(metas, navs, returns)
        assert exporter.export_mutual_funds(tmp_path / "catalog") == 1
        assert exporter.export_mf_nav(tmp_path / "catalog") == 1
        assert exporter.export_mf_returns(tmp_path / "catalog") == 1
    finally:
        metas.close()
        navs.close()
        returns.close()

    meta_files = sorted((tmp_path / "catalog" / "mutual_funds").glob("*.parquet"))
    assert [file.name for file in meta_files] == ["2026-11-13.parquet"]
    table = pq.read_table(meta_files[0])
    assert table.column_names == [
        "as_of_date",
        "scheme_code",
        "scheme_name",
        "amc",
        "category",
        "isin_growth",
        "isin_div_reinvestment",
        "plan",
        "option",
        "source",
    ]
    assert table.schema.field("as_of_date").type == pa.date32()
    schemes = table.column("scheme_code").to_pylist()
    assert sorted(schemes) == ["129008", "129009", "135759", "135762", "135765"]

    nav_files = sorted((tmp_path / "catalog" / "mf_nav").glob("*.parquet"))
    assert [file.name for file in nav_files] == ["2026-11-13.parquet"]
    nav_table = pq.read_table(nav_files[0])
    assert nav_table.column_names == [
        "nav_date",
        "scheme_code",
        "nav",
        "repurchase_price",
        "sale_price",
        "source",
    ]
    assert nav_table.schema.field("nav").type == pa.decimal128(18, 4)
    assert Decimal("31.0814") in nav_table.column("nav").to_pylist()

    ret_files = sorted((tmp_path / "catalog" / "mf_returns").glob("*.parquet"))
    assert [file.name for file in ret_files] == ["2026-11-13.parquet"]
    ret_table = pq.read_table(ret_files[0])
    assert ret_table.schema.field("cagr_1y").type == pa.decimal128(18, 4)
    assert ret_table.column("cagr_1y").to_pylist() == [Decimal("0.1000")]


def test_export_missing_stores_writes_nothing(tmp_path):
    exporter = _make_exporter(None, None, None)
    assert exporter.export_mutual_funds(tmp_path / "catalog") == 0
    assert exporter.export_mf_nav(tmp_path / "catalog") == 0
    assert exporter.export_mf_returns(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "mutual_funds").exists()
    assert not (tmp_path / "catalog" / "mf_nav").exists()
    assert not (tmp_path / "catalog" / "mf_returns").exists()


def test_export_bridge_cli_datasets(tmp_path):
    from tantra_data.cli.export_bridge import main as export_cli_main

    db = str(tmp_path / "mutual_funds.duckdb")
    metas = DuckDbMutualFundStore(db)
    navs = DuckDbMutualFundNavStore(db)
    returns = DuckDbMutualFundReturnStore(db)
    try:
        _seed_stores(metas, navs, returns)
    finally:
        metas.close()
        navs.close()
        returns.close()

    catalog = str(tmp_path / "catalog")
    for dataset, folder in (
        ("mutual-funds", "mutual_funds"),
        ("mf-nav", "mf_nav"),
        ("mf-returns", "mf_returns"),
    ):
        rc = export_cli_main(["--dataset", dataset, "--db", db, "--catalog", catalog])
        assert rc == 0
        assert (tmp_path / "catalog" / folder / "2026-11-13.parquet").exists()

    rc = export_cli_main(["--dataset", "all", "--db", db, "--catalog", catalog])
    assert rc == 0
