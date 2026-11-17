"""Tests for the DuckDB mutual-fund screener store (gap 0023).

Covers the scheme / fund-stats / NAV / return records, the ``mf_holdings.isin``
↔ ``companies.isin_code`` lookthrough (weighted portfolio ROCE / P/E /
market-cap breakdown, exact ``Decimal`` math), portfolio-overlap queries, the
disclosure-lag no-look-ahead window, and the honest missing-data degradation
(no fabricated rows).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from tantra_data.store.mutual_funds import (
    LARGE_CAP_MIN_CR,
    MID_CAP_MIN_CR,
    DuckDbMutualFundsStore,
    MutualFundsStoreError,
)

_ASOF = date(2025, 12, 31)
_HOLDINGS_ASOF = date(2025, 11, 30)  # disclosed 31 days before as-of (lag 10 respected)

# isin -> (symbol, company_name, market_cap_cr, roce_pct, pe_ratio, debt_to_equity)
_COMPANIES: dict[str, tuple[str, str, str, str, str, str]] = {
    "INE002A01018": ("RELIANCE", "Reliance Industries Ltd", "1500000.0", "10.0", "10.0", "1.0"),
    "INE467B01029": ("TCS", "Tata Consultancy Services Ltd", "1200000.0", "20.0", "20.0", "0.5"),
    "INE062A01020": ("SBIN", "State Bank of India", "400000.0", "15.0", "15.0", "2.0"),
    "INE397D01024": ("BHARTIARTL", "Bharti Airtel Ltd", "4000.0", "5.0", "5.0", "0.1"),
    "INE040A01034": ("HDFCBANK", "HDFC Bank Ltd", "900000.0", "16.5", "19.0", "0.6"),
    "INE009A01021": ("INFY", "Infosys Ltd", "600000.0", "30.0", "25.0", "0.1"),
}


@pytest.fixture
def mf_store(tmp_path: Path) -> DuckDbMutualFundsStore:
    """An empty ``mutual_funds.duckdb`` store (schema only)."""
    return DuckDbMutualFundsStore(tmp_path / "mutual_funds.duckdb")


@pytest.fixture
def equity_db(tmp_path: Path) -> Path:
    """A ``fundamentals.duckdb`` with an isin-keyed ``companies`` table."""
    db = tmp_path / "fundamentals.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE TABLE companies (symbol VARCHAR, company_name VARCHAR, isin_code VARCHAR, "
        "market_cap_cr DECIMAL(18,4), roce_pct DECIMAL(18,4), pe_ratio DECIMAL(18,4), "
        "debt_to_equity DECIMAL(18,4))"
    )
    rows = [
        (company[0], company[1], isin, company[2], company[3], company[4], company[5])
        for isin, company in _COMPANIES.items()
    ]
    con.executemany(
        "INSERT INTO companies VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.close()
    return db


def _put_schemes(store: DuckDbMutualFundsStore) -> None:
    store.put_scheme(
        "122639",
        "Quant Flexi Cap Fund",
        "Equity:Flexi Cap",
        "Direct Plan",
        "Growth Option",
        "Quant Mutual Fund",
        _ASOF,
    )
    store.put_scheme(
        "122640",
        "Quant Flexi Cap Fund",
        "Equity:Flexi Cap",
        "Regular Plan",
        "Growth Option",
        "Quant Mutual Fund",
        _ASOF,
    )
    store.put_scheme(
        "118989",
        "Axis ELSS Tax Saver Fund",
        "Equity:ELSS",
        "Direct Plan",
        "Growth Option",
        "Axis Mutual Fund",
        _ASOF,
    )


def _put_stats(store: DuckDbMutualFundsStore) -> None:
    store.put_fund_stats(
        "122639",
        _ASOF,
        "123.4500",
        "0.6500",
        "12500.0000",
        "41.0000",
    )
    store.put_fund_stats(
        "122639",
        date(2025, 11, 28),
        "120.0000",
        "0.6700",
        "11800.0000",
        "39.0000",
    )
    store.put_fund_stats(
        "118989",
        _ASOF,
        "345.6000",
        "0.5200",
        "2100.0000",
        "25.0000",
    )


def _put_nav_and_returns(store: DuckDbMutualFundsStore) -> None:
    store.put_nav("122639", date(2025, 11, 28), "120.0000")
    store.put_nav("122639", _ASOF, "123.4500")
    store.put_return("122639", _ASOF, "0.0820", "0.6500")


def _put_flexi_holdings(store: DuckDbMutualFundsStore) -> None:
    """Scheme 122639: 4 matched ISINs at weights summing to 100 (all covered)."""
    store.put_holdings(
        "122639",
        _HOLDINGS_ASOF,
        [
            ("INE002A01018", "Reliance Industries", "40.0"),
            ("INE467B01029", "Tata Consultancy", "30.0"),
            ("INE062A01020", "State Bank of India", "20.0"),
            ("INE397D01024", "Bharti Airtel", "10.0"),
        ],
    )


# ---------------------------------------------------------------------------
# Schemes / stats / NAV / returns records
# ---------------------------------------------------------------------------


def test_get_schemes_filters_by_category_plan_option(mf_store: DuckDbMutualFundsStore) -> None:
    _put_schemes(mf_store)
    direct = mf_store.get_schemes(category="Equity:Flexi Cap", plan="Direct", option="Growth")
    assert [s["scheme_code"] for s in direct] == ["122639"]
    assert direct[0]["plan"] == "Direct Plan"
    assert direct[0]["option_type"] == "Growth Option"
    assert direct[0]["amc_name"] == "Quant Mutual Fund"

    flexi = mf_store.get_schemes(category="Equity:Flexi Cap")
    assert {s["scheme_code"] for s in flexi} == {"122639", "122640"}

    all_schemes = mf_store.get_schemes()
    assert len(all_schemes) == 3


def test_get_schemes_empty_store_returns_no_rows(mf_store: DuckDbMutualFundsStore) -> None:
    assert mf_store.get_schemes() == []
    assert mf_store.get_schemes(category="Equity:Flexi Cap", plan="Direct", option="Growth") == []


def test_fund_stats_records_nav_aum_ter_exact(mf_store: DuckDbMutualFundsStore) -> None:
    _put_stats(mf_store)
    stats = mf_store.get_fund_stats("122639", _ASOF)
    assert stats is not None
    assert stats["nav"] == Decimal("123.4500")
    assert stats["aum_cr"] == Decimal("12500.0000")
    assert stats["ter_pct"] == Decimal("0.6500")
    assert stats["portfolio_turnover_pct"] == Decimal("41.0000")

    point_in_time = mf_store.get_fund_stats("122639", date(2025, 11, 30))
    assert point_in_time is not None
    assert point_in_time["nav"] == Decimal("120.0000")
    assert mf_store.get_fund_stats("999999", _ASOF) is None


def test_nav_history_and_returns_read_exact(mf_store: DuckDbMutualFundsStore) -> None:
    _put_nav_and_returns(mf_store)
    navs = mf_store.get_nav_history("122639")
    assert [n["nav"] for n in navs] == [Decimal("120.0000"), Decimal("123.4500")]
    assert all(isinstance(n["nav"], Decimal) for n in navs)

    window = mf_store.get_nav_history("122639", start=_ASOF)
    assert [n["nav"] for n in window] == [Decimal("123.4500")]

    returns = mf_store.get_returns("122639")
    assert len(returns) == 1
    assert returns[0]["cagr_1y"] == Decimal("0.0820")
    assert returns[0]["expense_ratio"] == Decimal("0.6500")


def test_seed_reference_data_is_idempotent_and_empty(tmp_path: Path) -> None:
    store = DuckDbMutualFundsStore(tmp_path / "mutual_funds.duckdb")
    store.seed_reference_data()  # second call must not fail or duplicate
    store.seed_reference_data()
    assert store.get_schemes() == []
    # An empty store never fabricates rows — the schema tables exist but hold nothing.
    con = duckdb.connect(str(store.db_path))
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    finally:
        con.close()
    assert {"mf_schemes", "mf_fund_stats", "mf_holdings", "mf_nav", "mf_returns"} <= tables


# ---------------------------------------------------------------------------
# Equity lookthrough: exact weighted ROCE / P/E / market-cap breakdown
# ---------------------------------------------------------------------------


def test_lookthrough_weighted_roce_pe_and_cap_breakdown_exact(
    mf_store: DuckDbMutualFundsStore, equity_db: Path
) -> None:
    _put_flexi_holdings(mf_store)
    look = mf_store.get_portfolio_fundamentals(
        "122639", _ASOF, equity_db_path=equity_db, disclosure_lag_days=10
    )

    # weighted ROCE = (40*10 + 30*20 + 20*15 + 10*5) / 100 = 13.5
    assert look["portfolio_roce"] == Decimal("13.5000")
    assert look["portfolio_pe"] == Decimal("13.5000")
    # weighted D/E = (40*1 + 30*0.5 + 20*2 + 10*0.1) / 100 = 0.96
    assert look["portfolio_debt_to_equity"] == Decimal("0.9600")
    assert look["coverage_pct"] == Decimal("100.0000")

    # market-cap breakdown: RELIANCE/TCS large (70), SBIN mid (20), BHARTIARTL small (10)
    assert look["large_cap_pct"] == Decimal("70.0000")
    assert look["mid_cap_pct"] == Decimal("20.0000")
    assert look["small_cap_pct"] == Decimal("10.0000")

    symbols = [h["symbol"] for h in look["holdings"]]
    assert symbols == ["RELIANCE", "TCS", "SBIN", "BHARTIARTL"]
    assert {h["cap_band"] for h in look["holdings"]} == {"large", "mid", "small"}
    assert look["matched_isins"] == 4
    assert look["total_isins"] == 4
    assert look["asof_date"] == _HOLDINGS_ASOF  # snapshot, not the evaluation date


def test_lookthrough_coverage_is_honest_when_holdings_unmatched(
    mf_store: DuckDbMutualFundsStore, equity_db: Path
) -> None:
    # Half the portfolio weight has no matching companies.isin_code row.
    mf_store.put_holdings(
        "222222",
        _HOLDINGS_ASOF,
        [
            ("INE002A01018", "Reliance Industries", "50.0"),
            ("IN00UNMATCHED1", "Unlisted Start-up", "30.0"),
            ("IN00UNMATCHED2", "Derivative Hedge", "20.0"),
        ],
    )
    look = mf_store.get_portfolio_fundamentals(
        "222222", _ASOF, equity_db_path=equity_db, disclosure_lag_days=10
    )
    assert look["portfolio_roce"] == Decimal("10.0000")  # only RELIANCE matched
    assert look["coverage_pct"] == Decimal("50.0000")
    assert look["matched_isins"] == 1
    assert look["total_isins"] == 3
    # Cap breakdown only counts classified (matched) weight.
    assert look["large_cap_pct"] == Decimal("100.0000")
    assert look["mid_cap_pct"] == Decimal("0.0000")
    assert look["small_cap_pct"] == Decimal("0.0000")


def test_lookthrough_respects_disclosure_lag_no_lookahead(
    mf_store: DuckDbMutualFundsStore, equity_db: Path
) -> None:
    _put_flexi_holdings(mf_store)
    # As-of is only 5 days after the disclosure (lag 10) -> holdings are not yet
    # public, so the lookthrough is honestly empty.
    look = mf_store.get_portfolio_fundamentals(
        "122639", date(2025, 12, 5), equity_db_path=equity_db, disclosure_lag_days=10
    )
    assert look["portfolio_roce"] is None
    assert look["portfolio_pe"] is None
    assert look["coverage_pct"] == Decimal("0")
    assert look["holdings"] == []
    assert look["total_isins"] == 0


def test_lookthrough_no_holdings_for_unknown_scheme(
    mf_store: DuckDbMutualFundsStore, equity_db: Path
) -> None:
    look = mf_store.get_portfolio_fundamentals(
        "999999", _ASOF, equity_db_path=equity_db, disclosure_lag_days=10
    )
    assert look["portfolio_roce"] is None
    assert look["coverage_pct"] == Decimal("0")


# ---------------------------------------------------------------------------
# Portfolio overlap
# ---------------------------------------------------------------------------


def test_portfolio_overlap_exact(mf_store: DuckDbMutualFundsStore) -> None:
    _put_flexi_holdings(mf_store)
    mf_store.put_holdings(
        "122640",
        _HOLDINGS_ASOF,
        [
            ("INE002A01018", "Reliance Industries", "40.0"),
            ("INE467B01029", "Tata Consultancy", "30.0"),
            ("INE040A01034", "HDFC Bank", "20.0"),
            ("INE009A01021", "Infosys", "10.0"),
        ],
    )
    overlap = mf_store.get_portfolio_overlap("122639", "122640", asof_date=_ASOF)
    assert overlap["overlap_pct"] == Decimal("70.0000")  # min(40,40)+min(30,30)
    assert overlap["common_isins"] == 2
    assert overlap["scheme_code"] == "122639"
    assert overlap["reference_scheme_code"] == "122640"

    disjoint = mf_store.get_portfolio_overlap("122639", "999999", asof_date=_ASOF)
    assert disjoint["overlap_pct"] == Decimal("0.0000")
    assert disjoint["common_isins"] == 0


def test_portfolio_overlap_latest_snapshot_without_asof(mf_store: DuckDbMutualFundsStore) -> None:
    _put_flexi_holdings(mf_store)
    overlap = mf_store.get_portfolio_overlap("122639", "122639")
    assert overlap["overlap_pct"] == Decimal("100.0000")
    assert overlap["common_isins"] == 4


# ---------------------------------------------------------------------------
# Honest missing-data degradation
# ---------------------------------------------------------------------------


def test_lookthrough_without_equity_db_raises_clear_error(
    mf_store: DuckDbMutualFundsStore,
) -> None:
    with pytest.raises(MutualFundsStoreError, match="provision a TANTRA_DATA_DIR"):
        mf_store.get_portfolio_fundamentals("122639", _ASOF, equity_db_path=None)


def test_lookthrough_missing_companies_table_raises(
    mf_store: DuckDbMutualFundsStore, tmp_path: Path
) -> None:
    empty_db = tmp_path / "empty.duckdb"  # no companies table
    duckdb.connect(str(empty_db)).close()
    _put_flexi_holdings(mf_store)  # holdings present -> the equity store must be read
    with pytest.raises(MutualFundsStoreError, match="'companies' table"):
        mf_store.get_portfolio_fundamentals("122639", _ASOF, equity_db_path=empty_db)


def test_lookthrough_missing_isin_code_column_raises(
    mf_store: DuckDbMutualFundsStore, tmp_path: Path
) -> None:
    db = tmp_path / "noisin.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE companies (symbol VARCHAR, company_name VARCHAR)")
    con.close()
    _put_flexi_holdings(mf_store)
    with pytest.raises(MutualFundsStoreError, match=r"companies\.isin_code"):
        mf_store.get_portfolio_fundamentals("122639", _ASOF, equity_db_path=db)


# ---------------------------------------------------------------------------
# Consumer wire contract
# ---------------------------------------------------------------------------


def test_store_db_path_and_default_construction(tmp_path: Path) -> None:
    store = DuckDbMutualFundsStore(tmp_path / "mutual_funds.duckdb")
    assert store.db_path == tmp_path / "mutual_funds.duckdb"
    assert store.db_path.is_file()


def test_consumer_read_only_connection_to_same_db_works(
    mf_store: DuckDbMutualFundsStore,
) -> None:
    """The screener opens its own read_only connection while the store is alive."""
    _put_stats(mf_store)
    con = duckdb.connect(str(mf_store.db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT nav, ter_pct, aum_cr, portfolio_turnover_pct, asof_date "
            "FROM mf_fund_stats WHERE scheme_code = ? AND asof_date <= ? "
            "ORDER BY asof_date DESC LIMIT 1",
            ["122639", _ASOF.isoformat()],
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    assert row[0] == Decimal("123.4500")
    assert row[1] == Decimal("0.6500")


def test_market_cap_thresholds_are_documented_constants() -> None:
    assert Decimal("500000") == LARGE_CAP_MIN_CR
    assert Decimal("5000") == MID_CAP_MIN_CR
