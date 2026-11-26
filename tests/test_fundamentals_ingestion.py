"""Tests for company-fundamentals ingestion (T5.13 / gap 0020).

Covers the yfinance provider (deterministic parse of canned yfinance-style
payloads + ratio derivation with exact ``Decimal``), the DuckDB stores,
the Polars reader, the ingest CLI and the bridge exporter.
"""

from datetime import date
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tantra_data.adapters.fundamental_ratios_store import DuckDbFundamentalRatiosStore
from tantra_data.adapters.fundamental_statement_store import DuckDbFinancialStatementStore
from tantra_data.cli.ingest_fundamentals import main as ingest_cli_main
from tantra_data.domain.fundamentals import (
    FinancialRatios,
    FinancialStatement,
    PeriodType,
    StatementType,
)
from tantra_data.providers import fundamentals as provider
from tantra_data.providers.fundamentals import (
    RatioRow,
    StatementRow,
    parse_fundamentals,
    rows_to_ratios,
    rows_to_statements,
)
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter
from tantra_data.service.fundamentals_reader import (
    RATIOS_SCHEMA,
    STATEMENT_SCHEMA,
    PythonFundamentalsReader,
)

PAYLOAD = {
    "symbol": "RELIANCE",
    "as_of_date": "2026-11-14",
    "info": {
        "trailingPE": 22.078114,
        "priceToBook": 1.9195719,
        "enterpriseToEbitda": 12.001,
        "dividendYield": 0.45,
        "returnOnEquity": 0.08759,
        "debtToEquity": 44.325,
    },
    "statements": {
        "annual": {
            "income_statement": {
                "2026-03-31": {
                    "Total Revenue": 10572190000000.0,
                    "Net Income": 807750000000.0,
                    "EBIT": 1472180000000.0,
                    "Interest Expense": 240560000000.0,
                    "Profit Before Tax": 1092980000000.0,
                },
                "2025-03-31": {
                    "Total Revenue": 9047620000000.0,
                    "Net Income": 793300000000.0,
                },
            },
            "balance_sheet": {
                "2026-03-31": {
                    "Total Assets": 21781400000000.0,
                    "Total Debt": 3980000000000.0,
                    "Stockholders Equity": 9040300000000.0,
                    "Current Liabilities": 5412540000000.0,
                }
            },
            "cash_flow": {
                "2026-03-31": {
                    "Operating Cash Flow": 1875000000000.0,
                    "Free Cash Flow": 900000000000.0,
                }
            },
        },
        "quarterly": {
            "income_statement": {
                "2026-09-30": {
                    "Total Revenue": 2835000000000.0,
                    "Net Income": 231400000000.0,
                }
            }
        },
    },
}


def _make_exporter(statements, ratios) -> LakehouseBridgeExporter:
    return LakehouseBridgeExporter(None, None, fundamentals=statements, fundamental_ratios=ratios)


def _sample_statements() -> list[FinancialStatement]:
    return rows_to_statements(parse_fundamentals(PAYLOAD).statements)


def _sample_ratios() -> FinancialRatios:
    parsed = parse_fundamentals(PAYLOAD)
    assert parsed.ratios is not None
    ratios = rows_to_ratios(parsed.ratios)
    assert ratios is not None
    return ratios


# ---------------------------------------------------------------------------
# Provider: deterministic parse of canned yfinance-style payloads
# ---------------------------------------------------------------------------


def test_parse_statements_exact_decimal_and_canonical_line_items() -> None:
    parsed = parse_fundamentals(PAYLOAD)
    assert parsed.symbol == "RELIANCE"
    assert parsed.as_of_date == date(2026, 11, 14)
    rows = parsed.statements
    assert len(rows) == 15  # 5 income + 2 prior income + 4 balance + 2 cash + 2 quarterly

    by_key = {
        (row.period_type, row.statement_type, row.period_end_date, row.line_item): row
        for row in rows
    }
    revenue = by_key[
        (PeriodType.ANNUAL, StatementType.INCOME_STATEMENT, date(2026, 3, 31), "total_revenue")
    ]
    assert isinstance(revenue, StatementRow)
    assert revenue.instrument == "NSE:RELIANCE-EQ"
    assert revenue.value == Decimal("10572190000000.0")
    assert isinstance(revenue.value, Decimal)

    assets = by_key[
        (PeriodType.ANNUAL, StatementType.BALANCE_SHEET, date(2026, 3, 31), "total_assets")
    ]
    assert assets.value == Decimal("21781400000000.0")

    cash = by_key[
        (PeriodType.ANNUAL, StatementType.CASH_FLOW, date(2026, 3, 31), "operating_cash_flow")
    ]
    assert cash.value == Decimal("1875000000000.0")

    # Unknown line items fall back to snake_case canonicalization.
    pbt = by_key[
        (
            PeriodType.ANNUAL,
            StatementType.INCOME_STATEMENT,
            date(2026, 3, 31),
            "profit_before_tax",
        )
    ]
    assert pbt.value == Decimal("1092980000000.0")

    # Quarterly statements carry the quarterly period type.
    quarterly = by_key[
        (PeriodType.QUARTERLY, StatementType.INCOME_STATEMENT, date(2026, 9, 30), "total_revenue")
    ]
    assert quarterly.period_type is PeriodType.QUARTERLY
    assert quarterly.filing_date == date(2026, 11, 14)
    quarterly_net = by_key[
        (PeriodType.QUARTERLY, StatementType.INCOME_STATEMENT, date(2026, 9, 30), "net_income")
    ]
    assert quarterly_net.value == Decimal("231400000000.0")

    # No floats anywhere in the parsed rows.
    assert all(isinstance(row.value, Decimal) for row in rows)


def test_parse_skips_non_numeric_line_items() -> None:
    payload = {
        "symbol": "TCS",
        "as_of_date": "2026-11-14",
        "statements": {
            "annual": {
                "income_statement": {
                    "2026-03-31": {"Total Revenue": "--", "Net Income": 45467000000.0}
                }
            }
        },
    }
    parsed = parse_fundamentals(payload)
    assert len(parsed.statements) == 1
    assert parsed.statements[0].line_item == "net_income"


def test_parse_rejects_non_dict_or_missing_symbol() -> None:
    with pytest.raises(ValueError, match="must be a dict"):
        parse_fundamentals("not a dict")
    with pytest.raises(ValueError, match="missing a symbol"):
        parse_fundamentals({"as_of_date": "2026-11-14", "statements": {}})


def test_look_ahead_guard_rejects_early_filing() -> None:
    with pytest.raises(ValueError, match="filing_date must be >= period_end_date"):
        FinancialStatement(
            instrument="NSE:RELIANCE-EQ",
            statement_type=StatementType.INCOME_STATEMENT,
            period_type=PeriodType.ANNUAL,
            period_end_date=date(2026, 3, 31),
            filing_date=date(2026, 1, 1),
            line_item="total_revenue",
            value=Decimal("10572190000000.0"),
        )


def test_ratios_derived_exactly_from_latest_annual_statement() -> None:
    parsed = parse_fundamentals(PAYLOAD)
    assert parsed.ratios is not None
    assert isinstance(parsed.ratios, RatioRow)
    # Valuation ratios come straight from the vendor ``info`` (exact Decimal).
    assert parsed.ratios.pe == Decimal("22.078114")
    assert parsed.ratios.pb == Decimal("1.9195719")
    assert parsed.ratios.ev_to_ebitda == Decimal("12.001")
    assert parsed.ratios.dividend_yield == Decimal("0.45")
    # Profitability / leverage ratios are derived with exact Decimal math:
    # roce = EBIT / (assets - current liabilities) = 0.0899
    assert parsed.ratios.roce == Decimal("0.0899")
    # roe = net income / equity = 0.0893
    assert parsed.ratios.roe == Decimal("0.0893")
    # debt/equity = debt / equity * 100 = 44.0251
    assert parsed.ratios.debt_to_equity == Decimal("44.0251")
    # interest coverage = EBIT / interest expense = 6.1198
    assert parsed.ratios.interest_coverage == Decimal("6.1198")
    assert parsed.ratios.source == "yfinance"


def test_ratios_fall_back_to_vendor_info_when_derivation_not_computable() -> None:
    payload = {
        "symbol": "TCS",
        "as_of_date": "2026-11-14",
        "info": {
            "returnOnEquity": 0.4879,
            "returnOnCapital": 0.51,
            "debtToEquity": 12.5,
        },
        "statements": {
            "quarterly": {"income_statement": {"2026-09-30": {"Total Revenue": 65420000000.0}}}
        },
    }
    parsed = parse_fundamentals(payload)
    assert parsed.ratios is not None
    assert parsed.ratios.pe is None
    assert parsed.ratios.pb is None
    assert parsed.ratios.roce == Decimal("0.51")
    assert parsed.ratios.roe == Decimal("0.4879")
    assert parsed.ratios.debt_to_equity == Decimal("12.5")
    assert parsed.ratios.interest_coverage is None


def test_rows_map_onto_domain_records() -> None:
    statements = _sample_statements()
    assert all(isinstance(statement, FinancialStatement) for statement in statements)
    assert statements[0].instrument == "NSE:RELIANCE-EQ"
    assert statements[0].currency == "INR"

    ratios = _sample_ratios()
    assert isinstance(ratios, FinancialRatios)
    assert ratios.instrument == "NSE:RELIANCE-EQ"
    assert ratios.as_of_date == date(2026, 11, 14)
    assert ratios.pe == Decimal("22.078114")
    assert ratios.roe == Decimal("0.0893")
    assert ratios.source == "yfinance"

    assert rows_to_ratios(None) is None


def test_fetch_raises_clear_import_error_without_yfinance(monkeypatch) -> None:
    monkeypatch.setattr(provider, "_YFINANCE_AVAILABLE", False)
    monkeypatch.setattr(provider, "yf", None)
    with pytest.raises(ImportError, match="yfinance is not installed"):
        provider.fetch_fundamentals("RELIANCE")


# ---------------------------------------------------------------------------
# Stores: DuckDB round trips with point-in-time semantics
# ---------------------------------------------------------------------------


def test_statement_store_round_trip_and_idempotent_upsert() -> None:
    store = DuckDbFinancialStatementStore(":memory:")
    try:
        statements = _sample_statements()
        for statement in statements:
            store.upsert(statement)
        store.upsert(statements[0])  # same key -> replace, not duplicate
        assert len(store.query()) == len(statements)

        revenue = store.query(line_item="total_revenue")
        assert len(revenue) == 3  # annual 2026 + annual 2025 + quarterly 2026
        assert all(record.line_item == "total_revenue" for record in revenue)

        annual = store.query(period_type=PeriodType.ANNUAL)
        assert len(annual) == 13
        assert all(record.period_type is PeriodType.ANNUAL for record in annual)

        reliance = store.query(instrument="NSE:RELIANCE-EQ")
        assert len(reliance) == len(statements)
        assert store.query(instrument="NSE:TCS-EQ") == []

        # As-of filtering: only line items filed on or before the date.
        visible = store.query(instrument="NSE:RELIANCE-EQ", as_of=date(2026, 11, 14))
        assert len(visible) == len(statements)
        assert store.query(as_of=date(2026, 11, 13)) == []

        balance = store.query(statement_type=StatementType.BALANCE_SHEET)
        assert len(balance) == 4
        assert all(record.statement_type is StatementType.BALANCE_SHEET for record in balance)
    finally:
        store.close()


def test_ratios_store_round_trip_with_none_columns() -> None:
    store = DuckDbFundamentalRatiosStore(":memory:")
    try:
        ratios = _sample_ratios()
        store.upsert(ratios)
        store.upsert(ratios)  # idempotent
        assert len(store.query()) == 1
        stored = store.query(instrument="NSE:RELIANCE-EQ")[0]
        assert stored.as_of_date == date(2026, 11, 14)
        assert stored.pe == Decimal("22.078114")
        assert stored.roce == Decimal("0.0899")
        assert stored.interest_coverage == Decimal("6.1198")
        assert stored.source == "yfinance"
        assert store.query(instrument="NSE:TCS-EQ") == []

        sparse = FinancialRatios(
            instrument="NSE:TCS-EQ",
            as_of_date=date(2026, 11, 14),
            pe=None,
            pb=None,
            ev_to_ebitda=None,
            dividend_yield=None,
            roce=None,
            roe=None,
            debt_to_equity=None,
            interest_coverage=None,
            source="yfinance",
        )
        store.upsert(sparse)
        assert len(store.query()) == 2
        tcs = store.query(instrument="NSE:TCS-EQ")[0]
        assert tcs.pe is None
        assert tcs.interest_coverage is None
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Reader: canonical Polars frames with exact Decimal(18, 4)
# ---------------------------------------------------------------------------


def test_reader_returns_exact_decimal_frames_and_canonical_empty() -> None:
    statements = DuckDbFinancialStatementStore(":memory:")
    ratios = DuckDbFundamentalRatiosStore(":memory:")
    reader = PythonFundamentalsReader(statements, ratios)
    try:
        for statement in _sample_statements():
            statements.upsert(statement)
        ratios.upsert(_sample_ratios())

        frame = reader.get_statements()
        assert frame.height == 15
        assert frame.columns == [
            "instrument",
            "statement_type",
            "period_type",
            "period_end_date",
            "filing_date",
            "line_item",
            "value",
            "currency",
        ]
        assert frame["value"].dtype == pl.Decimal(18, 4)
        assert frame["period_end_date"].dtype == pl.Date
        revenue = frame.filter(
            (pl.col("line_item") == "total_revenue") & (pl.col("period_type") == "annual")
        )
        assert revenue["value"].to_list() == [
            Decimal("9047620000000.0000"),
            Decimal("10572190000000.0000"),
        ]

        quarterly = reader.get_statements(period_type="quarterly")
        assert quarterly.height == 2
        assert quarterly["period_type"][0] == "quarterly"

        ratios_frame = reader.get_ratios()
        assert ratios_frame.height == 1
        assert ratios_frame.columns == list(RATIOS_SCHEMA)
        assert ratios_frame["pe"].dtype == pl.Decimal(18, 4)
        assert ratios_frame["pe"][0] == Decimal("22.0781")
        assert ratios_frame["roce"][0] == Decimal("0.0899")

        empty = reader.get_statements(instrument="NSE:TCS-EQ")
        assert empty.height == 0
        assert empty.columns == list(STATEMENT_SCHEMA)
        assert empty["value"].dtype == pl.Decimal(18, 4)
        empty_ratios = reader.get_ratios(instrument="NSE:TCS-EQ")
        assert empty_ratios.height == 0
        assert empty_ratios.columns == list(RATIOS_SCHEMA)
    finally:
        statements.close()
        ratios.close()


# ---------------------------------------------------------------------------
# CLI: ingest fundamentals into the DuckDB stores
# ---------------------------------------------------------------------------


def test_ingest_cli_upserts_into_stores(tmp_path, monkeypatch) -> None:
    parsed = parse_fundamentals(PAYLOAD)
    monkeypatch.setattr(provider, "fetch_fundamentals", lambda symbol, as_of=None: parsed)

    db = str(tmp_path / "fundamentals.duckdb")
    rc = ingest_cli_main(["--db", db, "--symbol", "RELIANCE"])
    assert rc == 0

    statements = DuckDbFinancialStatementStore(db)
    ratios = DuckDbFundamentalRatiosStore(db)
    try:
        assert len(statements.query()) == 15
        assert len(ratios.query()) == 1
        assert ratios.query()[0].instrument == "NSE:RELIANCE-EQ"
    finally:
        statements.close()
        ratios.close()


# ---------------------------------------------------------------------------
# Bridge exporter: per-date Parquet projections (exact Decimal128)
# ---------------------------------------------------------------------------


def test_export_fundamentals_and_ratios_write_parquet(tmp_path):
    statements = DuckDbFinancialStatementStore(":memory:")
    ratios = DuckDbFundamentalRatiosStore(":memory:")
    exporter = _make_exporter(statements, ratios)
    try:
        for statement in _sample_statements():
            statements.upsert(statement)
        ratios.upsert(_sample_ratios())
        assert exporter.export_fundamentals(tmp_path / "catalog") == 1
        assert exporter.export_fundamental_ratios(tmp_path / "catalog") == 1
    finally:
        statements.close()
        ratios.close()

    files = sorted((tmp_path / "catalog" / "fundamentals").glob("*.parquet"))
    assert [file.name for file in files] == ["2026-11-14.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == [
        "filing_date",
        "instrument",
        "statement_type",
        "period_type",
        "period_end_date",
        "line_item",
        "value",
        "currency",
    ]
    assert table.schema.field("filing_date").type == pa.date32()
    assert table.schema.field("period_end_date").type == pa.date32()
    assert table.schema.field("value").type == pa.decimal128(18, 4)
    data = table.to_pydict()
    revenue_values = [
        value
        for value, line_item, period_type in zip(
            data["value"],
            data["line_item"],
            data["period_type"],
            strict=True,
        )
        if line_item == "total_revenue" and period_type == "annual"
    ]
    assert sorted(revenue_values) == [
        Decimal("9047620000000.0000"),
        Decimal("10572190000000.0000"),
    ]

    ratio_files = sorted((tmp_path / "catalog" / "fundamental_ratios").glob("*.parquet"))
    assert [file.name for file in ratio_files] == ["2026-11-14.parquet"]
    ratio_table = pq.read_table(ratio_files[0])
    assert ratio_table.column_names == [
        "as_of_date",
        "instrument",
        "pe",
        "pb",
        "ev_to_ebitda",
        "dividend_yield",
        "roce",
        "roe",
        "debt_to_equity",
        "interest_coverage",
        "source",
    ]
    assert ratio_table.schema.field("pe").type == pa.decimal128(18, 4)
    assert ratio_table.schema.field("roe").type == pa.decimal128(18, 4)
    assert ratio_table.column("pe").to_pylist() == [Decimal("22.0781")]
    assert ratio_table.column("roce").to_pylist() == [Decimal("0.0899")]


def test_export_missing_stores_writes_nothing(tmp_path):
    exporter = _make_exporter(None, None)
    assert exporter.export_fundamentals(tmp_path / "catalog") == 0
    assert exporter.export_fundamental_ratios(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "fundamentals").exists()
    assert not (tmp_path / "catalog" / "fundamental_ratios").exists()


def test_export_bridge_cli_dataset_fundamentals(tmp_path):
    from tantra_data.cli.export_bridge import main as export_cli_main

    db = str(tmp_path / "fundamentals.duckdb")
    statements = DuckDbFinancialStatementStore(db)
    ratios = DuckDbFundamentalRatiosStore(db)
    try:
        for statement in _sample_statements():
            statements.upsert(statement)
        ratios.upsert(_sample_ratios())
    finally:
        statements.close()
        ratios.close()

    catalog = str(tmp_path / "catalog")
    rc = export_cli_main(["--dataset", "fundamentals", "--db", db, "--catalog", catalog])
    assert rc == 0
    assert (tmp_path / "catalog" / "fundamentals" / "2026-11-14.parquet").exists()

    rc = export_cli_main(["--dataset", "fundamental-ratios", "--db", db, "--catalog", catalog])
    assert rc == 0
    assert (tmp_path / "catalog" / "fundamental_ratios" / "2026-11-14.parquet").exists()

    rc = export_cli_main(["--dataset", "all", "--db", db, "--catalog", catalog])
    assert rc == 0
