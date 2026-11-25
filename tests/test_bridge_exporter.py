"""Tests for the lakehouse bridge exporter (gaps 0007/0008 Parquet projections)."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.adapters.block_deals_store import DuckDbBlockDealsStore
from tantra_data.adapters.delivery_store import DuckDbDeliveryStore
from tantra_data.adapters.duckdb_store import (
    DuckDbCalendarStore,
    DuckDbCorporateActionsStore,
)
from tantra_data.adapters.filings_store import DuckDbFilingsStore
from tantra_data.adapters.institutional_flow_store import DuckDbInstitutionalFlowStore
from tantra_data.adapters.manifest_store import ManifestStore
from tantra_data.adapters.news_store import DuckDbNewsStore
from tantra_data.cli.export_bridge import main
from tantra_data.domain.block_deals import BlockDeal
from tantra_data.domain.calendar import CalendarEvent
from tantra_data.domain.corporate_actions import Bonus, Dividend, Split, to_record
from tantra_data.domain.delivery import DeliveryRecord
from tantra_data.domain.filings import Filing, FilingType
from tantra_data.domain.institutional_flow import InstitutionalFlow
from tantra_data.domain.news import NewsArticle
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter


def test_export_filings_writes_one_parquet_per_filing_date(tmp_path):
    store = DuckDbFilingsStore(":memory:")
    store.upsert(
        Filing(
            instrument="NSE:RELIANCE-EQ",
            filing_date=date(2026, 1, 10),
            filing_type=FilingType.CORPORATE_ACTION,
            title="Board meeting outcome",
            source_url="https://example.com/board-meeting",
        )
    )
    store.upsert(
        Filing(
            instrument="NSE:TCS-EQ",
            filing_date=date(2026, 1, 10),
            filing_type=FilingType.SHAREHOLDING,
            title="Shareholding pattern",
            source_url=None,
        )
    )
    store.upsert(
        Filing(
            instrument="NSE:HDFC-EQ",
            filing_date=date(2026, 2, 5),
            filing_type=FilingType.ANNUAL_REPORT,
            title="Annual report FY26",
            source_url="https://example.com/annual-report",
        )
    )

    exporter = LakehouseBridgeExporter(store, DuckDbCorporateActionsStore(":memory:"))
    try:
        assert exporter.export_filings(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted((tmp_path / "catalog" / "filings").glob("*.parquet"))
    assert len(files) == 2

    table = pq.read_table(files[0])
    assert table.column_names == [
        "filing_date",
        "instrument",
        "filing_type",
        "title",
        "source_url",
    ]
    assert table.schema.field("filing_date").type == pa.date32()
    assert table.schema.field("source_url").type == pa.string()

    rows = sorted(
        zip(
            table.column("instrument").to_pylist(),
            table.column("filing_type").to_pylist(),
            table.column("title").to_pylist(),
            table.column("source_url").to_pylist(),
            strict=False,
        )
    )
    assert rows == [
        (
            "NSE:RELIANCE-EQ",
            "corporate_action",
            "Board meeting outcome",
            "https://example.com/board-meeting",
        ),
        ("NSE:TCS-EQ", "shareholding", "Shareholding pattern", None),
    ]
    assert all(d == date(2026, 1, 10) for d in table.column("filing_date").to_pylist())


def test_export_corporate_actions_groups_by_ex_date(tmp_path):
    store = DuckDbCorporateActionsStore(":memory:")
    store.add(
        Split(
            instrument="NSE:RELIANCE-EQ",
            ex_date=date(2026, 3, 1),
            old_shares=1,
            new_shares=2,
        )
    )
    store.add(
        Dividend(
            instrument="NSE:TCS-EQ",
            ex_date=date(2026, 3, 1),
            per_share=Decimal("15.50"),
        )
    )
    store.add(
        Bonus(
            instrument="NSE:INFY-EQ",
            ex_date=date(2026, 4, 1),
            old_shares=1,
            new_shares=2,
        )
    )

    record = to_record(
        Split(
            instrument="NSE:RELIANCE-EQ",
            ex_date=date(2026, 3, 1),
            old_shares=1,
            new_shares=2,
        )
    )
    assert record.action_type.value == "split"
    assert record.old_shares == 1
    assert record.new_shares == 2

    exporter = LakehouseBridgeExporter(DuckDbFilingsStore(":memory:"), store)
    try:
        assert exporter.export_corporate_actions(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted((tmp_path / "catalog" / "corporate_actions").glob("*.parquet"))
    assert len(files) == 2

    table = pq.read_table(files[0])
    assert table.column_names == [
        "ex_date",
        "instrument",
        "action_type",
        "old_shares",
        "new_shares",
        "per_share",
        "issue_price",
        "currency",
    ]
    assert table.schema.field("old_shares").type == pa.int32()
    assert table.schema.field("new_shares").type == pa.int32()
    assert table.schema.field("per_share").type == pa.decimal128(18, 4)
    assert table.schema.field("issue_price").type == pa.decimal128(18, 4)

    rows = sorted(
        zip(
            table.column("instrument").to_pylist(),
            table.column("action_type").to_pylist(),
            table.column("old_shares").to_pylist(),
            table.column("new_shares").to_pylist(),
            table.column("per_share").to_pylist(),
            table.column("issue_price").to_pylist(),
            table.column("currency").to_pylist(),
            strict=False,
        )
    )
    assert rows == [
        ("NSE:RELIANCE-EQ", "split", 1, 2, None, None, "INR"),
        ("NSE:TCS-EQ", "dividend", None, None, Decimal("15.50"), None, "INR"),
    ]

    other = pq.read_table(files[1])
    assert other.column("ex_date").to_pylist() == [date(2026, 4, 1)]
    assert other.column("action_type").to_pylist() == ["bonus"]
    assert other.column("old_shares").to_pylist() == [1]
    assert other.column("new_shares").to_pylist() == [2]


def test_empty_stores_write_no_files(tmp_path):
    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
    )
    assert exporter.export_filings(tmp_path / "catalog") == 0
    assert exporter.export_corporate_actions(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "filings").exists()
    assert not (tmp_path / "catalog" / "corporate_actions").exists()


def test_export_block_deals_groups_by_trade_date(tmp_path):
    store = DuckDbBlockDealsStore(":memory:")
    store.upsert(
        BlockDeal(
            trade_date=date(2026, 6, 1),
            exchange="NSE",
            instrument="NSE:RELIANCE-EQ",
            security_name="RELIANCE",
            client_name="CLIENT A",
            side="buy",
            category="block",
            quantity=Decimal("250000.00"),
            price=Decimal("2500.50"),
            remarks="Block deal window",
        )
    )
    store.upsert(
        BlockDeal(
            trade_date=date(2026, 6, 1),
            exchange="NSE",
            instrument="NSE:TCS-EQ",
            security_name="TCS",
            client_name="CLIENT B",
            side="sell",
            category="bulk",
            quantity=Decimal("10000.00"),
            price=Decimal("4000.00"),
        )
    )
    store.upsert(
        BlockDeal(
            trade_date=date(2026, 6, 2),
            exchange="NSE",
            instrument="NSE:INFY-EQ",
            security_name="INFOSYS",
            client_name="CLIENT C",
            side="buy",
            category="block",
            quantity=Decimal("50000.00"),
            price=Decimal("1800.25"),
        )
    )

    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
        block_deals=store,
    )
    try:
        assert exporter.export_block_deals(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted((tmp_path / "catalog" / "block_deals").glob("*.parquet"))
    assert len(files) == 2
    assert [f.name for f in files] == ["2026-06-01.parquet", "2026-06-02.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == [
        "trade_date",
        "exchange",
        "instrument",
        "security_name",
        "client_name",
        "side",
        "category",
        "quantity",
        "price",
        "remarks",
    ]
    assert table.schema.field("quantity").type == pa.decimal128(18, 4)
    assert table.schema.field("price").type == pa.decimal128(18, 4)

    rows = sorted(
        zip(
            table.column("instrument").to_pylist(),
            table.column("client_name").to_pylist(),
            table.column("side").to_pylist(),
            table.column("category").to_pylist(),
            table.column("quantity").to_pylist(),
            table.column("price").to_pylist(),
            table.column("remarks").to_pylist(),
            strict=False,
        )
    )
    assert rows == [
        (
            "NSE:RELIANCE-EQ",
            "CLIENT A",
            "BUY",
            "block",
            Decimal("250000.00"),
            Decimal("2500.50"),
            "Block deal window",
        ),
        (
            "NSE:TCS-EQ",
            "CLIENT B",
            "SELL",
            "bulk",
            Decimal("10000.00"),
            Decimal("4000.00"),
            None,
        ),
    ]
    assert all(d == date(2026, 6, 1) for d in table.column("trade_date").to_pylist())


def test_block_deals_missing_store_exports_nothing(tmp_path):
    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
    )
    assert exporter.export_block_deals(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "block_deals").exists()


def test_export_news_groups_by_published_date(tmp_path):
    store = DuckDbNewsStore(":memory:")
    store.upsert(
        NewsArticle(
            article_id="n1",
            title="Reliance board meeting",
            source="moneycontrol",
            url="https://example.com/reliance-board",
            published_at=datetime(2026, 1, 5, 10, 30, tzinfo=UTC),
            summary="Q3 update",
            instruments=("NSE:RELIANCE-EQ",),
        )
    )
    store.upsert(
        NewsArticle(
            article_id="n2",
            title="TCS results",
            source="moneycontrol",
            url="https://example.com/tcs-results",
            published_at=datetime(2026, 1, 6, 9, 0, tzinfo=UTC),
            summary=None,
            instruments=("NSE:TCS-EQ", "NSE:INFY-EQ"),
        )
    )

    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
        news=store,
    )
    try:
        assert exporter.export_news(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted((tmp_path / "catalog" / "news").glob("*.parquet"))
    assert [f.name for f in files] == ["2026-01-05.parquet", "2026-01-06.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == [
        "published_date",
        "published_at",
        "article_id",
        "title",
        "source",
        "url",
        "summary",
        "instruments",
    ]
    assert table.schema.field("published_date").type == pa.date32()
    assert table.schema.field("published_at").type == pa.string()
    assert table.schema.field("summary").type == pa.string()

    assert table.column("published_date").to_pylist() == [date(2026, 1, 5)]
    assert table.column("published_at").to_pylist() == ["2026-01-05T10:30:00+00:00"]
    assert table.column("article_id").to_pylist() == ["n1"]
    assert table.column("title").to_pylist() == ["Reliance board meeting"]
    assert table.column("source").to_pylist() == ["moneycontrol"]
    assert table.column("url").to_pylist() == ["https://example.com/reliance-board"]
    assert table.column("summary").to_pylist() == ["Q3 update"]
    assert table.column("instruments").to_pylist() == ["NSE:RELIANCE-EQ"]

    other = pq.read_table(files[1])
    assert other.column("summary").to_pylist() == [None]
    assert other.column("instruments").to_pylist() == ["NSE:TCS-EQ,NSE:INFY-EQ"]


def test_news_missing_store_exports_nothing(tmp_path):
    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
    )
    assert exporter.export_news(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "news").exists()


def test_export_institutional_flows_groups_by_trade_date(tmp_path):
    store = DuckDbInstitutionalFlowStore(":memory:")
    store.upsert(
        InstitutionalFlow(
            trade_date=date(2026, 7, 1),
            participant="FII",
            segment="cash",
            gross_buy=Decimal("12000000000.00"),
            gross_sell=Decimal("9000000000.00"),
            net=Decimal("3000000000.00"),
        )
    )
    store.upsert(
        InstitutionalFlow(
            trade_date=date(2026, 7, 1),
            participant="DII",
            segment="cash",
            gross_buy=Decimal("8000000000.00"),
            gross_sell=Decimal("11000000000.00"),
            net=Decimal("-3000000000.00"),
        )
    )
    store.upsert(
        InstitutionalFlow(
            trade_date=date(2026, 7, 2),
            participant="FII",
            segment="index_futures",
            gross_buy=Decimal("500000000.00"),
            gross_sell=Decimal("200000000.00"),
            net=Decimal("300000000.00"),
        )
    )

    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
        institutional_flows=store,
    )
    try:
        assert exporter.export_institutional_flows(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted((tmp_path / "catalog" / "institutional_flows").glob("*.parquet"))
    assert [f.name for f in files] == ["2026-07-01.parquet", "2026-07-02.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == [
        "trade_date",
        "participant",
        "segment",
        "gross_buy",
        "gross_sell",
        "net",
        "source",
    ]
    assert table.schema.field("trade_date").type == pa.date32()
    assert table.schema.field("gross_buy").type == pa.decimal128(18, 4)
    assert table.schema.field("gross_sell").type == pa.decimal128(18, 4)
    assert table.schema.field("net").type == pa.decimal128(18, 4)

    rows = sorted(
        zip(
            table.column("participant").to_pylist(),
            table.column("segment").to_pylist(),
            table.column("gross_buy").to_pylist(),
            table.column("gross_sell").to_pylist(),
            table.column("net").to_pylist(),
            table.column("source").to_pylist(),
            strict=False,
        )
    )
    assert rows == [
        (
            "DII",
            "cash",
            Decimal("8000000000.0000"),
            Decimal("11000000000.0000"),
            Decimal("-3000000000.0000"),
            "nse",
        ),
        (
            "FII",
            "cash",
            Decimal("12000000000.0000"),
            Decimal("9000000000.0000"),
            Decimal("3000000000.0000"),
            "nse",
        ),
    ]
    assert all(d == date(2026, 7, 1) for d in table.column("trade_date").to_pylist())


def test_institutional_flows_missing_store_exports_nothing(tmp_path):
    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
    )
    assert exporter.export_institutional_flows(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "institutional_flows").exists()


def test_export_delivery_groups_by_trade_date(tmp_path):
    store = DuckDbDeliveryStore(":memory:")
    store.upsert(
        DeliveryRecord(
            trade_date=date(2026, 7, 3),
            instrument="NSE:RELIANCE-EQ",
            traded_quantity=100000,
            deliverable_quantity=75000,
            delivery_pct=Decimal("75.00"),
        )
    )
    store.upsert(
        DeliveryRecord(
            trade_date=date(2026, 7, 3),
            instrument="NSE:TCS-EQ",
            traded_quantity=50000,
            deliverable_quantity=12500,
            delivery_pct=Decimal("25.00"),
        )
    )
    store.upsert(
        DeliveryRecord(
            trade_date=date(2026, 7, 4),
            instrument="NSE:INFY-EQ",
            traded_quantity=80000,
            deliverable_quantity=40000,
            delivery_pct=Decimal("50.00"),
        )
    )

    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
        delivery=store,
    )
    try:
        assert exporter.export_delivery(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted((tmp_path / "catalog" / "delivery").glob("*.parquet"))
    assert [f.name for f in files] == ["2026-07-03.parquet", "2026-07-04.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == [
        "trade_date",
        "exchange",
        "instrument",
        "traded_quantity",
        "deliverable_quantity",
        "delivery_pct",
    ]
    assert table.schema.field("trade_date").type == pa.date32()
    assert table.schema.field("traded_quantity").type == pa.int64()
    assert table.schema.field("deliverable_quantity").type == pa.int64()
    assert table.schema.field("delivery_pct").type == pa.decimal128(18, 4)

    rows = sorted(
        zip(
            table.column("exchange").to_pylist(),
            table.column("instrument").to_pylist(),
            table.column("traded_quantity").to_pylist(),
            table.column("deliverable_quantity").to_pylist(),
            table.column("delivery_pct").to_pylist(),
            strict=False,
        )
    )
    assert rows == [
        ("NSE", "NSE:RELIANCE-EQ", 100000, 75000, Decimal("75.0000")),
        ("NSE", "NSE:TCS-EQ", 50000, 12500, Decimal("25.0000")),
    ]
    assert all(d == date(2026, 7, 3) for d in table.column("trade_date").to_pylist())


def test_delivery_missing_store_exports_nothing(tmp_path):
    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
    )
    assert exporter.export_delivery(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "delivery").exists()


def test_export_economic_calendar_groups_by_date(tmp_path, monkeypatch):
    # DuckDB converts tz-aware datetimes to the session timezone on insert
    # into a naive TIMESTAMP column; pin TZ to UTC so the round-trip is
    # byte-for-byte identical regardless of the host machine's timezone.
    monkeypatch.setenv("TZ", "UTC")
    store = DuckDbCalendarStore(":memory:")
    store.upsert(
        CalendarEvent(
            instrument=None,
            scheduled_at=datetime(2026, 7, 5, 6, 0, tzinfo=UTC),
            country="IN",
            currency="INR",
            category="Economic",
            event_name="CPI Inflation YoY",
            impact_level="High",
            actual=Decimal("5.10"),
            forecast=Decimal("5.20"),
            prior=Decimal("4.90"),
            period_label="Jun",
        )
    )
    store.upsert(
        CalendarEvent(
            instrument=None,
            scheduled_at=datetime(2026, 7, 5, 11, 0, tzinfo=UTC),
            country="IN",
            currency="INR",
            category="IPO",
            event_name="Swasthik IPO Listing",
            impact_level="Medium",
            actual=None,
            forecast=None,
            prior=None,
            period_label=None,
        )
    )
    store.upsert(
        CalendarEvent(
            instrument="NSE:RELIANCE-EQ",
            scheduled_at=datetime(2026, 7, 6, 14, 30, tzinfo=UTC),
            country="IN",
            currency="INR",
            category="Earnings",
            event_name="Reliance Q1 Results",
            impact_level="High",
            actual=None,
            forecast=None,
            prior=None,
            period_label="Q1",
        )
    )

    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
        calendar=store,
    )
    try:
        assert exporter.export_economic_calendar(tmp_path / "catalog") == 2
    finally:
        store.close()

    files = sorted((tmp_path / "catalog" / "economic_calendar").glob("*.parquet"))
    assert [f.name for f in files] == ["2026-07-05.parquet", "2026-07-06.parquet"]

    table = pq.read_table(files[0])
    assert table.column_names == [
        "scheduled_date",
        "scheduled_at",
        "instrument",
        "country",
        "currency",
        "category",
        "event_name",
        "impact_level",
        "actual",
        "forecast",
        "prior",
        "period_label",
    ]
    assert table.schema.field("scheduled_date").type == pa.date32()
    assert table.schema.field("scheduled_at").type == pa.string()
    assert table.schema.field("instrument").type == pa.string()
    assert table.schema.field("actual").type == pa.decimal128(18, 4)
    assert table.schema.field("forecast").type == pa.decimal128(18, 4)
    assert table.schema.field("prior").type == pa.decimal128(18, 4)
    assert table.schema.field("period_label").type == pa.string()

    rows = sorted(
        zip(
            table.column("scheduled_at").to_pylist(),
            table.column("instrument").to_pylist(),
            table.column("category").to_pylist(),
            table.column("event_name").to_pylist(),
            table.column("impact_level").to_pylist(),
            table.column("actual").to_pylist(),
            table.column("forecast").to_pylist(),
            table.column("prior").to_pylist(),
            table.column("period_label").to_pylist(),
            strict=False,
        )
    )
    assert rows == [
        (
            "2026-07-05T06:00:00+00:00",
            None,
            "Economic",
            "CPI Inflation YoY",
            "High",
            Decimal("5.1000"),
            Decimal("5.2000"),
            Decimal("4.9000"),
            "Jun",
        ),
        (
            "2026-07-05T11:00:00+00:00",
            None,
            "IPO",
            "Swasthik IPO Listing",
            "Medium",
            None,
            None,
            None,
            None,
        ),
    ]
    assert all(d == date(2026, 7, 5) for d in table.column("scheduled_date").to_pylist())


def test_calendar_missing_store_exports_nothing(tmp_path):
    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"),
        DuckDbCorporateActionsStore(":memory:"),
    )
    assert exporter.export_economic_calendar(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "economic_calendar").exists()


def test_main_cli_exports_all_datasets(tmp_path):
    db = str(tmp_path / "b.duckdb")
    catalog = str(tmp_path / "catalog")

    filings = DuckDbFilingsStore(db)
    filings.upsert(
        Filing(
            instrument="NSE:RELIANCE-EQ",
            filing_date=date(2026, 5, 1),
            filing_type=FilingType.INSIDER_TRADE,
            title="Insider trade disclosure",
        )
    )
    filings.close()

    actions = DuckDbCorporateActionsStore(db)
    actions.add(
        Split(
            instrument="NSE:SBI-EQ",
            ex_date=date(2026, 5, 2),
            old_shares=1,
            new_shares=2,
        )
    )
    actions.close()

    deals = DuckDbBlockDealsStore(db)
    deals.upsert(
        BlockDeal(
            trade_date=date(2026, 5, 3),
            exchange="NSE",
            instrument="NSE:HDFC-EQ",
            security_name="HDFC",
            client_name="CLIENT D",
            side="BUY",
            category="block",
            quantity=Decimal("75000.00"),
            price=Decimal("1600.00"),
        )
    )
    deals.close()

    news = DuckDbNewsStore(db)
    news.upsert(
        NewsArticle(
            article_id="n3",
            title="Infosys results",
            source="moneycontrol",
            url="https://example.com/infy-results",
            published_at=datetime(2026, 5, 4, 9, 30, tzinfo=UTC),
        )
    )
    news.close()

    flows = DuckDbInstitutionalFlowStore(db)
    flows.upsert(
        InstitutionalFlow(
            trade_date=date(2026, 5, 5),
            participant="FII",
            segment="cash",
            gross_buy=Decimal("1000000000.00"),
            gross_sell=Decimal("800000000.00"),
            net=Decimal("200000000.00"),
        )
    )
    flows.close()

    delivery = DuckDbDeliveryStore(db)
    delivery.upsert(
        DeliveryRecord(
            trade_date=date(2026, 5, 6),
            instrument="NSE:RELIANCE-EQ",
            traded_quantity=120000,
            deliverable_quantity=96000,
            delivery_pct=Decimal("80.00"),
        )
    )
    delivery.close()

    calendar = DuckDbCalendarStore(db)
    calendar.upsert(
        CalendarEvent(
            instrument=None,
            scheduled_at=datetime(2026, 5, 7, 6, 0, tzinfo=UTC),
            country="IN",
            currency="INR",
            category="Economic",
            event_name="RBI Policy Rate Decision",
            impact_level="High",
            actual=None,
            forecast=Decimal("6.50"),
            prior=Decimal("6.50"),
            period_label="May",
        )
    )
    calendar.close()

    rc = main(["--dataset", "all", "--db", db, "--catalog", catalog])
    assert rc == 0
    assert (tmp_path / "catalog" / "filings" / "2026-05-01.parquet").exists()
    assert (tmp_path / "catalog" / "corporate_actions" / "2026-05-02.parquet").exists()
    assert (tmp_path / "catalog" / "block_deals" / "2026-05-03.parquet").exists()
    assert (tmp_path / "catalog" / "news" / "2026-05-04.parquet").exists()
    assert (tmp_path / "catalog" / "institutional_flows" / "2026-05-05.parquet").exists()
    assert (tmp_path / "catalog" / "delivery" / "2026-05-06.parquet").exists()
    assert (tmp_path / "catalog" / "economic_calendar" / "2026-05-07.parquet").exists()

    rc = main(["--dataset", "block-deals", "--db", db, "--catalog", catalog])
    assert rc == 0
    assert (tmp_path / "catalog" / "block_deals" / "2026-05-03.parquet").exists()

    rc = main(["--dataset", "institutional-flows", "--db", db, "--catalog", catalog])
    assert rc == 0
    assert (tmp_path / "catalog" / "institutional_flows" / "2026-05-05.parquet").exists()


def test_sequential_bridge_exports_preserve_both_datasets_on_main(tmp_path):
    """Regression test for gap 0027: sequential exports must keep all datasets on main."""
    filings = DuckDbFilingsStore(":memory:")
    filings.upsert(
        Filing(
            instrument="NSE:RELIANCE-EQ",
            filing_date=date(2026, 5, 1),
            filing_type=FilingType.CORPORATE_ACTION,
            title="Board meeting",
            source_url="https://example.com/board",
        )
    )
    delivery = DuckDbDeliveryStore(":memory:")
    delivery.upsert(
        DeliveryRecord(
            trade_date=date(2026, 5, 2),
            instrument="NSE:RELIANCE-EQ",
            traded_quantity=100000,
            deliverable_quantity=65000,
            delivery_pct=Decimal("65.00"),
        )
    )
    actions = DuckDbCorporateActionsStore(":memory:")
    manifest = ManifestStore(tmp_path / "catalog")
    exporter = LakehouseBridgeExporter(
        filings,
        actions,
        delivery=delivery,
        manifest=manifest,
    )
    try:
        assert exporter.export_filings(tmp_path / "catalog") == 1
        assert exporter.export_delivery(tmp_path / "catalog") == 1
        head_filings = manifest.resolve_partition("fundamentals", "filings")
        head_delivery = manifest.resolve_partition("disclosures", "delivery")
        assert len(head_filings) == 1
        assert len(head_delivery) == 1
        head = manifest.resolve_ref("main")
        assert head is not None
        assert len(manifest.entries_for_commit(head.commit_sha)) == 2
    finally:
        manifest.close()
        actions.close()
        delivery.close()
        filings.close()
