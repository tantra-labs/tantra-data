"""Tests for the lakehouse bridge exporter (gaps 0007/0008 Parquet projections)."""

from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.adapters.duckdb_store import DuckDbCorporateActionsStore
from tantra_data.adapters.filings_store import DuckDbFilingsStore
from tantra_data.cli.export_bridge import main
from tantra_data.domain.corporate_actions import Bonus, Dividend, Split, to_record
from tantra_data.domain.filings import Filing, FilingType
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

    exporter = LakehouseBridgeExporter(
        store, DuckDbCorporateActionsStore(":memory:")
    )
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
    assert all(
        d == date(2026, 1, 10)
        for d in table.column("filing_date").to_pylist()
    )


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

    exporter = LakehouseBridgeExporter(
        DuckDbFilingsStore(":memory:"), store
    )
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

    rc = main(["--dataset", "all", "--db", db, "--catalog", catalog])
    assert rc == 0
    assert (tmp_path / "catalog" / "filings" / "2026-05-01.parquet").exists()
    assert (
        tmp_path / "catalog" / "corporate_actions" / "2026-05-02.parquet"
    ).exists()
