"""Backfill CLI for the Parquet catalog from NSE bhavcopy (gap 0004)."""

from __future__ import annotations

from decimal import Decimal

from tantra_data.adapters.parquet_catalog import ParquetCatalog
from tantra_data.cli import ingest_catalog
from tantra_data.providers.bhavcopy import BhavcopyRow, parse_bhavcopy

LIVE_SAMPLE = (
    "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, CLOSE_PRICE,"
    " AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER\n"
    "20MICRONS, EQ, 06-Nov-2026, 180.26, 181.00, 185.81, 177.00, 183.00, 183.22, 183.19, 82090,"
    " 150.38, 2891, 39593, 48.23\n"
    "21STCENMGM, BE, 06-Nov-2026, 25.00, 25.50, 26.00, 24.80, 25.75, 25.70, 25.60, 1000, 2.56,"
    " 10, -, -\n"
)


def _fake_fetch(_date_text: str) -> list[BhavcopyRow]:
    return parse_bhavcopy(LIVE_SAMPLE)


def test_ingest_catalog_dry_run_reports_count(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(ingest_catalog, "fetch_bhavcopy", _fake_fetch)
    code = ingest_catalog.main(["--dry-run", "--catalog", str(tmp_path)])
    assert code == 0
    assert list(tmp_path.rglob("*.parquet")) == []


def test_ingest_catalog_writes_bars(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(ingest_catalog, "fetch_bhavcopy", _fake_fetch)
    catalog_dir = tmp_path / "catalog"
    code = ingest_catalog.main(
        ["--start", "2026-11-06", "--end", "2026-11-06", "--catalog", str(catalog_dir)]
    )
    assert code == 0
    bars = ParquetCatalog(catalog_dir).read_bars("NSE:20MICRONS-EQ")
    assert not bars.is_empty()
    assert bars["close"].to_list() == [Decimal("183.2200")]


def test_ingest_catalog_missing_file_fails(monkeypatch, tmp_path) -> None:
    def boom(date_text: str) -> list[BhavcopyRow]:
        raise RuntimeError(f"no bhavcopy for {date_text}")

    monkeypatch.setattr(ingest_catalog, "fetch_bhavcopy", boom)
    code = ingest_catalog.main(["--dry-run", "--catalog", str(tmp_path)])
    assert code == 1
