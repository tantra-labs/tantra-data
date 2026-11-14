"""Seed the Parquet catalog from NSE bhavcopy archives (gap 0004).

Backfills the catalog for a date range through tantra-data's own ingestion
pipeline: each trading day's bhavcopy is fetched from the NSE archive, parsed
into ``Decimal`` rows by :mod:`tantra_data.providers.bhavcopy`, and written as
canonical OHLCV bars into the ``ParquetCatalog`` root (``data/catalog`` by
default) consumed by tantra-runtime.

Closes gap ``0004``: ``data/catalog`` is seeded by tantra-data's own bhavcopy
ingestion pipeline instead of ad-hoc copies.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

from tantra_data.adapters.parquet_catalog import ParquetCatalog
from tantra_data.providers.bhavcopy import BhavcopyRow, fetch_bhavcopy, rows_to_bars


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill the Parquet catalog from NSE bhavcopy archives"
    )
    parser.add_argument(
        "--catalog",
        default="data/catalog",
        help="Path to Parquet catalog root directory (default: data/catalog)",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="Start date to backfill (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End date to backfill (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Parse bhavcopy files and report counts without writing bars",
    )
    args = parser.parse_args(argv)

    today = date.today()
    start = date.fromisoformat(args.start) if args.start else today
    end = date.fromisoformat(args.end) if args.end else today

    catalog = ParquetCatalog(args.catalog)
    total_bars = 0
    total_instruments = 0
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        date_iso = day.isoformat()
        try:
            rows = fetch_bhavcopy(date_iso)
        except Exception as exc:
            print(f"[{date_iso}] ERROR: {exc}", file=sys.stderr)
            return 1
        by_symbol: dict[str, list[BhavcopyRow]] = {}
        for row in rows:
            if row.series != "EQ":
                continue
            by_symbol.setdefault(row.symbol, []).append(row)
        if args.dry_run:
            print(f"[{date_iso}] dry-run: {len(by_symbol)} instruments, {len(rows)} rows")
            continue
        for symbol in sorted(by_symbol):
            bars = rows_to_bars(by_symbol[symbol])
            catalog.write_bars(f"NSE:{symbol}-EQ", bars)
        total_bars += sum(len(value) for value in by_symbol.values())
        total_instruments += len(by_symbol)
        print(f"[{date_iso}] wrote {len(by_symbol)} instruments ({len(rows)} rows)")
    if not args.dry_run and total_instruments:
        print(f"backfill complete: {total_bars} bars across {total_instruments} instruments")
    return 0


if __name__ == "__main__":
    sys.exit(main())
