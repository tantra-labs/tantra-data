"""Import NSE daily bhavcopy archives into the Lakehouse v3 bars layout."""

from __future__ import annotations

import argparse
import io
import lzma
import os
import re
import sys
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from tantra_data.adapters.lakehouse_v3 import write_aliases, write_daily_bars
from tantra_data.providers.bhavcopy_archive import (
    BhavcopyArchiveError,
    Dialect,
    isin_index_from_frames,
    merge_sessions,
    read_archive_file,
    resolve_missing_isins,
)

_NAME_RE = re.compile(r"^bhavcopy_(\d{4})(\d{2})(\d{2})\.csv\.xz$")
_UDIFF_CUTOFF = date(2024, 7, 8)
_MON = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


class SessionNotPublished(Exception):
    """The exchange has no archive for the day (holiday / HTTP 404)."""


@dataclass
class DownloadReport:
    downloaded: int = 0
    holidays: int = 0
    skipped_existing: int = 0
    failures: list[tuple[date, str]] = field(default_factory=list)


def session_url(day: date) -> str:
    ymd = day.strftime("%Y%m%d")
    if day >= _UDIFF_CUTOFF:
        return f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip"
    mon = _MON[day.month - 1]
    return f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{day.year}/{mon}/cm{day.day:02d}{mon}{day.year}bhav.csv.zip"


def default_fetch(url: str) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data: bytes = resp.read()
            return data
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise SessionNotPublished(url) from exc
        raise


def download_missing_sessions(
    cache_dir: Path,
    start: date,
    end: date,
    *,
    workers: int = 4,
    fetch: Callable[[str], bytes] = default_fetch,
) -> DownloadReport:
    """Download weekday sessions missing from the cache as ``.csv.xz`` files."""
    report = DownloadReport()
    cache_dir.mkdir(parents=True, exist_ok=True)
    todo: list[date] = []
    day = start
    while day <= end:
        if day.weekday() < 5:
            if (cache_dir / f"bhavcopy_{day:%Y%m%d}.csv.xz").exists():
                report.skipped_existing += 1
            else:
                todo.append(day)
        day += timedelta(days=1)

    def one(d: date) -> None:
        raw = fetch(session_url(d))
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_bytes = zf.read(zf.namelist()[0])
        target = cache_dir / f"bhavcopy_{d:%Y%m%d}.csv.xz"
        tmp = target.with_name(target.name + ".tmp")
        tmp.write_bytes(lzma.compress(csv_bytes))
        os.replace(tmp, target)

    with ThreadPoolExecutor(max_workers=max(1, min(workers, 8))) as pool:
        futures = [(d, pool.submit(one, d)) for d in todo]
        for d, fut in futures:
            try:
                fut.result()
                report.downloaded += 1
            except SessionNotPublished:
                report.holidays += 1
            except Exception as exc:
                report.failures.append((d, f"{type(exc).__name__}: {exc}"))
    return report


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ingest-bars-v3", description=__doc__)
    ap.add_argument("--cache-dir", type=Path, default=Path("data/cache/bhavcopy"))
    ap.add_argument("--root", type=Path, default=Path("data"))
    ap.add_argument("--download", action="store_true")
    ap.add_argument("--start", type=_parse_date)
    ap.add_argument("--end", type=_parse_date)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    exit_code = 0

    if args.download:
        if args.start is None or args.end is None:
            ap.error("--download requires --start and --end")
        rep = download_missing_sessions(
            args.cache_dir, args.start, args.end, workers=min(args.workers, 8)
        )
        print(
            f"download: {rep.downloaded} downloaded, {rep.holidays} holidays, "
            f"{rep.skipped_existing} already cached, {len(rep.failures)} failures"
        )
        for d, msg in rep.failures:
            print(f"download failure {d}: {msg}", file=sys.stderr)
            exit_code = 1

    files = sorted(p for p in args.cache_dir.glob("bhavcopy_*.csv.xz") if _NAME_RE.match(p.name))
    parse_errors = 0

    # Pass 1: cheap ISIN index from ISIN-bearing dialects.
    def isin_frames() -> Iterator[pl.DataFrame]:
        nonlocal parse_errors
        for p in files:
            try:
                dialect, df = read_archive_file(p)
            except BhavcopyArchiveError as exc:
                print(f"error: {exc}", file=sys.stderr)
                parse_errors += 1
                continue
            if dialect is not Dialect.SEC_BHAVDATA:
                yield df.select("symbol", "trade_date", "isin")

    index = isin_index_from_frames(isin_frames())

    # Pass 2: per filename-year parse, resolve, merge, write.
    by_year: dict[str, list[Path]] = {}
    for p in files:
        m = _NAME_RE.match(p.name)
        assert m is not None
        by_year.setdefault(m.group(1), []).append(p)

    dialect_counts: Counter[str] = Counter()
    rows_written: dict[int, int] = {}
    uids: set[str] = set()
    dropped_total = 0
    alias_parts: list[pl.DataFrame] = []
    for year in sorted(by_year):
        parsed: list[tuple[Dialect, pl.DataFrame]] = []
        for p in by_year[year]:
            try:
                dialect, df = read_archive_file(p)
            except BhavcopyArchiveError as exc:
                print(f"error: {exc}", file=sys.stderr)
                parse_errors += 1
                continue
            dialect_counts[dialect.name.lower()] += 1
            df, dropped = resolve_missing_isins(df, index)
            dropped_total += dropped
            parsed.append((dialect, df))
        merged = merge_sessions(parsed)
        del parsed
        if merged.height == 0:
            continue
        bars = merged.rename({"isin": "instrument_uid"})
        uids.update(bars["instrument_uid"].unique().to_list())
        alias_parts.append(
            bars.group_by("symbol", "series", "instrument_uid").agg(
                pl.col("trade_date").min().alias("valid_from"),
                (pl.col("trade_date").max() + pl.duration(days=1)).alias("valid_to"),
            )
        )
        if args.dry_run:
            for y, n in bars.group_by(pl.col("trade_date").dt.year().alias("y")).len().iter_rows():
                rows_written[y] = rows_written.get(y, 0) + n
        else:
            rows_written.update(write_daily_bars(args.root, bars))
        del bars, merged

    alias_rows = 0
    if alias_parts:
        aliases = pl.concat(alias_parts)
        if args.dry_run:
            alias_rows = aliases.select("symbol", "series", "instrument_uid").unique().height
        else:
            alias_rows = write_aliases(args.root, aliases)

    mode = "dry-run (nothing written)" if args.dry_run else "written"
    print(f"files read per dialect: {dict(dialect_counts)}")
    print(f"rows {mode} per year: {dict(sorted(rows_written.items()))}")
    print(f"distinct instrument_uids: {len(uids)}")
    print(f"ISIN-unresolved rows dropped: {dropped_total}")
    print(f"alias rows: {alias_rows}")
    if parse_errors:
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
