"""Pinned, closed, Decimal-exact bar reader over the lakehouse v3 catalog.

Gap 0026 provider side. Resolves a ``ref`` once to a commit, reads bars at that
commit sha (reproducible), drops any bar whose IST session date is after the
commit's ``as_of_ist`` (no look-ahead), and bounds the row count.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl

from tantra_data.adapters.manifest_store import ManifestStore
from tantra_data.adapters.parquet_catalog import ParquetCatalog

_PRICE_COLUMNS = ("open", "high", "low", "close")


class LibraryBarsError(Exception):
    """Base error for the library bar reader."""


class UnknownRef(LibraryBarsError):
    """The ref does not resolve to a data commit."""


class BarLimitExceeded(LibraryBarsError):
    """The read would return more rows than ``max_rows``."""


class NonDecimalBars(LibraryBarsError):
    """A price column was not a Decimal dtype."""


@dataclass(frozen=True, slots=True)
class ClosedBar:
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True, slots=True)
class PinnedBars:
    instrument: str
    interval: str
    ref: str
    commit_sha: str
    as_of_ist: date
    bars: tuple[ClosedBar, ...]


def _as_utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


class LakehouseBarReader:
    def __init__(self, root: str | Path, *, max_rows: int = 200_000) -> None:
        self._root = Path(root)
        self._max_rows = max_rows

    def read_closed_bars(
        self,
        instrument: str,
        *,
        interval: str = "1d",
        ref: str = "main",
        start: date | None = None,
        end: date | None = None,
    ) -> PinnedBars:
        store = ManifestStore(self._root)
        try:
            commit = store.resolve_ref(ref)
        finally:
            store.close()
        if commit is None:
            raise UnknownRef(ref)

        df = ParquetCatalog(self._root).read_bars(
            instrument, start, end, interval, as_of_ref=commit.commit_sha
        )
        for col in _PRICE_COLUMNS:
            if not isinstance(df.schema[col], pl.Decimal):
                raise NonDecimalBars(col)

        # Closed bars / no look-ahead, filtered in Polars before any row is materialised:
        # keep bars whose IST session date is <= the pinned commit's as_of_ist (and <= end).
        ts_dtype = df.schema["ts"]
        ts_utc_expr = (
            pl.col("ts").dt.replace_time_zone("UTC")
            if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is None
            else pl.col("ts").dt.convert_time_zone("UTC")
        )
        cutoff = commit.as_of_ist if end is None else min(commit.as_of_ist, end)
        session = ts_utc_expr.dt.convert_time_zone("Asia/Kolkata").dt.date()
        df = df.filter(session <= cutoff)
        if df.height > self._max_rows:
            raise BarLimitExceeded(f"more than {self._max_rows} bars for {instrument}")

        bars: list[ClosedBar] = []
        for ts, o, h, lo, c, v in df.select(["ts", *_PRICE_COLUMNS, "volume"]).iter_rows():
            bars.append(ClosedBar(_as_utc(ts), o, h, lo, c, 0 if v is None else int(v)))
        return PinnedBars(
            instrument=instrument,
            interval=interval,
            ref=ref,
            commit_sha=commit.commit_sha,
            as_of_ist=commit.as_of_ist,
            bars=tuple(bars),
        )
