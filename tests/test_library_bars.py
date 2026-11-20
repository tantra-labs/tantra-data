"""LakehouseBarReader: pinned, closed, Decimal bars from a tmp v3 CAS fixture."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from tantra_data.adapters.cas_store import write_cas_object
from tantra_data.adapters.manifest_store import SCHEMA_VERSION, ManifestStore
from tantra_data.domain.lakehouse import CommitRecord, InstrumentAlias, ManifestEntry
from tantra_data.service.library_bars import (
    BarLimitExceeded,
    LakehouseBarReader,
    UnknownRef,
)

UID = "INE002A01018"
INSTRUMENT = "NSE:RELIANCE-EQ"


def utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=UTC)


def _frame(days: list[int]) -> pl.DataFrame:
    n = len(days)
    return pl.DataFrame(
        {
            "instrument_uid": [UID] * n,
            "ts": [utc(2024, 1, d) for d in days],
            "open": [f"{100 + d}.0000" for d in days],
            "high": [f"{105 + d}.0000" for d in days],
            "low": [f"{99 + d}.0000" for d in days],
            "close": [f"{104 + d}.5000" for d in days],
            "volume": [1000 * d for d in days],
        }
    ).with_columns(pl.col(c).cast(pl.Decimal(18, 4)) for c in ("open", "high", "low", "close"))


def _commit(
    store: ManifestStore,
    root: Path,
    days: list[int],
    as_of: date,
    parent: CommitRecord | None,
) -> CommitRecord:
    df = _frame(days)
    sha = write_cas_object(df, sort_key=["instrument_uid", "ts"], root=root)
    return store.commit(
        f"bars to {as_of}",
        as_of_ist=as_of,
        schema_version=SCHEMA_VERSION,
        parent=parent,
        entries=[
            ManifestEntry(
                domain="market",
                dataset="bars",
                exchange="NSE",
                asset_class="EQ",
                timeframe="1d",
                time_bucket="year=2024",
                object_sha=sha,
                min_ts=utc(2024, 1, days[0]),
                max_ts=utc(2024, 1, days[-1]),
                row_count=df.height,
                sort_key="(instrument_uid, ts)",
            )
        ],
    )


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    store = ManifestStore(tmp_path)
    store.write_alias(
        InstrumentAlias(
            exchange="NSE",
            symbol="RELIANCE",
            series="EQ",
            valid_from=date(2020, 1, 1),
            valid_to=None,
            instrument_uid=UID,
        )
    )
    first = _commit(store, tmp_path, [1, 2], date(2024, 1, 2), None)
    store.tag("pin-1")
    # Later commit carries bars 1..5 but only 3 are "closed" as of 2024-01-03.
    _commit(store, tmp_path, [1, 2, 3, 4, 5], date(2024, 1, 3), first)
    store.close()
    return tmp_path


def test_reads_decimal_bars_at_main_excluding_lookahead(lake: Path) -> None:
    out = LakehouseBarReader(lake).read_closed_bars(INSTRUMENT)
    assert out.ref == "main"
    assert out.as_of_ist == date(2024, 1, 3)
    assert [b.ts for b in out.bars] == [utc(2024, 1, 1), utc(2024, 1, 2), utc(2024, 1, 3)]
    assert out.bars[0].close == Decimal("105.5000")
    assert out.bars[2].volume == 3000
    for b in out.bars:
        for v in (b.open, b.high, b.low, b.close):
            assert isinstance(v, Decimal)


def test_tag_pins_older_commit(lake: Path) -> None:
    reader = LakehouseBarReader(lake)
    pinned = reader.read_closed_bars(INSTRUMENT, ref="pin-1")
    assert [b.ts for b in pinned.bars] == [utc(2024, 1, 1), utc(2024, 1, 2)]
    main = reader.read_closed_bars(INSTRUMENT)
    assert pinned.commit_sha != main.commit_sha


def test_commit_sha_reproduces_identical_bars(lake: Path) -> None:
    reader = LakehouseBarReader(lake)
    first = reader.read_closed_bars(INSTRUMENT)
    again = reader.read_closed_bars(INSTRUMENT, ref=first.commit_sha)
    assert again.bars == first.bars
    assert again.commit_sha == first.commit_sha
    assert again.ref == first.commit_sha


def test_end_clamps(lake: Path) -> None:
    out = LakehouseBarReader(lake).read_closed_bars(INSTRUMENT, end=date(2024, 1, 2))
    assert len(out.bars) == 2


def test_unknown_ref(lake: Path) -> None:
    with pytest.raises(UnknownRef):
        LakehouseBarReader(lake).read_closed_bars(INSTRUMENT, ref="nope")


def test_bar_limit_exceeded(lake: Path) -> None:
    with pytest.raises(BarLimitExceeded):
        LakehouseBarReader(lake, max_rows=2).read_closed_bars(INSTRUMENT)


def test_unknown_instrument_is_empty(lake: Path) -> None:
    out = LakehouseBarReader(lake).read_closed_bars("NSE:NOPE-EQ")
    assert out.bars == ()
