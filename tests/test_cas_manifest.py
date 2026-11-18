"""Lakehouse v3 CAS object store + ``catalog.duckdb`` manifest (plan 0009 Phase B).

Network-free tests over tmp paths covering: content deduplication and
sort-key order in the CAS writer; atomic staging->main commit rollback on a
bad entry; ref/tag pinning with time-travel; GC reachability safety; alias
continuity across the ``ZOMATO`` -> ``ETERNAL`` rename; and the dual-read
``ParquetCatalog`` (manifest first, legacy fallback).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tantra_data.adapters.cas_store import (
    gc_unreachable,
    read_cas_object,
    write_cas_object,
)
from tantra_data.adapters.manifest_store import SCHEMA_VERSION, ManifestStore
from tantra_data.adapters.parquet_catalog import ParquetCatalog
from tantra_data.domain.lakehouse import (
    CommitRecord,
    InstrumentAlias,
    ManifestEntry,
)


def utc(y: int, m: int, d: int, hour: int = 0) -> datetime:
    return datetime(y, m, d, hour, tzinfo=UTC)


def make_entry(
    *,
    domain: str = "market",
    dataset: str = "bars",
    object_sha: str = "ab" + "b" * 62,
    min_ts: datetime | None = None,
    max_ts: datetime | None = None,
    row_count: int = 100,
    **overrides: Any,
) -> ManifestEntry:
    base: dict[str, Any] = {
        "domain": domain,
        "dataset": dataset,
        "exchange": "NSE",
        "asset_class": "EQ",
        "timeframe": "1d",
        "time_bucket": "year=2024",
        "object_sha": object_sha,
        "min_ts": min_ts or utc(2024, 1, 1),
        "max_ts": max_ts or utc(2024, 1, 31),
        "row_count": row_count,
        "sort_key": "(instrument_uid, ts)",
    }
    base.update(overrides)
    return ManifestEntry(**base)


def bars_df() -> pl.DataFrame:
    """A tiny multi-instrument bar frame with exact decimals."""
    return pl.DataFrame(
        {
            "instrument_uid": ["u1", "u1", "u2", "u2"],
            "ts": [
                utc(2024, 1, 1),
                utc(2024, 1, 2),
                utc(2024, 1, 1),
                utc(2024, 1, 2),
            ],
            "close": ["100.0000", "101.0000", "50.0000", "51.0000"],
        }
    ).with_columns(pl.col("close").cast(pl.Decimal(18, 4)))


# ---------------------------------------------------------------------------
# CAS object store
# ---------------------------------------------------------------------------


def test_cas_writer_deduplicates_identical_tables(tmp_path: Path) -> None:
    df = bars_df()
    sha1 = write_cas_object(df, sort_key=["instrument_uid", "ts"], root=tmp_path)
    sha2 = write_cas_object(df, sort_key=["instrument_uid", "ts"], root=tmp_path)
    assert sha1 == sha2
    assert len(sha1) == 64
    objects = list((tmp_path / "objects" / "sha256").rglob("*.parquet"))
    assert len(objects) == 1
    assert objects[0].name == f"{sha1}.parquet"


def test_cas_writer_preserves_sort_key_order_and_is_zstd(tmp_path: Path) -> None:
    # Unsorted input: (instrument_uid, ts) sort key must be enforced on disk.
    unsorted = pl.DataFrame(
        {
            "instrument_uid": ["u2", "u1", "u2", "u1"],
            "ts": [utc(2024, 1, 2), utc(2024, 1, 2), utc(2024, 1, 1), utc(2024, 1, 1)],
            "close": ["51.0000", "101.0000", "50.0000", "100.0000"],
        }
    ).with_columns(pl.col("close").cast(pl.Decimal(18, 4)))
    sha = write_cas_object(unsorted, sort_key=["instrument_uid", "ts"], root=tmp_path)
    out = read_cas_object(tmp_path, sha)
    assert out["instrument_uid"].to_list() == ["u1", "u1", "u2", "u2"]
    assert out["close"].to_list() == [
        Decimal("100.0000"),
        Decimal("101.0000"),
        Decimal("50.0000"),
        Decimal("51.0000"),
    ]

    path = tmp_path / "objects" / "sha256" / sha[:2] / f"{sha}.parquet"
    parquet = pq.ParquetFile(path)
    assert parquet.metadata.row_group(0).column(0).compression == "ZSTD"
    assert parquet.schema_arrow.field("close").type == pa.decimal128(18, 4)


def test_cas_writer_rejects_float_columns(tmp_path: Path) -> None:
    floaty = pl.DataFrame(
        {
            "instrument_uid": ["u1"],
            "ts": [utc(2024, 1, 1)],
            "close": [100.5],  # float — forbidden by the Zero-Floats policy
        }
    )
    with pytest.raises(ValueError, match="zero-floats"):
        write_cas_object(floaty, sort_key=["instrument_uid", "ts"], root=tmp_path)


def test_gc_unreachable_prunes_only_orphaned_blobs(tmp_path: Path) -> None:
    pinned = write_cas_object(bars_df(), sort_key=["instrument_uid", "ts"], root=tmp_path)
    orphan = pl.DataFrame(
        {
            "instrument_uid": ["u9", "u9"],
            "ts": [utc(2024, 1, 1), utc(2024, 1, 2)],
            "close": ["9.0000", "9.5000"],
        }
    ).with_columns(pl.col("close").cast(pl.Decimal(18, 4)))
    orphan_sha = write_cas_object(orphan, sort_key=["instrument_uid", "ts"], root=tmp_path)
    assert orphan_sha != pinned

    dry = gc_unreachable(tmp_path, {pinned}, dry_run=True)
    assert [p.stem for p in dry] == [orphan_sha]

    removed = gc_unreachable(tmp_path, {pinned}, dry_run=False)
    assert [p.stem for p in removed] == [orphan_sha]
    assert (tmp_path / "objects" / "sha256" / pinned[:2] / f"{pinned}.parquet").exists()
    assert not (tmp_path / "objects" / "sha256" / orphan_sha[:2] / f"{orphan_sha}.parquet").exists()


# ---------------------------------------------------------------------------
# Manifest / refs / tags
# ---------------------------------------------------------------------------


def test_commit_fast_forwards_main_and_records_entries(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    entry = make_entry()
    record = store.commit(
        "daily bars",
        as_of_ist=date(2024, 1, 1),
        schema_version=SCHEMA_VERSION,
        entries=[entry],
    )
    assert isinstance(record, CommitRecord)
    assert record.parent_sha is None
    assert store.resolve_ref("main") == record
    assert store.resolve_ref("refs/heads/main") == record
    # immutable commit & tree files on disk matching tree_hash
    assert (tmp_path / "commits" / f"{record.commit_sha}.json").exists()
    tree_path = tmp_path / "trees" / f"{record.tree_hash}.json"
    assert tree_path.exists()
    tree_data = json.loads(tree_path.read_text())
    assert len(tree_data) == 1
    assert tree_data[0]["object_sha"] == entry.object_sha
    computed_tree_hash = hashlib.sha256(
        json.dumps(tree_data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert computed_tree_hash == record.tree_hash

    parts = store.resolve_partition("market", "bars", exchange="NSE", timeframe="1d")
    assert [p.object_sha for p in parts] == [entry.object_sha]

    child = store.commit(
        "daily bars +1",
        as_of_ist=date(2024, 1, 2),
        schema_version=SCHEMA_VERSION,
        entries=[make_entry(min_ts=utc(2024, 2, 1), max_ts=utc(2024, 2, 28))],
        parent=record,
    )
    assert child.parent_sha == record.commit_sha
    assert store.resolve_ref("main") == child
    store.close()


def test_commit_rolls_back_on_float_entry(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    good = store.commit(
        "baseline",
        as_of_ist=date(2024, 1, 1),
        schema_version=SCHEMA_VERSION,
        entries=[make_entry(object_sha="c" * 64)],
    )
    commits_before = list((tmp_path / "commits").glob("*.json"))

    with pytest.raises(ValueError, match="zero-floats"):
        store.commit(
            "bad float",
            as_of_ist=date(2024, 1, 2),
            schema_version=SCHEMA_VERSION,
            entries=[make_entry(object_sha="d" * 64, row_count=100.5)],  # type: ignore[arg-type]
        )
    # main unchanged, no new commit file, and the tree is still the baseline's
    assert store.resolve_ref("main") == good
    assert list((tmp_path / "commits").glob("*.json")) == commits_before
    baseline_entries = store.resolve_partition("market", "bars", exchange="NSE")
    assert [e.object_sha for e in baseline_entries] == ["c" * 64]
    store.close()


def test_commit_rolls_back_on_non_monotonic_entries(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    good = store.commit(
        "baseline",
        as_of_ist=date(2024, 1, 1),
        schema_version=SCHEMA_VERSION,
        entries=[make_entry(object_sha="c" * 64)],
    )
    bad_entries = [
        make_entry(object_sha="d" * 64, min_ts=utc(2024, 2, 1), max_ts=utc(2024, 2, 28)),
        make_entry(object_sha="e" * 64, min_ts=utc(2024, 1, 15), max_ts=utc(2024, 1, 20)),
    ]
    with pytest.raises(ValueError, match="non-monotonic"):
        store.commit(
            "bad order",
            as_of_ist=date(2024, 1, 2),
            schema_version=SCHEMA_VERSION,
            entries=bad_entries,
        )
    assert store.resolve_ref("main") == good
    store.close()


def test_tag_pinning_survives_later_commits(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    first = store.commit(
        "first",
        as_of_ist=date(2024, 1, 1),
        schema_version=SCHEMA_VERSION,
        entries=[make_entry(object_sha="c" * 64)],
    )
    store.tag("backtest-2026-11-17")

    second = store.commit(
        "second",
        as_of_ist=date(2024, 1, 2),
        schema_version=SCHEMA_VERSION,
        entries=[make_entry(object_sha="d" * 64)],
        parent=first,
    )
    assert store.resolve_ref("main") == second
    # the tag still resolves to the pinned commit
    assert store.resolve_ref("refs/tags/backtest-2026-11-17") == first
    assert store.resolve_ref("backtest-2026-11-17") == first
    # time-travel partition read through the tag sees the first tree
    parts = store.resolve_partition(
        "market", "bars", exchange="NSE", ref="refs/tags/backtest-2026-11-17"
    )
    assert [p.object_sha for p in parts] == ["c" * 64]
    store.close()


def test_unknown_ref_resolves_to_none(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    assert store.resolve_ref("main") is None
    assert store.resolve_ref("refs/tags/nope") is None
    store.close()


# ---------------------------------------------------------------------------
# Alias continuity (ZOMATO -> ETERNAL)
# ---------------------------------------------------------------------------


def test_alias_continuity_zomato_to_eternal(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    uid = "INE999Z01019"
    store.write_alias(
        InstrumentAlias(
            exchange="NSE",
            symbol="ZOMATO",
            series="EQ",
            valid_from=date(2021, 7, 23),
            valid_to=date(2024, 9, 13),
            instrument_uid=uid,
        )
    )
    store.write_alias(
        InstrumentAlias(
            exchange="NSE",
            symbol="ETERNAL",
            series="EQ",
            valid_from=date(2024, 9, 14),
            valid_to=None,
            instrument_uid=uid,
        )
    )
    # old ticker resolves up to its last valid day, new ticker from its first
    assert store.resolve_alias("NSE", "ZOMATO", as_of=date(2024, 9, 13)) == uid
    assert store.resolve_alias("NSE", "ETERNAL", as_of=date(2024, 9, 14)) == uid
    assert store.resolve_alias("NSE", "ZOMATO", as_of=date(2024, 9, 14)) is None
    assert store.resolve_alias("NSE", "ETERNAL", as_of=date(2024, 9, 13)) is None
    # live (no as_of) resolution returns the current ticker's identity
    assert store.resolve_alias("NSE", "ETERNAL") == uid
    assert store.resolve_alias("NSE", "ZOMATO") == uid
    store.close()


# ---------------------------------------------------------------------------
# Dual-read ParquetCatalog
# ---------------------------------------------------------------------------


def test_dual_read_manifest_bars_match_legacy(tmp_path: Path) -> None:
    catalog_root = tmp_path / "catalog"
    catalog = ParquetCatalog(catalog_root)
    uid = "INE002A01018"
    other = "INE009A01021"
    df = pl.DataFrame(
        {
            "instrument_uid": [uid, uid, other],
            "ts": [utc(2024, 1, 1), utc(2024, 1, 2), utc(2024, 1, 1)],
            "open": ["100.0000", "101.0000", "50.0000"],
            "high": ["105.0000", "106.0000", "52.0000"],
            "low": ["99.0000", "100.0000", "49.0000"],
            "close": ["104.0000", "105.5000", "51.0000"],
            "volume": [1_000_000, 1_200_000, 500_000],
        }
    ).with_columns(
        pl.col("open").cast(pl.Decimal(18, 4)),
        pl.col("high").cast(pl.Decimal(18, 4)),
        pl.col("low").cast(pl.Decimal(18, 4)),
        pl.col("close").cast(pl.Decimal(18, 4)),
    )
    # Legacy write path first.
    legacy_bars = df.filter(pl.col("instrument_uid") == uid).drop("instrument_uid")
    catalog.write_bars("NSE:RELIANCE-EQ", legacy_bars)
    expected = catalog.read_bars("NSE:RELIANCE-EQ")

    # Now build a v3 manifest with a CAS object carrying the same bars.
    store = ManifestStore(catalog_root)
    store.write_alias(
        InstrumentAlias(
            exchange="NSE",
            symbol="RELIANCE",
            series="EQ",
            valid_from=date(2020, 1, 1),
            valid_to=None,
            instrument_uid=uid,
        )
    )
    sha = write_cas_object(df, sort_key=["instrument_uid", "ts"], root=catalog_root)
    store.commit(
        "bars 2024",
        as_of_ist=date(2024, 1, 2),
        schema_version=SCHEMA_VERSION,
        entries=[
            ManifestEntry(
                domain="market",
                dataset="bars",
                exchange="NSE",
                asset_class="EQ",
                timeframe="1d",
                time_bucket="year=2024",
                object_sha=sha,
                min_ts=utc(2024, 1, 1),
                max_ts=utc(2024, 1, 2),
                row_count=df.height,
                sort_key="(instrument_uid, ts)",
            )
        ],
    )
    via_manifest = catalog.read_bars("NSE:RELIANCE-EQ")
    assert via_manifest.height == expected.height
    assert via_manifest["close"].to_list() == expected["close"].to_list()
    assert via_manifest["close"].to_list() == [
        Decimal("104.0000"),
        Decimal("105.5000"),
    ]
    assert via_manifest["ts"].to_list() == [utc(2024, 1, 1), utc(2024, 1, 2)]
    windowed = catalog.read_bars("NSE:RELIANCE-EQ", start=date(2024, 1, 2), end=date(2024, 1, 2))
    assert windowed.height == 1
    assert windowed["close"].to_list() == [Decimal("105.5000")]
    store.close()


def test_dual_read_legacy_fallback_without_manifest(
    catalog: ParquetCatalog, sample_bars: pl.DataFrame
) -> None:
    catalog.write_bars("NSE:RELIANCE-EQ", sample_bars)
    bars = catalog.read_bars("NSE:RELIANCE-EQ")
    assert bars.height == 4
    assert bars["close"].to_list() == [
        Decimal("1250.0000"),
        Decimal("1260.0000"),
        Decimal("1270.0000"),
        Decimal("1280.0000"),
    ]


def test_dual_read_falls_back_when_manifest_has_no_bars(tmp_path: Path) -> None:
    catalog_root = tmp_path / "catalog"
    catalog = ParquetCatalog(catalog_root)
    df = bars_df().rename({"close": "open"})
    df = df.with_columns(
        pl.col("open").alias("high"),
        pl.col("open").alias("low"),
        pl.col("open").alias("close"),
        pl.lit(1_000_000).alias("volume"),
    ).select(["instrument_uid", "ts", "open", "high", "low", "close", "volume"])
    legacy = df.filter(pl.col("instrument_uid") == "u1").drop("instrument_uid")
    catalog.write_bars("NSE:U1-EQ", legacy)

    # Manifest exists but has no alias -> must fall back to legacy Hive files.
    store = ManifestStore(catalog_root)
    store.commit(
        "empty",
        as_of_ist=date(2024, 1, 1),
        schema_version=SCHEMA_VERSION,
        entries=[],
    )
    bars = catalog.read_bars("NSE:U1-EQ")
    assert bars.height == 2
    assert bars["close"].to_list() == [Decimal("100.0000"), Decimal("101.0000")]
    store.close()
