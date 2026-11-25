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


# ---------------------------------------------------------------------------
# Snapshot commit semantics & key-based merge (gap 0027 closure)
# ---------------------------------------------------------------------------


def test_sequential_dataset_commits_preserve_other_datasets_on_main(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    delivery = make_entry(
        domain="disclosures",
        dataset="delivery",
        exchange=None,
        asset_class=None,
        timeframe=None,
        time_bucket="year=2026",
        object_sha="1" * 64,
        min_ts=utc(2026, 1, 1),
        max_ts=utc(2026, 1, 10),
    )
    fundamentals = make_entry(
        domain="fundamentals",
        dataset="filings",
        exchange=None,
        asset_class=None,
        timeframe=None,
        time_bucket="year=2026",
        object_sha="2" * 64,
        min_ts=utc(2026, 1, 2),
        max_ts=utc(2026, 1, 15),
    )
    c1 = store.commit(
        "export delivery",
        as_of_ist=date(2026, 1, 10),
        schema_version=SCHEMA_VERSION,
        entries=[delivery],
    )
    c2 = store.commit(
        "export filings",
        as_of_ist=date(2026, 1, 15),
        schema_version=SCHEMA_VERSION,
        entries=[fundamentals],
        parent=c1,
    )
    assert [e.object_sha for e in store.resolve_partition("disclosures", "delivery")] == ["1" * 64]
    assert [e.object_sha for e in store.resolve_partition("fundamentals", "filings")] == ["2" * 64]

    tree_data = json.loads((tmp_path / "trees" / f"{c2.tree_hash}.json").read_text())
    assert len(tree_data) == 2
    assert {row["dataset"] for row in tree_data} == {"delivery", "filings"}
    store.close()


def test_commit_entries_replace_only_writes_replacement_tree(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    first = store.commit(
        "first",
        as_of_ist=date(2026, 1, 1),
        schema_version=SCHEMA_VERSION,
        entries=[
            make_entry(
                domain="disclosures",
                dataset="delivery",
                exchange=None,
                asset_class=None,
                timeframe=None,
                object_sha="a" * 64,
            )
        ],
    )
    second = store.commit(
        "replace all",
        as_of_ist=date(2026, 1, 2),
        schema_version=SCHEMA_VERSION,
        entries=[
            make_entry(
                domain="fundamentals",
                dataset="filings",
                exchange=None,
                asset_class=None,
                timeframe=None,
                object_sha="b" * 64,
            )
        ],
        parent=first,
        entries_replace_only=True,
    )
    assert store.resolve_partition("disclosures", "delivery") == []
    assert [e.object_sha for e in store.resolve_partition("fundamentals", "filings")] == ["b" * 64]
    tree_data = json.loads((tmp_path / "trees" / f"{second.tree_hash}.json").read_text())
    assert len(tree_data) == 1
    store.close()


def test_golden_snapshot_tree_json_vector(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    e1 = make_entry(
        domain="market",
        dataset="bars",
        exchange="NSE",
        asset_class="EQ",
        timeframe="1d",
        time_bucket="year=2025",
        object_sha="1" * 64,
        min_ts=utc(2025, 1, 1),
        max_ts=utc(2025, 12, 31),
        row_count=250,
    )
    e2 = make_entry(
        domain="fundamentals",
        dataset="filings",
        exchange=None,
        asset_class=None,
        timeframe=None,
        time_bucket="year=2026",
        object_sha="2" * 64,
        min_ts=utc(2026, 1, 1),
        max_ts=utc(2026, 1, 5),
        row_count=12,
    )
    c1 = store.commit(
        "bars 2025",
        as_of_ist=date(2025, 12, 31),
        schema_version=SCHEMA_VERSION,
        entries=[e1],
    )
    c2 = store.commit(
        "filings 2026",
        as_of_ist=date(2026, 1, 5),
        schema_version=SCHEMA_VERSION,
        entries=[e2],
        parent=c1,
    )
    expected_tree = [
        {
            "asset_class": "EQ",
            "dataset": "bars",
            "domain": "market",
            "exchange": "NSE",
            "max_ts": "2025-12-31T00:00:00+00:00",
            "min_ts": "2025-01-01T00:00:00+00:00",
            "object_sha": "1" * 64,
            "row_count": 250,
            "sort_key": "(instrument_uid, ts)",
            "time_bucket": "year=2025",
            "timeframe": "1d",
        },
        {
            "asset_class": None,
            "dataset": "filings",
            "domain": "fundamentals",
            "exchange": None,
            "max_ts": "2026-01-05T00:00:00+00:00",
            "min_ts": "2026-01-01T00:00:00+00:00",
            "object_sha": "2" * 64,
            "row_count": 12,
            "sort_key": "(instrument_uid, ts)",
            "time_bucket": "year=2026",
            "timeframe": None,
        },
    ]
    expected_canonical = json.dumps(expected_tree, sort_keys=True, separators=(",", ":"))
    expected_hash = hashlib.sha256(expected_canonical.encode("utf-8")).hexdigest()
    assert c2.tree_hash == expected_hash
    assert (tmp_path / "trees" / f"{c2.tree_hash}.json").read_text().strip() == expected_canonical
    store.close()


def test_hypothesis_snapshot_merge_idempotence_and_replacement(tmp_path: Path) -> None:
    from hypothesis import given, settings
    from hypothesis import strategies as st

    datasets = ["bars", "delivery", "filings", "news", "block_deals"]
    entry_strat = st.tuples(
        st.sampled_from(datasets),
        st.sampled_from(["year=2025", "year=2026"]),
        st.integers(min_value=1, max_value=20),
    )

    @given(
        batch1=st.lists(entry_strat, min_size=1, max_size=5),
        batch2=st.lists(entry_strat, min_size=1, max_size=5),
    )
    @settings(max_examples=20)
    def check(batch1: list[tuple[str, str, int]], batch2: list[tuple[str, str, int]]) -> None:
        sub = tmp_path / hashlib.sha256(repr((batch1, batch2)).encode()).hexdigest()[:12]
        store = ManifestStore(sub)
        try:

            def build_entries(batch: list[tuple[str, str, int]], tag: str) -> list[ManifestEntry]:
                dedup: dict[tuple[str, str], ManifestEntry] = {}
                for ds, bucket, day in batch:
                    sha = hashlib.sha256(f"{ds}:{bucket}:{day}:{tag}".encode()).hexdigest()
                    dedup[(ds, bucket)] = make_entry(
                        domain="disclosures"
                        if ds in ("delivery", "news", "block_deals")
                        else "market",
                        dataset=ds,
                        exchange=None,
                        asset_class=None,
                        timeframe=None,
                        time_bucket=bucket,
                        object_sha=sha,
                        min_ts=utc(2026, 1, day),
                        max_ts=utc(2026, 1, day + 1),
                    )
                return sorted(dedup.values(), key=lambda e: (e.min_ts, e.max_ts))

            e1 = build_entries(batch1, "v1")
            c1 = store.commit(
                "b1", as_of_ist=date(2026, 1, 20), schema_version=SCHEMA_VERSION, entries=e1
            )
            e2 = build_entries(batch2, "v2")
            c2 = store.commit(
                "b2",
                as_of_ist=date(2026, 1, 21),
                schema_version=SCHEMA_VERSION,
                entries=e2,
                parent=c1,
            )
            # Re-committing e2 onto c2 must keep the exact same tree_hash (idempotence).
            c3 = store.commit(
                "b2-idem",
                as_of_ist=date(2026, 1, 21),
                schema_version=SCHEMA_VERSION,
                entries=e2,
                parent=c2,
            )
            assert c3.tree_hash == c2.tree_hash

            expected_keys = {(e.domain, e.dataset, e.time_bucket): e.object_sha for e in e1}
            for e in e2:
                expected_keys[(e.domain, e.dataset, e.time_bucket)] = e.object_sha
            actual_tree = store.entries_for_commit(c2.commit_sha)
            assert {
                (e.domain, e.dataset, e.time_bucket): e.object_sha for e in actual_tree
            } == expected_keys
        finally:
            store.close()

    check()
