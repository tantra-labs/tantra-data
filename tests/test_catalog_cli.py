"""``tantra-catalog`` CLI: status/log/tag/verify/compact/gc/views/migrate-v3 (plan 0009 Phase C).

Network-free tests over tmp paths. They call ``tantra_data.cli.catalog.main``
directly (matching the repo's other CLI tests) and assert on return codes,
stdout, on-disk refs/commits/objects, and exact ``Decimal`` parity for
``migrate-v3``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.adapters.cas_store import object_path, read_cas_object, write_cas_object
from tantra_data.adapters.manifest_store import SCHEMA_VERSION, ManifestStore
from tantra_data.adapters.parquet_catalog import ParquetCatalog
from tantra_data.cli import catalog as cli
from tantra_data.domain.lakehouse import ManifestEntry


def utc(y: int, m: int, d: int, hour: int = 0) -> datetime:
    return datetime(y, m, d, hour, tzinfo=UTC)


def make_entry(**overrides: Any) -> ManifestEntry:
    base: dict[str, Any] = {
        "domain": "market",
        "dataset": "bars",
        "exchange": "NSE",
        "asset_class": "EQ",
        "timeframe": "1d",
        "time_bucket": "year=2024",
        "object_sha": "ab" + "b" * 62,
        "min_ts": utc(2024, 1, 1),
        "max_ts": utc(2024, 1, 2),
        "row_count": 4,
        "sort_key": "(instrument_uid, ts)",
    }
    base.update(overrides)
    return ManifestEntry(**base)


def bars_df() -> pl.DataFrame:
    """Two-instrument bar frame with exact ``Decimal128(18, 4)`` prices."""
    return pl.DataFrame(
        {
            "instrument_uid": ["NSE:RELIANCE", "NSE:RELIANCE", "NSE:TCS", "NSE:TCS"],
            "ts": [utc(2024, 1, 1), utc(2024, 1, 2), utc(2024, 1, 1), utc(2024, 1, 2)],
            "open": ["100.0000", "101.0000", "50.0000", "51.0000"],
            "high": ["105.0000", "106.0000", "52.0000", "53.0000"],
            "low": ["99.0000", "100.0000", "49.0000", "50.0000"],
            "close": ["104.0000", "105.5000", "51.0000", "52.0000"],
            "volume": [1_000_000, 1_200_000, 500_000, 600_000],
        }
    ).with_columns(
        pl.col("open").cast(pl.Decimal(18, 4)),
        pl.col("high").cast(pl.Decimal(18, 4)),
        pl.col("low").cast(pl.Decimal(18, 4)),
        pl.col("close").cast(pl.Decimal(18, 4)),
    )


def seed_store(root: Path, entries: list[ManifestEntry], *, summary: str = "seed") -> ManifestStore:
    store = ManifestStore(root)
    store.commit(
        summary,
        as_of_ist=date(2024, 1, 1),
        schema_version=SCHEMA_VERSION,
        entries=entries,
    )
    return store


# ---------------------------------------------------------------------------
# status / log / tag / verify
# ---------------------------------------------------------------------------


def test_status_and_log_after_seeded_commit(tmp_path: Path, capsys) -> None:
    root = tmp_path / "lake"
    store = seed_store(root, [make_entry()])
    store.close()

    assert cli.main(["status", "--root", str(root)]) == 0
    out = capsys.readouterr().out
    assert "refs/heads/main: " in out
    assert "commits: 1" in out
    assert "entries (main): 1" in out
    assert "objects: 0" in out

    assert cli.main(["log", "--root", str(root), "--n", "5"]) == 0
    log_out = capsys.readouterr().out
    assert "seed" in log_out
    assert "v3" in log_out


def test_tag_and_verify_pass_on_good_ref(tmp_path: Path, capsys) -> None:
    root = tmp_path / "lake"
    sha = write_cas_object(bars_df(), sort_key=["instrument_uid", "ts"], root=root)
    store = seed_store(
        root,
        [make_entry(object_sha=sha, row_count=4, min_ts=utc(2024, 1, 1), max_ts=utc(2024, 1, 2))],
    )
    store.close()

    assert cli.main(["tag", "v1", "--ref", "main", "--root", str(root)]) == 0
    assert (root / "refs" / "tags" / "v1").exists()
    tag_out = capsys.readouterr().out
    assert "tagged v1 -> " in tag_out

    assert cli.main(["verify", "--ref", "main", "--root", str(root)]) == 0
    assert "verify PASS" in capsys.readouterr().out


def test_verify_fails_on_float_typed_object(tmp_path: Path, capsys) -> None:
    root = tmp_path / "lake"
    sha = "ab" + "c" * 62
    table = pa.Table.from_arrays(
        [
            pa.array(["NSE:RELIANCE"], type=pa.string()),
            pa.array([utc(2024, 1, 1)], type=pa.timestamp("us", tz="UTC")),
            pa.array([100.5], type=pa.float64()),
        ],
        names=["instrument_uid", "ts", "close"],
    )
    obj = object_path(root, sha)
    obj.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, obj)

    store = seed_store(root, [make_entry(object_sha=sha, row_count=1)])
    store.close()

    assert cli.main(["verify", "--ref", "main", "--root", str(root)]) == 1
    out = capsys.readouterr().out
    assert "verify FAIL" in out
    assert "float" in out


def test_verify_fails_on_mismatched_row_count(tmp_path: Path, capsys) -> None:
    root = tmp_path / "lake"
    sha = write_cas_object(bars_df(), sort_key=["instrument_uid", "ts"], root=root)
    store = seed_store(root, [make_entry(object_sha=sha, row_count=999)])
    store.close()

    assert cli.main(["verify", "--ref", "main", "--root", str(root)]) == 1
    out = capsys.readouterr().out
    assert "verify FAIL" in out
    assert "row_count 999 != actual 4" in out


# ---------------------------------------------------------------------------
# compact
# ---------------------------------------------------------------------------


def test_compact_noop_creates_follow_on_commit(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    store = seed_store(root, [make_entry()])
    store.close()
    assert len(list((root / "commits").glob("*.json"))) == 1

    assert cli.main(["compact", "--root", str(root)]) == 0
    assert len(list((root / "commits").glob("*.json"))) == 2


def _part_frame(rows: list[tuple[str, datetime, str, str, str, str, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "instrument_uid": [r[0] for r in rows],
            "ts": [r[1] for r in rows],
            "open": [r[2] for r in rows],
            "high": [r[3] for r in rows],
            "low": [r[4] for r in rows],
            "close": [r[5] for r in rows],
            "volume": [r[6] for r in rows],
        }
    ).with_columns(
        pl.col("open").cast(pl.Decimal(18, 4)),
        pl.col("high").cast(pl.Decimal(18, 4)),
        pl.col("low").cast(pl.Decimal(18, 4)),
        pl.col("close").cast(pl.Decimal(18, 4)),
    )


def test_compact_merges_part_files_into_sorted_object(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    store = seed_store(root, [make_entry()])
    store.close()

    flush = root / "bars" / "timeframe=1d" / "exchange=NSE" / "year=2024"
    flush.mkdir(parents=True)
    _part_frame(
        [
            ("u1", utc(2024, 1, 1), "100.0000", "105.0000", "99.0000", "104.0000", 1_000),
            ("u2", utc(2024, 1, 2), "50.0000", "53.0000", "49.0000", "52.0000", 2_000),
        ]
    ).write_parquet(flush / "part-0.parquet")
    _part_frame(
        [
            ("u3", utc(2024, 1, 1), "30.0000", "33.0000", "29.0000", "32.0000", 3_000),
            ("u2", utc(2024, 1, 1), "51.0000", "54.0000", "50.0000", "53.0000", 4_000),
        ]
    ).write_parquet(flush / "part-1.parquet")

    assert cli.main(["compact", "--root", str(root)]) == 0

    store = ManifestStore(root)
    main = store.resolve_ref("main")
    assert main is not None
    assert main.summary.startswith("compact: merged 2 flush segment(s)")
    entries = store.resolve_partition("market", "bars", exchange="NSE", timeframe="1d")
    store.close()

    assert len(entries) == 1
    assert entries[0].row_count == 4
    merged = read_cas_object(root, entries[0].object_sha)
    assert merged.height == 4
    # object is sorted by (instrument_uid, ts), even though the flush parts
    # arrived in a different order
    assert merged.equals(merged.sort(["instrument_uid", "ts"]))
    assert merged["instrument_uid"].to_list() == ["u1", "u2", "u2", "u3"]
    assert merged["close"].to_list() == [
        Decimal("104.0000"),
        Decimal("53.0000"),
        Decimal("52.0000"),
        Decimal("32.0000"),
    ]


# ---------------------------------------------------------------------------
# gc
# ---------------------------------------------------------------------------


def test_gc_dry_run_lists_and_gc_removes_unreferenced(tmp_path: Path, capsys) -> None:
    root = tmp_path / "lake"
    sha_a = write_cas_object(bars_df().head(2), sort_key=["instrument_uid", "ts"], root=root)
    store = seed_store(
        root,
        [make_entry(object_sha=sha_a, row_count=2, min_ts=utc(2024, 1, 1), max_ts=utc(2024, 1, 2))],
    )
    store.tag("v1")
    sha_b = write_cas_object(bars_df().tail(2), sort_key=["instrument_uid", "ts"], root=root)
    store.commit(
        "second",
        as_of_ist=date(2024, 1, 2),
        schema_version=SCHEMA_VERSION,
        entries=[make_entry(object_sha=sha_b, row_count=2)],
        parent=store.resolve_ref("main"),
    )
    store.close()
    sha_c = write_cas_object(
        pl.DataFrame(
            {
                "instrument_uid": ["u9"],
                "ts": [utc(2024, 1, 1)],
                "close": ["9.0000"],
            }
        ).with_columns(pl.col("close").cast(pl.Decimal(18, 4))),
        sort_key=["instrument_uid", "ts"],
        root=root,
    )

    assert cli.main(["gc", "--dry-run", "--root", str(root)]) == 0
    dry = capsys.readouterr().out
    assert sha_c in dry
    assert sha_a not in dry
    assert sha_b not in dry

    assert cli.main(["gc", "--root", str(root)]) == 0
    assert not object_path(root, sha_c).exists()
    assert object_path(root, sha_a).exists()  # pinned by refs/tags/v1
    assert object_path(root, sha_b).exists()  # pinned by refs/heads/main


# ---------------------------------------------------------------------------
# materialize-views
# ---------------------------------------------------------------------------


def test_materialize_views_creates_resolving_views(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    sha = write_cas_object(bars_df(), sort_key=["instrument_uid", "ts"], root=root)
    store = seed_store(root, [make_entry(object_sha=sha, row_count=4)])
    store.close()

    assert cli.main(["materialize-views", "--root", str(root)]) == 0
    view = (
        root
        / "views"
        / "market"
        / "bars"
        / "exchange=NSE"
        / "asset_class=EQ"
        / "timeframe=1d"
        / "year=2024"
        / f"{sha}.parquet"
    )
    assert view.exists()
    assert pl.read_parquet(view).equals(read_cas_object(root, sha))
    # re-running is idempotent (views already exist)
    assert cli.main(["materialize-views", "--root", str(root)]) == 0


# ---------------------------------------------------------------------------
# migrate-v3
# ---------------------------------------------------------------------------


def _legacy_bars(rows: list[tuple[datetime, str, str, str, str, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ts": [r[0] for r in rows],
            "open": [r[1] for r in rows],
            "high": [r[2] for r in rows],
            "low": [r[3] for r in rows],
            "close": [r[4] for r in rows],
            "volume": [r[5] for r in rows],
        }
    ).with_columns(
        pl.col("open").cast(pl.Decimal(18, 4)),
        pl.col("high").cast(pl.Decimal(18, 4)),
        pl.col("low").cast(pl.Decimal(18, 4)),
        pl.col("close").cast(pl.Decimal(18, 4)),
    )


def _seed_legacy_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _legacy_bars(
        [
            (utc(2026, 1, 5), "1240.0000", "1260.0000", "1230.0000", "1250.0000", 100_000),
            (utc(2026, 1, 6), "1255.0000", "1270.0000", "1245.0000", "1260.0000", 110_000),
            (utc(2026, 1, 7), "1260.0000", "1280.0000", "1250.0000", "1270.0000", 120_000),
        ]
    ).write_parquet(root / "RELIANCE.NSE.parquet")

    runtime = root / "catalog" / "1D" / "NSE" / "RELIANCE"
    runtime.mkdir(parents=True)
    _legacy_bars(
        [
            (utc(2026, 2, 2), "1290.0000", "1310.0000", "1280.0000", "1300.0000", 90_000),
            (utc(2026, 2, 3), "1305.0000", "1320.0000", "1295.0000", "1310.0000", 95_000),
            (utc(2026, 2, 4), "1310.0000", "1330.0000", "1300.0000", "1320.0000", 100_000),
            (utc(2026, 2, 5), "1325.0000", "1340.0000", "1315.0000", "1330.0000", 105_000),
        ]
    ).write_parquet(runtime / "2026.parquet")

    filings = root / "catalog" / "filings"
    filings.mkdir(parents=True)
    pl.DataFrame(
        {
            "filing_date": [date(2026, 11, 17), date(2026, 11, 17)],
            "instrument": ["NSE:RELIANCE-EQ", "NSE:TCS-EQ"],
            "filing_type": ["QUARTERLY_RESULT", "QUARTERLY_RESULT"],
            "title": ["Q3 FY27 Results", "Q3 FY27 Results"],
            "source_url": ["https://example.com/r", "https://example.com/t"],
        }
    ).write_parquet(filings / "2026-11-17.parquet")


def test_migrate_v3_dry_run_then_real_with_parity(tmp_path: Path, capsys) -> None:
    root = tmp_path / "lake"
    _seed_legacy_fixture(root)

    assert cli.main(["migrate-v3", "--root", str(root), "--dry-run"]) == 0
    dry = capsys.readouterr().out
    assert "RELIANCE.NSE.parquet" in dry
    assert "catalog/1D/NSE/RELIANCE/2026.parquet" in dry
    assert "catalog/filings: 1 file(s)" in dry
    assert "dry-run" in dry

    assert cli.main(["migrate-v3", "--root", str(root)]) == 0
    out = capsys.readouterr().out
    assert "migrate-v3 parity" in out
    assert "PASS" in out

    assert (root / "catalog.duckdb").exists()
    assert (root / "refs" / "heads" / "main").exists()

    # migrated bars read back through the manifest with exact decimals
    bars = ParquetCatalog(root).read_bars("NSE:RELIANCE-EQ")
    assert bars.height == 7
    assert bars["close"].to_list() == [
        Decimal("1250.0000"),
        Decimal("1260.0000"),
        Decimal("1270.0000"),
        Decimal("1300.0000"),
        Decimal("1310.0000"),
        Decimal("1320.0000"),
        Decimal("1330.0000"),
    ]

    # legacy inputs are left in place (deprecation is cross-repo Phase 3)
    assert (root / "RELIANCE.NSE.parquet").exists()
    assert (root / "catalog" / "1D" / "NSE" / "RELIANCE" / "2026.parquet").exists()
    assert (root / "catalog" / "filings" / "2026-11-17.parquet").exists()

    # the migrated tree itself satisfies the CLI's verify invariants
    assert cli.main(["verify", "--ref", "main", "--root", str(root)]) == 0


def test_migrate_v3_empty_root_is_noop(tmp_path: Path, capsys) -> None:
    root = tmp_path / "lake"
    assert cli.main(["migrate-v3", "--root", str(root)]) == 0
    assert "no legacy inputs" in capsys.readouterr().out
    assert not (root / "catalog.duckdb").exists()
