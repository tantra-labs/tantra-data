"""``tantra-catalog`` — git-style lakehouse v3 operational CLI (plan 0009 Phase C).

Implements ADR ``docs/design/0004`` §2.1-§3 / ``tantra-data`` plan 0009
Phase C on top of the Phase A/B CAS + manifest engine:

- ``status`` / ``log`` / ``tag`` — inspect refs, commits, and pin snapshots.
- ``verify`` — walk a ref's ``manifest_entries``, read every CAS object, and
  enforce Zero-Floats (``Decimal128(18, 4)`` money, no ``float`` anywhere),
  per-dataset ``(min_ts, max_ts)`` monotonicity, and exact ``row_count``.
- ``compact`` — merge legacy v2 ``part-*`` flush segments (``{root}/bars/...``,
  ``{root}/ticks/...``) into sorted CAS objects under one new commit without
  touching blobs pinned by existing ``refs/tags/*``.
- ``gc`` — prune ``objects/sha256/*`` not reachable from any ``refs/{heads,tags}/*``.
- ``materialize-views`` — hardlink (or symlink) read-only Hive views under
  ``{root}/views/`` pointing at the current ``main`` CAS blobs.
- ``migrate-v3`` — one-shot migration of the legacy ``*.NSE.parquet``,
  ``catalog/{TF}/{EXCHANGE}/{SYMBOL}/{YYYY}.parquet`` and flat bridge
  projections with a strict bar-count + OHLCV ``Decimal``-sum parity gate.

Zero-floats: migration normalizes legacy float prices to exact
``Decimal128(18, 4)`` before they ever touch the CAS store.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.adapters.cas_store import (
    gc_unreachable,
    object_path,
    read_cas_object,
    write_cas_object,
)
from tantra_data.adapters.manifest_store import SCHEMA_VERSION, ManifestStore
from tantra_data.domain.lakehouse import CommitRecord, InstrumentAlias, ManifestEntry
from tantra_data.service.bridge_exporter import _V3_PROJECTIONS

_DEFAULT_ROOT = "data"
_MONEY_COLUMNS = ("open", "high", "low", "close")
_BAR_COLUMNS = (*_MONEY_COLUMNS, "volume")
_INTRADAY_TIMEFRAMES = {"1m", "5m", "15m", "1h"}


# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _load_commit_file(root: Path, sha: str) -> CommitRecord | None:
    commit_file = root / "commits" / f"{sha}.json"
    if not commit_file.exists():
        return None
    payload = json.loads(commit_file.read_text())
    return CommitRecord(
        commit_sha=sha,
        parent_sha=payload.get("parent_sha"),
        tree_hash=payload["tree_hash"],
        created_at_utc=datetime.fromisoformat(payload["created_at_utc"]),
        as_of_ist=date.fromisoformat(payload["as_of_ist"]),
        schema_version=payload["schema_version"],
        summary=payload["summary"],
    )


def _entries_for_commit(root: Path, commit_sha: str) -> list[ManifestEntry]:
    """Read the manifest entries of one commit (fresh DuckDB connection)."""
    con = duckdb.connect(str(root / "catalog.duckdb"))
    try:
        rows = con.execute(
            "SELECT domain, dataset, exchange, asset_class, timeframe, time_bucket, "
            "object_sha, min_ts, max_ts, row_count, sort_key "
            "FROM manifest_entries WHERE commit_sha = ? ORDER BY min_ts, max_ts",
            [commit_sha],
        ).fetchall()
    finally:
        con.close()
    return [
        ManifestEntry(
            domain=r[0],
            dataset=r[1],
            exchange=r[2],
            asset_class=r[3],
            timeframe=r[4],
            time_bucket=r[5],
            object_sha=r[6],
            min_ts=_to_utc(r[7]),
            max_ts=_to_utc(r[8]),
            row_count=r[9],
            sort_key=r[10],
        )
        for r in rows
    ]


def _history(root: Path, store: ManifestStore) -> list[CommitRecord]:
    """Walk ``main`` back through ``parent_sha`` (newest first)."""
    records: list[CommitRecord] = []
    seen: set[str] = set()
    current = store.resolve_ref("main")
    while current is not None and current.commit_sha not in seen:
        seen.add(current.commit_sha)
        records.append(current)
        if current.parent_sha is None:
            break
        current = _load_commit_file(root, current.parent_sha)
    return records


def _all_refs(root: Path) -> list[str]:
    """Names of every ref pointer file under ``refs/{heads,tags}`` (relative to root)."""
    refs: list[str] = []
    for area in ("heads", "tags"):
        area_dir = root / "refs" / area
        if not area_dir.exists():
            continue
        for ref_file in area_dir.rglob("*"):
            if not ref_file.is_file():
                continue
            content = ref_file.read_text().strip()
            if len(content) == 64 and set(content) <= set("0123456789abcdef"):
                refs.append(str(ref_file.relative_to(root)))
    return refs


def _reachable_objects(root: Path) -> set[str]:
    if not (root / "catalog.duckdb").exists():
        return set()
    reachable: set[str] = set()
    for ref in _all_refs(root):
        sha = (root / ref).read_text().strip()
        for entry in _entries_for_commit(root, sha):
            reachable.add(entry.object_sha)
    return reachable


# ---------------------------------------------------------------------------
# bar / ts normalization (zero-floats)
# ---------------------------------------------------------------------------


def _normalize_ts(df: pl.DataFrame, column: str = "ts") -> pl.DataFrame:
    dtype = df[column].dtype
    if dtype == pl.Int64:
        df = df.with_columns(
            pl.when(pl.col(column) > 1_000_000_000_000_000_000)
            .then(pl.col(column).cast(pl.Datetime("ns")).dt.cast_time_unit("us"))
            .when(pl.col(column) > 1_000_000_000_000_000)
            .then(pl.col(column).cast(pl.Datetime("us")))
            .otherwise(pl.col(column) * 1_000_000)
            .cast(pl.Datetime("us"))
            .alias(column)
        )
    elif dtype.is_temporal():
        df = df.with_columns(pl.col(column).dt.cast_time_unit("us"))
    else:
        raise ValueError(f"{column} column has unsupported dtype {dtype}")
    if df[column].dtype.time_zone is None:
        df = df.with_columns(pl.col(column).dt.replace_time_zone("UTC"))
    return df


def _normalize_money(df: pl.DataFrame) -> pl.DataFrame:
    for column in _MONEY_COLUMNS:
        if column not in df.columns:
            continue
        dtype = df[column].dtype
        if dtype.is_float():
            df = df.with_columns(pl.col(column).round(4).cast(pl.String).cast(pl.Decimal(18, 4)))
        elif dtype != pl.Decimal(18, 4):
            df = df.with_columns(pl.col(column).cast(pl.Decimal(18, 4)))
    return df


def _normalize_bars(frame: pl.DataFrame, instrument_uid: str) -> pl.DataFrame:
    missing = [c for c in ("ts", *_MONEY_COLUMNS, "volume") if c not in frame.columns]
    if missing:
        raise ValueError(f"legacy bar file missing columns: {missing}")
    df = frame.select(["ts", *_MONEY_COLUMNS, "volume"])
    df = _normalize_ts(df)
    df = _normalize_money(df)
    return df.with_columns(
        pl.col("volume").cast(pl.Int64),
        pl.lit(instrument_uid).alias("instrument_uid"),
    ).select(["instrument_uid", "ts", *_MONEY_COLUMNS, "volume"])


def _sort_key_for(frame: pl.DataFrame) -> list[str]:
    if "instrument_uid" in frame.columns and "ts" in frame.columns:
        return ["instrument_uid", "ts"]
    if "instrument_uid" in frame.columns and "exchange_ts" in frame.columns:
        return ["instrument_uid", "exchange_ts"]
    if "ts" in frame.columns:
        return ["ts"]
    raise ValueError("cannot derive a sort key: expected instrument_uid/ts columns")


def _ts_bounds(frame: pl.DataFrame, column: str = "ts") -> tuple[datetime, datetime]:
    lo = frame[column].min()
    hi = frame[column].max()
    if lo is None or hi is None:
        raise ValueError(f"cannot compute {column} bounds on an empty frame")
    return _to_utc(lo), _to_utc(hi)


def _instrument_uid(exchange: str, symbol: str) -> str:
    return f"{exchange}:{symbol}"


# ---------------------------------------------------------------------------
# status / log / tag / verify
# ---------------------------------------------------------------------------


def _cmd_status(args: argparse.Namespace) -> int:
    root = Path(args.root)
    store = ManifestStore(root)
    main = store.resolve_ref("main")
    store.close()
    tags_dir = root / "refs" / "tags"
    tags = sorted(tags_dir.glob("*")) if tags_dir.exists() else []
    commits_dir = root / "commits"
    commit_count = len(list(commits_dir.glob("*.json"))) if commits_dir.exists() else 0
    entries = _entries_for_commit(root, main.commit_sha) if main else []
    objects_dir = root / "objects" / "sha256"
    blobs = (
        [p for p in objects_dir.glob("*/*.parquet") if p.parent.name != ".tmp"]
        if objects_dir.exists()
        else []
    )
    if main is None:
        print("refs/heads/main: (no commits)")
    else:
        print(f"refs/heads/main: {main.commit_sha}  ({main.summary})")
    print(f"refs/tags: {len(tags)}")
    for tag in tags:
        print(f"  {tag.name}: {tag.read_text().strip()}")
    print(f"commits: {commit_count}")
    print(f"entries (main): {len(entries)}")
    print(f"objects: {len(blobs)}")
    return 0


def _cmd_log(args: argparse.Namespace) -> int:
    root = Path(args.root)
    store = ManifestStore(root)
    try:
        records = _history(root, store)
        for record in records[: args.n]:
            print(
                f"{record.commit_sha}  {record.created_at_utc.isoformat()}  "
                f"{record.as_of_ist.isoformat()}  v{record.schema_version}  {record.summary}"
            )
        if not records:
            print("(no commits)")
        return 0
    finally:
        store.close()


def _cmd_tag(args: argparse.Namespace) -> int:
    root = Path(args.root)
    store = ManifestStore(root)
    try:
        store.tag(args.name, ref=args.ref)
    except ValueError as exc:
        print(f"tag failed: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()
    tag_file = root / "refs" / "tags" / args.name
    print(f"tagged {args.name} -> {tag_file.read_text().strip()}")
    return 0


def _verify_entry(root: Path, entry: ManifestEntry, failures: list[str]) -> None:
    obj = object_path(root, entry.object_sha)
    if not obj.exists():
        failures.append(
            f"{entry.domain}/{entry.dataset} {entry.time_bucket}: missing object {entry.object_sha}"
        )
        return
    try:
        table = pq.read_table(obj)
    except Exception as exc:
        failures.append(
            f"{entry.domain}/{entry.dataset} {entry.time_bucket}: "
            f"unreadable object {entry.object_sha}: {exc}"
        )
        return
    for field in table.schema:
        if pa.types.is_floating(field.type):
            failures.append(
                f"{entry.domain}/{entry.dataset} {entry.time_bucket}: "
                f"column {field.name!r} is float {field.type}"
            )
        if pa.types.is_decimal(field.type) and field.type != pa.decimal128(18, 4):
            failures.append(
                f"{entry.domain}/{entry.dataset} {entry.time_bucket}: "
                f"column {field.name!r} is {field.type}, expected decimal128(18, 4)"
            )
        if field.name in _MONEY_COLUMNS and field.type != pa.decimal128(18, 4):
            failures.append(
                f"{entry.domain}/{entry.dataset} {entry.time_bucket}: "
                f"money column {field.name!r} is {field.type}, "
                "expected decimal128(18, 4)"
            )
    if table.num_rows != entry.row_count:
        failures.append(
            f"{entry.domain}/{entry.dataset} {entry.time_bucket}: "
            f"row_count {entry.row_count} != actual {table.num_rows}"
        )


def _verify_monotonic(entries: list[ManifestEntry], failures: list[str]) -> None:
    by_dataset: dict[tuple[str, str], list[ManifestEntry]] = {}
    for entry in entries:
        by_dataset.setdefault((entry.domain, entry.dataset), []).append(entry)
    for (domain, dataset), group in by_dataset.items():
        ordered = sorted(group, key=lambda e: (e.min_ts, e.max_ts))
        for prev, current in itertools.pairwise(ordered):
            if (current.min_ts, current.max_ts) < (prev.min_ts, prev.max_ts):
                failures.append(
                    f"{domain}/{dataset}: non-monotonic (min_ts, max_ts) between "
                    f"{prev.object_sha} and {current.object_sha}"
                )


def _cmd_verify(args: argparse.Namespace) -> int:
    root = Path(args.root)
    store = ManifestStore(root)
    commit = store.resolve_ref(args.ref)
    store.close()
    if commit is None:
        print(f"verify FAIL: ref {args.ref!r} does not resolve")
        return 1
    entries = _entries_for_commit(root, commit.commit_sha)
    failures: list[str] = []
    for entry in entries:
        _verify_entry(root, entry, failures)
    _verify_monotonic(entries, failures)
    if failures:
        print(f"verify FAIL ({len(failures)} problem(s)) on ref {args.ref!r} ({commit.commit_sha})")
        for problem in failures:
            print(f"  - {problem}")
        return 1
    print(f"verify PASS: {len(entries)} object(s) on ref {args.ref!r} ({commit.commit_sha})")
    return 0


# ---------------------------------------------------------------------------
# compact / gc / materialize-views
# ---------------------------------------------------------------------------


def _hive_parts(root: Path, path: Path) -> dict[str, str]:
    hive: dict[str, str] = {}
    try:
        rel = path.relative_to(root)
    except ValueError:
        return hive
    for part in rel.parts:
        if "=" in part:
            key, value = part.split("=", 1)
            if value:
                hive[key] = value
    return hive


def _bucket_from_hive(hive: dict[str, str]) -> str:
    if "month" in hive and "year" in hive:
        return f"year={hive['year']}/month={hive['month']}"
    if "year" in hive:
        return f"year={hive['year']}"
    if "date" in hive:
        return f"date={hive['date']}"
    raise ValueError(f"cannot derive a time bucket from hive parts: {hive}")


def _flush_groups(root: Path) -> dict[tuple[Any, ...], list[Path]]:
    groups: dict[tuple[Any, ...], list[Path]] = {}
    for top in ("bars", "ticks"):
        top_dir = root / top
        if not top_dir.exists():
            continue
        for path in sorted(top_dir.rglob("part-*.parquet")):
            hive = _hive_parts(root, path)
            key = (
                top,
                hive.get("timeframe"),
                hive.get("exchange"),
                hive.get("asset_class"),
                _bucket_from_hive(hive),
            )
            groups.setdefault(key, []).append(path)
    return groups


def _cmd_compact(args: argparse.Namespace) -> int:
    root = Path(args.root)
    store = ManifestStore(root)
    try:
        groups = _flush_groups(root)
        parent = store.resolve_ref("main")
        if not groups:
            record = store.commit(
                "compact: no legacy part-* flush segments to merge (no-op)",
                as_of_ist=date.today(),
                schema_version=SCHEMA_VERSION,
                entries=[],
                parent=parent,
            )
            print(
                f"compact: no legacy part-* flushes found; recorded no-op commit "
                f"{record.commit_sha}"
            )
            return 0
        entries: list[ManifestEntry] = []
        merged_count = 0
        for key, files in sorted(groups.items()):
            dataset, timeframe, exchange, asset_class, time_bucket = key
            frames = [pl.read_parquet(str(path)) for path in files]
            combined = _normalize_money(_normalize_ts(pl.concat(frames, how="diagonal_relaxed")))
            sort_key = _sort_key_for(combined)
            sha = write_cas_object(combined, sort_key=sort_key, root=root)
            min_ts, max_ts = _ts_bounds(combined)
            entries.append(
                ManifestEntry(
                    domain="market",
                    dataset=dataset,
                    exchange=exchange,
                    asset_class=asset_class,
                    timeframe=timeframe,
                    time_bucket=time_bucket,
                    object_sha=sha,
                    min_ts=min_ts,
                    max_ts=max_ts,
                    row_count=combined.height,
                    sort_key=f"({', '.join(sort_key)})",
                )
            )
            merged_count += len(files)
        entries.sort(key=lambda e: (e.min_ts, e.max_ts))
        record = store.commit(
            f"compact: merged {merged_count} flush segment(s) into {len(entries)} CAS object(s)",
            as_of_ist=date.today(),
            schema_version=SCHEMA_VERSION,
            entries=entries,
            parent=parent,
        )
        print(
            f"compact: merged {merged_count} flush segment(s) into {len(entries)} "
            f"object(s); commit {record.commit_sha} on refs/heads/main"
        )
        return 0
    finally:
        store.close()


def _cmd_gc(args: argparse.Namespace) -> int:
    root = Path(args.root)
    reachable = _reachable_objects(root)
    pruned = gc_unreachable(root, reachable, dry_run=args.dry_run)
    verb = "would prune" if args.dry_run else "pruned"
    print(f"gc: {len(pruned)} unreferenced object(s) {verb}")
    for path in pruned:
        print(f"  {path.relative_to(root)}")
    print(f"gc: {len(reachable)} object(s) reachable from refs/{{heads,tags}}/*")
    return 0


def _view_path(root: Path, entry: ManifestEntry) -> Path:
    parts: list[str] = [entry.domain, entry.dataset]
    if entry.exchange:
        parts.append(f"exchange={entry.exchange}")
    if entry.asset_class:
        parts.append(f"asset_class={entry.asset_class}")
    if entry.timeframe:
        parts.append(f"timeframe={entry.timeframe}")
    parts.append(entry.time_bucket)
    return root / "views" / Path(*parts) / f"{entry.object_sha}.parquet"


def _cmd_materialize_views(args: argparse.Namespace) -> int:
    root = Path(args.root)
    store = ManifestStore(root)
    commit = store.resolve_ref("main")
    store.close()
    if commit is None:
        print("materialize-views: refs/heads/main has no commits", file=sys.stderr)
        return 1
    entries = _entries_for_commit(root, commit.commit_sha)
    created = 0
    symlinked = 0
    for entry in entries:
        obj = object_path(root, entry.object_sha)
        if not obj.exists():
            print(f"materialize-views: skipping missing object {entry.object_sha}")
            continue
        view = _view_path(root, entry)
        if view.exists():
            continue
        view.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(obj, view)
        except OSError:
            os.symlink(obj, view)
            symlinked += 1
        created += 1
    note = f" ({symlinked} symlink fallback(s))" if symlinked else ""
    print(f"materialize-views: {created} view(s) under {root}/views{note}")
    return 0


# ---------------------------------------------------------------------------
# migrate-v3
# ---------------------------------------------------------------------------


def _classify_catalog_path(root: Path, path: Path) -> tuple[Any, ...] | None:
    try:
        rel = path.relative_to(root / "catalog")
    except ValueError:
        return None
    parts = rel.parts
    if len(parts) == 2 and parts[0] in _V3_PROJECTIONS:
        return ("proj", parts[0])
    if len(parts) == 4 and parts[3].endswith(".parquet"):
        return ("bars", parts[0], parts[1], parts[2])
    return None


def _plan_legacy(root: Path, plan: list[dict[str, Any]]) -> None:
    for path in sorted(root.glob("*.NSE.parquet")):
        symbol = path.name.split(".NSE.parquet")[0]
        plan.append(
            {
                "kind": "bars",
                "input": str(path.relative_to(root)),
                "path": path,
                "exchange": "NSE",
                "symbol": symbol,
                "timeframe": "1d",
            }
        )
    catalog_dir = root / "catalog"
    if not catalog_dir.exists():
        return
    proj_files: dict[str, list[Path]] = {}
    for path in sorted(catalog_dir.rglob("*.parquet")):
        classified = _classify_catalog_path(root, path)
        if classified is None:
            continue
        if classified[0] == "bars":
            _, tf, exchange, symbol = classified
            plan.append(
                {
                    "kind": "bars",
                    "input": str(path.relative_to(root)),
                    "path": path,
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": tf.lower(),
                }
            )
        else:
            proj_files.setdefault(classified[1], []).append(path)
    for dataset, paths in sorted(proj_files.items()):
        plan.append({"kind": "proj", "dataset": dataset, "paths": paths})


def _bar_entries(
    root: Path,
    item: dict[str, Any],
    entries: list[ManifestEntry],
    shas: list[str],
) -> pl.DataFrame:
    frame = pl.read_parquet(item["path"])
    uid = _instrument_uid(item["exchange"], item["symbol"])
    normalized = _normalize_bars(frame, uid)
    timeframe = item["timeframe"]
    intraday = timeframe in _INTRADAY_TIMEFRAMES
    df = normalized.with_columns(
        pl.col("ts").dt.year().alias("_year"),
        pl.col("ts").dt.month().alias("_month"),
    )
    if intraday:
        buckets = sorted(zip(df["_year"].to_list(), df["_month"].to_list(), strict=True))
        for year, month in buckets:
            chunk = df.filter((pl.col("_year") == year) & (pl.col("_month") == month)).drop(
                ["_year", "_month"]
            )
            bucket = f"year={year}/month={month:02d}"
            _append_bar_entry(root, chunk, bucket, item, entries, shas)
    else:
        for year in sorted(df["_year"].unique().to_list()):
            chunk = df.filter(pl.col("_year") == year).drop(["_year", "_month"])
            bucket = f"year={year}"
            _append_bar_entry(root, chunk, bucket, item, entries, shas)
    return normalized


def _append_bar_entry(
    root: Path,
    chunk: pl.DataFrame,
    bucket: str,
    item: dict[str, Any],
    entries: list[ManifestEntry],
    shas: list[str],
) -> None:
    sha = write_cas_object(chunk, sort_key=["instrument_uid", "ts"], root=root)
    shas.append(sha)
    min_ts, max_ts = _ts_bounds(chunk)
    entries.append(
        ManifestEntry(
            domain="market",
            dataset="bars",
            exchange=item["exchange"],
            asset_class="EQ",
            timeframe=item["timeframe"],
            time_bucket=bucket,
            object_sha=sha,
            min_ts=min_ts,
            max_ts=max_ts,
            row_count=chunk.height,
            sort_key="(instrument_uid, ts)",
        )
    )


def _projection_bounds(frame: pl.DataFrame, column: str) -> tuple[datetime, datetime, date]:
    dates: list[date] = []
    for value in frame[column].to_list():
        if isinstance(value, datetime):
            dates.append(value.date())
        elif isinstance(value, date):
            dates.append(value)
        else:
            raise TypeError(f"column {column!r} is not a date/timestamp column: {value!r}")
    lo, hi = min(dates), max(dates)
    min_ts = datetime.combine(lo, time.min, tzinfo=UTC)
    max_ts = datetime.combine(hi, time.max, tzinfo=UTC)
    return min_ts, max_ts, hi


def _migrate_projection(root: Path, item: dict[str, Any], entries: list[ManifestEntry]) -> None:
    dataset = item["dataset"]
    frames = [pl.read_parquet(str(path)) for path in item["paths"]]
    combined = pl.concat(frames, how="diagonal_relaxed")
    domain, sort_col = _V3_PROJECTIONS[dataset]
    min_ts, max_ts, as_of_ist = _projection_bounds(combined, sort_col)
    sha = write_cas_object(combined, sort_key=sort_col, root=root)
    entries.append(
        ManifestEntry(
            domain=domain,
            dataset=dataset,
            exchange=None,
            asset_class=None,
            timeframe=None,
            time_bucket=f"year={as_of_ist.year}",
            object_sha=sha,
            min_ts=min_ts,
            max_ts=max_ts,
            row_count=combined.height,
            sort_key=sort_col,
        )
    )


def _parity_row(
    parity: list[dict[str, Any]], item: dict[str, Any], before: pl.DataFrame, after: pl.DataFrame
) -> None:
    row: dict[str, Any] = {
        "input": item["input"],
        "before": before.height,
        "after": after.height,
    }
    if item["kind"] == "bars":
        row["sums"] = {
            column: (before[column].sum(), after[column].sum()) for column in _BAR_COLUMNS
        }
    parity.append(row)


def _assert_parity(parity: list[dict[str, Any]]) -> None:
    for row in parity:
        if "sums" not in row:
            continue
        if row["before"] != row["after"]:
            raise AssertionError(
                f"migrate-v3 parity failure for {row['input']}: bar count "
                f"{row['before']} before != {row['after']} after"
            )
        for column, (before_sum, after_sum) in row["sums"].items():
            if before_sum != after_sum:
                raise AssertionError(
                    f"migrate-v3 parity failure for {row['input']}: sum({column}) "
                    f"{before_sum} before != {after_sum} after"
                )


def _print_parity(parity: list[dict[str, Any]]) -> None:
    print("migrate-v3 parity (bar count + OHLCV Decimal sums, before vs after)")
    for row in parity:
        if "sums" in row:
            sums_ok = all(sb == sa for sb, sa in row["sums"].values())
            status = "ok" if sums_ok else "MISMATCH"
            print(
                f"  PASS  {row['input']:<46} {row['before']:>6} bars == "
                f"{row['after']:>6} bars  sums={status}"
            )
        else:
            print(f"  PASS  {row['input']:<46} {row['before']:>6} rows == {row['after']:>6} rows")


def _write_aliases(store: ManifestStore, plan: list[dict[str, Any]]) -> None:
    for item in plan:
        if item["kind"] != "bars":
            continue
        uid = _instrument_uid(item["exchange"], item["symbol"])
        store.write_alias(
            InstrumentAlias(
                exchange=item["exchange"],
                symbol=item["symbol"],
                series=None,
                valid_from=None,
                valid_to=None,
                instrument_uid=uid,
            )
        )


def _format_plan(item: dict[str, Any]) -> str:
    if item["kind"] == "bars":
        frame = pl.read_parquet(item["path"])
        uid = _instrument_uid(item["exchange"], item["symbol"])
        normalized = _normalize_bars(frame, uid)
        bucket = "year" if item["timeframe"] not in _INTRADAY_TIMEFRAMES else "year/month"
        return (
            f"  {item['input']:<44} -> market/bars/timeframe={item['timeframe']}/"
            f"exchange={item['exchange']} ({bucket}, {normalized.height} bars)"
        )
    domain = _V3_PROJECTIONS[item["dataset"]][0]
    return (
        f"  catalog/{item['dataset']}: {len(item['paths'])} file(s) -> {domain}/{item['dataset']}"
    )


def _cmd_migrate_v3(args: argparse.Namespace) -> int:
    root = Path(args.root)
    plan: list[dict[str, Any]] = []
    _plan_legacy(root, plan)
    if not plan:
        print(
            "migrate-v3: no legacy inputs found under {root}/*.NSE.parquet, "
            "{root}/catalog/{TF}/... or {root}/catalog/<dataset>/... "
            f"(root: {root})"
        )
        return 0
    if args.dry_run:
        for item in plan:
            print(_format_plan(item))
        print(f"migrate-v3 dry-run: {len(plan)} legacy input(s) would migrate")
        return 0

    store = ManifestStore(root)
    try:
        entries: list[ManifestEntry] = []
        parity: list[dict[str, Any]] = []
        for item in plan:
            if item["kind"] == "bars":
                shas: list[str] = []
                before = _bar_entries(root, item, entries, shas)
                after_frames = [read_cas_object(root, sha) for sha in shas]
                after = (
                    pl.concat(after_frames) if after_frames else pl.DataFrame(schema=before.schema)
                )
                _parity_row(parity, item, before, after)
            else:
                _migrate_projection(root, item, entries)
        entries.sort(key=lambda e: (e.min_ts, e.max_ts))
        _assert_parity(parity)
        _write_aliases(store, plan)
        as_of_ist = max((entry.max_ts.date() for entry in entries), default=date.today())
        parent = store.resolve_ref("main")
        record = store.commit(
            "migrate-v3: migrated legacy bars + bridge projections to v3 CAS",
            as_of_ist=as_of_ist,
            schema_version=SCHEMA_VERSION,
            entries=entries,
            parent=parent,
        )
        _print_parity(parity)
        print(
            f"migrate-v3: committed {record.commit_sha} with {len(entries)} "
            f"entry/entries on refs/heads/main; legacy inputs left in place"
        )
        return 0
    finally:
        store.close()


# ---------------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------------


def _add_root(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--root",
        default=_DEFAULT_ROOT,
        help=f"lakehouse root (default: {_DEFAULT_ROOT})",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tantra-catalog",
        description="Git-style content-addressed lakehouse v3 CLI (ADR 0004 / plan 0009)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="print refs, commit/entry/blob counts")
    _add_root(status)

    log = sub.add_parser("log", help="list the last N commits")
    _add_root(log)
    log.add_argument("--n", type=int, default=10, help="number of commits to list (default: 10)")

    tag = sub.add_parser("tag", help="pin a refs/tags/<name> pointer")
    tag.add_argument("name", help="tag name")
    tag.add_argument("--ref", default="main", help="ref to pin (default: main)")
    _add_root(tag)

    verify = sub.add_parser("verify", help="verify zero-floats/monotonic/row-count invariants")
    verify.add_argument("--ref", default="main", help="ref to verify (default: main)")
    _add_root(verify)

    compact = sub.add_parser("compact", help="merge legacy part-* flushes into sorted CAS objects")
    _add_root(compact)

    gc = sub.add_parser("gc", help="prune unreachable CAS blobs")
    gc.add_argument("--dry-run", action="store_true", help="only list would-be-pruned blobs")
    _add_root(gc)

    views = sub.add_parser("materialize-views", help="hardlink Hive views under {root}/views")
    _add_root(views)

    migrate = sub.add_parser("migrate-v3", help="one-shot migration of legacy v1/v2 layouts")
    migrate.add_argument("--dry-run", action="store_true", help="report only, do not write")
    _add_root(migrate)

    return parser


_COMMANDS = {
    "status": _cmd_status,
    "log": _cmd_log,
    "tag": _cmd_tag,
    "verify": _cmd_verify,
    "compact": _cmd_compact,
    "gc": _cmd_gc,
    "materialize-views": _cmd_materialize_views,
    "migrate-v3": _cmd_migrate_v3,
}


def main(argv: list[str] | None = None) -> int:
    """Run the ``tantra-catalog`` CLI; returns 0 on success, 1 on failure."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return _COMMANDS[args.command](args)
    except Exception as exc:
        print(f"tantra-catalog {args.command} failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
