"""Content-addressed Parquet object store for the v3 lakehouse (ADR 0004).

Blobs are immutable, ``Zstd``-compressed Parquet segments sorted by a
dataset's clustering key and addressed by the SHA-256 of their serialized
bytes::

    {root}/objects/sha256/<ab>/<sha256>.parquet

Repeated identical segments deduplicate naturally (the same table bytes
produce the same object path). Every write goes through a ``.tmp`` sibling
and is atomically promoted, so readers never observe a partial object. Money
columns are always ``Decimal128(18, 4)`` — never floating point.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterable
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

_MONEY = pa.decimal128(18, 4)


def _canonical_table(table_or_df: pa.Table | pl.DataFrame) -> pa.Table:
    """Normalize a table/DataFrame to a Zero-Floats canonical Arrow table.

    Decimal columns are promoted to ``Decimal128(18, 4)``; floating-point
    columns are rejected outright — the lakehouse never stores ``float``.
    """
    if isinstance(table_or_df, pl.DataFrame):
        table = table_or_df.to_arrow()
    elif isinstance(table_or_df, pa.Table):
        table = table_or_df
    else:
        raise TypeError(f"expected polars.DataFrame or pyarrow.Table, got {type(table_or_df)!r}")

    fields: list[pa.Field] = []
    arrays: list[pa.Array] = []
    for field, column in zip(table.schema, table.columns, strict=True):
        if pa.types.is_floating(field.type):
            raise ValueError(
                f"zero-floats: column {field.name!r} has float type {field.type}; "
                "pass exact decimal128 values instead"
            )
        if pa.types.is_decimal(field.type) and field.type != _MONEY:
            fields.append(pa.field(field.name, _MONEY))
            arrays.append(column.cast(_MONEY))
        else:
            fields.append(field)
            arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _sort_key(sort_key: str | Iterable[str]) -> list[str]:
    if isinstance(sort_key, str):
        return [sort_key]
    keys = list(sort_key)
    if not keys:
        raise ValueError("sort_key must name at least one column")
    return keys


def object_path(root: Path, sha: str) -> Path:
    """Absolute path of the object for ``sha`` under ``root``."""
    return root / "objects" / "sha256" / sha[:2] / f"{sha}.parquet"


def write_cas_object(
    table_or_df: pa.Table | pl.DataFrame,
    *,
    sort_key: str | Iterable[str],
    root: Path,
    row_group_size: int = 100_000,
) -> str:
    """Write a sorted, Zstd Parquet object and return its SHA-256.

    The table is sorted by ``sort_key``, serialized as Zstd Parquet with
    ``row_group_size`` rows per group, hashed, and atomically promoted to
    ``{root}/objects/sha256/<ab>/<sha>.parquet``. Writing an identical table
    is idempotent: the existing object is reused and the same sha returned.
    """
    table = _canonical_table(table_or_df)
    keys = _sort_key(sort_key)
    missing = [k for k in keys if k not in table.schema.names]
    if missing:
        raise ValueError(f"sort_key columns missing from table: {missing}")
    table = table.sort_by([(key, "ascending") for key in keys])

    root = Path(root)
    staging = root / "objects" / "sha256" / ".tmp"
    staging.mkdir(parents=True, exist_ok=True)
    tmp_path = staging / f"{uuid.uuid4().hex}.parquet"
    try:
        pq.write_table(
            table,
            tmp_path,
            compression="zstd",
            version="2.6",
            row_group_size=row_group_size,
        )
        digest = hashlib.sha256()
        with tmp_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        sha = digest.hexdigest()
        target = object_path(root, sha)
        if target.exists():
            return sha
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp_path, target)
        return sha
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def read_cas_object(root: Path, sha: str) -> pl.DataFrame:
    """Read the Parquet object for ``sha`` as a :class:`polars.DataFrame`."""
    path = object_path(Path(root), sha)
    if not path.exists():
        raise FileNotFoundError(f"no CAS object for sha {sha} at {path}")
    return pl.read_parquet(path)


def gc_unreachable(root: Path, reachable: set[str], *, dry_run: bool) -> list[Path]:
    """Prune ``objects/sha256/*`` blobs not referenced by ``reachable``.

    Returns the list of pruned (or, with ``dry_run=True``, would-be-pruned)
    object paths. Refs/tags must have already been folded into ``reachable``
    by the caller so pinned blobs are never collected.
    """
    root = Path(root)
    objects_root = root / "objects" / "sha256"
    if not objects_root.exists():
        return []
    candidates: list[Path] = []
    for bucket in sorted(p for p in objects_root.iterdir() if p.is_dir()):
        if bucket.name == ".tmp":
            continue
        for blob in sorted(bucket.glob("*.parquet")):
            sha = blob.stem
            if sha not in reachable:
                candidates.append(blob)
    if not dry_run:
        for blob in candidates:
            blob.unlink(missing_ok=True)
    return candidates
