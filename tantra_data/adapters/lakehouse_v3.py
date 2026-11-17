"""Lakehouse v3 (ADR 0004, Phase 1) Hive-partitioned bar and alias writers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

_DEC = pa.decimal128(18, 4)
_SOURCE_PRECEDENCE = {"udiff": 3, "legacy": 2, "sec_bhavdata": 1}

BARS_SCHEMA = pa.schema(
    [
        pa.field("instrument_uid", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("series", pa.string(), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("ts", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("open", _DEC, nullable=False),
        pa.field("high", _DEC, nullable=False),
        pa.field("low", _DEC, nullable=False),
        pa.field("close", _DEC, nullable=False),
        pa.field("prev_close", _DEC, nullable=True),
        pa.field("volume", _DEC, nullable=True),
        pa.field("turnover", _DEC, nullable=True),
        pa.field("trades", pa.int64(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
    ]
)

ALIASES_SCHEMA = pa.schema(
    [
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("series", pa.string(), nullable=False),
        pa.field("instrument_uid", pa.string(), nullable=False),
        pa.field("valid_from", pa.date32(), nullable=False),
        pa.field("valid_to", pa.date32(), nullable=False),
    ]
)


def bars_path(root: Path, year: int, *, exchange: str = "NSE", asset_class: str = "equity") -> Path:
    return (
        root
        / "market"
        / "bars"
        / "timeframe=1d"
        / f"exchange={exchange}"
        / f"asset_class={asset_class}"
        / f"year={year}"
        / "part-0000.parquet"
    )


def aliases_path(root: Path, *, exchange: str = "NSE") -> Path:
    return root / "reference" / "aliases" / f"exchange={exchange}" / "aliases.parquet"


def _to_frame(table: pa.Table) -> pl.DataFrame:
    out = pl.from_arrow(table)
    assert isinstance(out, pl.DataFrame)
    return out


def _atomic_write(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd", row_group_size=100_000)
    os.replace(tmp, path)


def write_daily_bars(
    root: Path,
    frame: pl.DataFrame,
    *,
    exchange: str = "NSE",
    asset_class: str = "equity",
) -> dict[int, int]:
    """Merge ``frame`` into per-year part files; returns year -> rows after write."""
    if frame.height == 0:
        return {}
    frame = frame.with_columns(
        (pl.col("trade_date").cast(pl.Datetime("ns")) + pl.duration(hours=3, minutes=45))
        .dt.replace_time_zone("UTC")
        .alias("ts"),
        pl.col("trade_date").dt.year().alias("_year"),
    )
    result: dict[int, int] = {}
    for year in sorted(cast("list[int]", frame["_year"].unique().to_list())):
        new = _to_frame(
            frame.filter(pl.col("_year") == year)
            .select(BARS_SCHEMA.names)
            .to_arrow()
            .cast(BARS_SCHEMA)
        )
        path = bars_path(root, year, exchange=exchange, asset_class=asset_class)
        parts = [new.with_columns(pl.lit(0, dtype=pl.Int8).alias("_age"))]
        if path.exists():
            old = _to_frame(pq.read_table(path).cast(BARS_SCHEMA))
            parts.append(old.with_columns(pl.lit(1, dtype=pl.Int8).alias("_age")))
        # Highest source precedence wins across runs too (a mislabelled archive at a
        # year boundary must not let sec_bhavdata overwrite udiff/legacy); ties -> new.
        merged = (
            pl.concat(parts)
            .with_columns(
                pl.col("source").replace_strict(_SOURCE_PRECEDENCE, default=0).alias("_prec")
            )
            .sort(["_prec", "_age"], descending=[True, False], maintain_order=True)
            .unique(subset=["instrument_uid", "trade_date"], keep="first", maintain_order=True)
            .drop("_prec", "_age")
            .sort("instrument_uid", "ts")
        )
        _atomic_write(merged.to_arrow().cast(BARS_SCHEMA), path)
        result[year] = merged.height
    return result


def write_aliases(root: Path, frame: pl.DataFrame, *, exchange: str = "NSE") -> int:
    """Merge alias rows (symbol, series, instrument_uid, valid_from, valid_to)."""
    path = aliases_path(root, exchange=exchange)
    parts = [
        _to_frame(
            frame.with_columns(pl.lit(exchange, dtype=pl.Utf8).alias("exchange"))
            .select(ALIASES_SCHEMA.names)
            .to_arrow()
            .cast(ALIASES_SCHEMA)
        )
    ]
    if path.exists():
        parts.append(_to_frame(pq.read_table(path).cast(ALIASES_SCHEMA)))
    merged = (
        pl.concat(parts)
        .group_by("exchange", "symbol", "series", "instrument_uid")
        .agg(pl.col("valid_from").min(), pl.col("valid_to").max())
        .sort("exchange", "symbol", "series", "instrument_uid")
    )
    _atomic_write(merged.to_arrow().cast(ALIASES_SCHEMA), path)
    return merged.height


def read_daily_bars(
    root: Path, *, exchange: str = "NSE", years: list[int] | None = None
) -> pl.DataFrame:
    """Read bars (all or selected years) sorted by (instrument_uid, ts)."""
    base = root / "market" / "bars" / "timeframe=1d" / f"exchange={exchange}" / "asset_class=equity"
    tables = []
    for part in sorted(base.glob("year=*/part-0000.parquet")):
        if years is not None and int(part.parent.name.split("=")[1]) not in years:
            continue
        tables.append(_to_frame(pq.read_table(part).cast(BARS_SCHEMA)))
    if not tables:
        return _to_frame(BARS_SCHEMA.empty_table())
    return pl.concat(tables).sort("instrument_uid", "ts")
