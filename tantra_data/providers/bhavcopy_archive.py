"""Parser for NSE daily bhavcopy archives (UDiFF, legacy cm bhav, sec bhavdata).

All monetary values go through ``decimal.Decimal`` (never float) and land in
``Decimal(18, 4)`` polars columns. The in-file trade date is authoritative; the
archive filename date is not reliable.
"""

from __future__ import annotations

import csv
import enum
import io
import logging
import lzma
import zipfile
from bisect import bisect_right
from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import polars as pl

KEEP_SERIES = frozenset({"EQ", "BE", "BZ"})
_Q = Decimal("0.0001")
_DEC = pl.Decimal(18, 4)

IsinIndex = dict[str, list[tuple[date, str]]]

FRAME_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "isin": pl.Utf8,
    "symbol": pl.Utf8,
    "series": pl.Utf8,
    "trade_date": pl.Date,
    "open": _DEC,
    "high": _DEC,
    "low": _DEC,
    "close": _DEC,
    "prev_close": _DEC,
    "volume": _DEC,
    "turnover": _DEC,
    "trades": pl.Int64,
}


_log = logging.getLogger(__name__)


class BhavcopyArchiveError(ValueError):
    """Raised when a bhavcopy archive cannot be parsed."""


class Dialect(enum.Enum):
    """Archive header dialects; the value is the dedupe precedence."""

    UDIFF = 3
    LEGACY = 2
    SEC_BHAVDATA = 1


_UDIFF_KEYS = frozenset(
    {
        "TRADDT",
        "FININSTRMTP",
        "ISIN",
        "TCKRSYMB",
        "SCTYSRS",
        "OPNPRIC",
        "HGHPRIC",
        "LWPRIC",
        "CLSPRIC",
        "PRVSCLSGPRIC",
        "TTLTRADGVOL",
        "TTLTRFVAL",
        "TTLNBOFTXSEXCTD",
    }
)
_LEGACY_KEYS = frozenset(
    {
        "SYMBOL",
        "SERIES",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "PREVCLOSE",
        "TOTTRDQTY",
        "TOTTRDVAL",
        "TIMESTAMP",
        "TOTALTRADES",
        "ISIN",
    }
)
_SEC_KEYS = frozenset(
    {
        "SYMBOL",
        "SERIES",
        "DATE1",
        "PREV_CLOSE",
        "OPEN_PRICE",
        "HIGH_PRICE",
        "LOW_PRICE",
        "CLOSE_PRICE",
        "TTL_TRD_QNTY",
        "TURNOVER_LACS",
        "NO_OF_TRADES",
    }
)


def _detect(header: set[str]) -> Dialect | None:
    if header >= _UDIFF_KEYS:
        return Dialect.UDIFF
    if header >= _LEGACY_KEYS:
        return Dialect.LEGACY
    if header >= _SEC_KEYS:
        return Dialect.SEC_BHAVDATA
    return None


def _dec(raw: str, field: str, *, required: bool) -> Decimal | None:
    s = raw.strip()
    if s in ("", "-"):
        if required:
            raise BhavcopyArchiveError(f"missing mandatory value for {field}")
        return None
    try:
        d = Decimal(s)
        if not d.is_finite():
            raise InvalidOperation
        return d.quantize(_Q)
    except InvalidOperation as exc:
        raise BhavcopyArchiveError(f"unparseable {field}: {raw!r}") from exc


def _int(raw: str) -> int | None:
    s = raw.strip()
    if s in ("", "-"):
        return None
    try:
        return int(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def _date(raw: str, fmt: str) -> date:
    try:
        return datetime.strptime(raw.strip(), fmt).date()
    except ValueError as exc:
        raise BhavcopyArchiveError(f"unparseable date: {raw!r}") from exc


def parse_archive_bhavcopy(text: str) -> tuple[Dialect, pl.DataFrame]:
    """Parse one bhavcopy CSV text into a normalised frame."""
    reader = csv.reader(io.StringIO(text))
    try:
        header_row = next(reader)
    except StopIteration as exc:
        raise BhavcopyArchiveError("empty bhavcopy file") from exc
    header = [h.strip().upper() for h in header_row]
    dialect = _detect({h for h in header if h})
    if dialect is None:
        raise BhavcopyArchiveError(f"unknown bhavcopy header: {header_row[:6]!r}")

    cols: dict[str, list[object]] = {k: [] for k in FRAME_SCHEMA}
    for row in reader:
        if not any(c.strip() for c in row):
            continue
        r = {h: c.strip() for h, c in zip(header, row, strict=False) if h}
        if dialect is Dialect.UDIFF:
            if r.get("FININSTRMTP", "") != "STK":
                continue
            series = r.get("SCTYSRS", "")
            isin: str | None = r.get("ISIN") or None
            symbol = r.get("TCKRSYMB", "")
            tdate = _date(r["TRADDT"], "%Y-%m-%d")
            o, h_, lo, c_, pc = (
                r.get("OPNPRIC", ""),
                r.get("HGHPRIC", ""),
                r.get("LWPRIC", ""),
                r.get("CLSPRIC", ""),
                r.get("PRVSCLSGPRIC", ""),
            )
            vol, tov, trd = (
                r.get("TTLTRADGVOL", ""),
                r.get("TTLTRFVAL", ""),
                r.get("TTLNBOFTXSEXCTD", ""),
            )
            turnover = _dec(tov, "turnover", required=False)
        elif dialect is Dialect.LEGACY:
            series = r.get("SERIES", "")
            isin = r.get("ISIN") or None
            symbol = r.get("SYMBOL", "")
            tdate = _date(r["TIMESTAMP"], "%d-%b-%Y")
            o, h_, lo, c_, pc = r["OPEN"], r["HIGH"], r["LOW"], r["CLOSE"], r["PREVCLOSE"]
            vol, tov, trd = r["TOTTRDQTY"], r["TOTTRDVAL"], r["TOTALTRADES"]
            turnover = _dec(tov, "turnover", required=False)
        else:
            series = r.get("SERIES", "")
            isin = None
            symbol = r.get("SYMBOL", "")
            tdate = _date(r["DATE1"], "%d-%b-%Y")
            o, h_, lo, c_, pc = (
                r["OPEN_PRICE"],
                r["HIGH_PRICE"],
                r["LOW_PRICE"],
                r["CLOSE_PRICE"],
                r["PREV_CLOSE"],
            )
            vol, tov, trd = r["TTL_TRD_QNTY"], r["TURNOVER_LACS"], r["NO_OF_TRADES"]
            lacs = _dec(tov, "turnover", required=False)
            turnover = None if lacs is None else (lacs * 100000).quantize(_Q)
        if series not in KEEP_SERIES:
            continue
        open_ = _dec(o, "open", required=False)
        high = _dec(h_, "high", required=False)
        low = _dec(lo, "low", required=False)
        close = _dec(c_, "close", required=False)
        if open_ is None or high is None or low is None or close is None:
            # A suspended/untraded row ("-" OHLC) is skipped on its own rather than
            # failing the whole session; nothing is fabricated in its place.
            _log.warning("skipping %s %s on %s: missing OHLC", symbol, series, tdate)
            continue
        prev = _dec(pc, "prev_close", required=False)
        volume = _dec(vol, "volume", required=False)
        if volume is not None and volume < 0:
            volume = Decimal("0.0000")
        cols["isin"].append(isin)
        cols["symbol"].append(symbol)
        cols["series"].append(series)
        cols["trade_date"].append(tdate)
        cols["open"].append(open_)
        cols["high"].append(max(high, open_, close))
        cols["low"].append(min(low, open_, close))
        cols["close"].append(close)
        cols["prev_close"].append(prev)
        cols["volume"].append(volume)
        cols["turnover"].append(turnover)
        cols["trades"].append(_int(trd))
    return dialect, pl.DataFrame(cols, schema=FRAME_SCHEMA)


def read_archive_file(path: Path) -> tuple[Dialect, pl.DataFrame]:
    """Read and parse a ``.csv.xz`` / ``.csv`` / ``.zip`` bhavcopy archive."""
    name = path.name.lower()
    try:
        if name.endswith(".xz"):
            raw = lzma.decompress(path.read_bytes())
        elif name.endswith(".zip"):
            with zipfile.ZipFile(path) as zf:
                raw = zf.read(zf.namelist()[0])
        else:
            raw = path.read_bytes()
        return parse_archive_bhavcopy(raw.decode("utf-8", errors="replace"))
    except BhavcopyArchiveError as exc:
        raise BhavcopyArchiveError(f"{path}: {exc}") from exc
    except (lzma.LZMAError, zipfile.BadZipFile, IndexError) as exc:
        raise BhavcopyArchiveError(f"{path}: unreadable archive: {exc}") from exc


def isin_index_from_frames(frames: Iterable[pl.DataFrame]) -> IsinIndex:
    """Build symbol -> sorted [(trade_date, isin)] from ISIN-bearing frames."""
    seen: dict[str, set[tuple[date, str]]] = {}
    for frame in frames:
        sub = frame.filter(pl.col("isin").is_not_null()).select("symbol", "trade_date", "isin")
        for sym, d, isin in sub.iter_rows():
            seen.setdefault(sym, set()).add((d, isin))
    return {sym: sorted(v) for sym, v in seen.items()}


def resolve_missing_isins(frame: pl.DataFrame, index: IsinIndex) -> tuple[pl.DataFrame, int]:
    """Fill null ISINs from the nearest observation; drop (and count) unresolved rows."""
    nulls = frame.filter(pl.col("isin").is_null())
    if nulls.height == 0:
        return frame, 0
    resolved: list[str | None] = []
    for sym, d in nulls.select("symbol", "trade_date").iter_rows():
        obs = index.get(sym)
        if not obs:
            resolved.append(None)
            continue
        i = bisect_right(obs, d, key=lambda t: t[0])
        resolved.append(obs[i - 1][1] if i > 0 else obs[0][1])
    fixed = nulls.with_columns(pl.Series("isin", resolved, dtype=pl.Utf8))
    fixed = fixed.filter(pl.col("isin").is_not_null())
    dropped = nulls.height - fixed.height
    out = pl.concat([frame.filter(pl.col("isin").is_not_null()), fixed])
    return out, dropped


def merge_sessions(frames: list[tuple[Dialect, pl.DataFrame]]) -> pl.DataFrame:
    """Concat frames and dedupe on (isin, trade_date), keeping the top dialect."""
    if not frames:
        return pl.DataFrame(schema={**FRAME_SCHEMA, "source": pl.Utf8})
    tagged = [
        f.with_columns(
            pl.lit(d.name.lower(), dtype=pl.Utf8).alias("source"),
            pl.lit(d.value, dtype=pl.Int64).alias("_prec"),
        )
        for d, f in frames
    ]
    merged = pl.concat(tagged).sort("_prec", descending=True, maintain_order=True)
    return merged.unique(subset=["isin", "trade_date"], keep="first", maintain_order=True).drop(
        "_prec"
    )
