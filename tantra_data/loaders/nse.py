"""tantra_data.loaders.nse: parse NSE EOD bhavcopy CSVs into Bars, dialect-sniffed.

NSE has published its end-of-day bhavcopy under three header dialects over the years: the
current UDiFF format (``TradDt``/``TckrSymb``/``SctySrs``), the Sec Bhavdata format
(``DATE1``/``SYMBOL``/``SERIES``/``OPEN_PRICE``) and the legacy format
(``TIMESTAMP``/``SYMBOL``/``SERIES``/``OPEN``).

Only the ``EQ``, ``BE`` and ``BZ`` series survive; every other series is dropped before the
row is read. ``high`` is clamped up to at least ``max(open, close)``, ``low`` down to
``min(open, close)`` and a negative volume to ``0`` - the invariants the Parquet store's
``append`` validates, so an anomalous exchange row still stores. A missing or unparseable
date falls back to the wall clock's current IST date. An unrecognised header is not an
error: parsing returns an empty dict instead of raising.

The ``tantra`` wheel is optional: the module imports without it, and
``parse_bhavcopy_csv`` lazy-imports the wheel's ``Bar``/``InstrumentId`` types with a
clear hint that ``pip install tantra[native]`` (or ``pip install tantra``) provides them.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.request import urlopen

if TYPE_CHECKING:
    from tantra.bar import Bar

__all__ = ["NseBhavcopyProvider", "parse_bhavcopy_csv"]

_IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
_SERIES_KEPT = frozenset({"EQ", "BE", "BZ"})
_IST_OPEN_UTC = dt.time(3, 45)  # 09:15 IST session open, the store's daily-bar stamp

_WHEEL_HINT = (
    "tantra_data.loaders.nse needs the optional `tantra` wheel to build bars; install it "
    "with `pip install tantra[native]` (or `pip install tantra`)"
)


@dataclass(frozen=True)
class _Dialect:
    """One recognised bhavcopy header shape, keyed by its upper-cased field names."""

    required: frozenset[str]
    date_format: str
    date_key: str
    symbol_key: str
    series_key: str
    ohlcv_keys: tuple[str, str, str, str, str]  # open, high, low, close, volume


_DIALECTS: tuple[_Dialect, ...] = (
    _Dialect(
        required=frozenset({"TRADDT", "TCKRSYMB", "SCTYSRS"}),
        date_format="%Y-%m-%d",
        date_key="TRADDT",
        symbol_key="TCKRSYMB",
        series_key="SCTYSRS",
        ohlcv_keys=("OPNPRIC", "HGHPRIC", "LWPRIC", "CLSPRIC", "TTLTRADQTY"),
    ),
    _Dialect(
        required=frozenset({"DATE1", "SYMBOL", "SERIES", "OPEN_PRICE"}),
        date_format="%d-%m-%Y",
        date_key="DATE1",
        symbol_key="SYMBOL",
        series_key="SERIES",
        ohlcv_keys=("OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "TTL_TRD_QNTY"),
    ),
    _Dialect(
        required=frozenset({"TIMESTAMP", "SYMBOL", "SERIES", "OPEN"}),
        date_format="%d-%m-%Y",
        date_key="TIMESTAMP",
        symbol_key="SYMBOL",
        series_key="SERIES",
        ohlcv_keys=("OPEN", "HIGH", "LOW", "CLOSE", "TOTTRDQTY"),
    ),
)


def parse_bhavcopy_csv(csv_content: str, exchange: str = "NSE") -> dict[str, Bar]:
    """Bars keyed by symbol, dialect-sniffed from ``csv_content``'s header.

    See the module docstring for the three dialects recognised. Rows outside the
    ``EQ``/``BE``/``BZ`` series are dropped; the last row for a repeated symbol wins. An
    unrecognised header returns an empty dict rather than raising.

    Requires the optional ``tantra`` wheel (see :data:`_WHEEL_HINT`).
    """
    try:
        from tantra.bar import Bar
        from tantra.instrument import InstrumentId
    except ImportError as exc:
        raise ImportError(_WHEEL_HINT) from exc
    reader = csv.DictReader(io.StringIO(csv_content))
    field_map = {name.strip().upper(): name for name in reader.fieldnames or [] if name}
    upper_fields = set(field_map)

    dialect = next((d for d in _DIALECTS if d.required <= upper_fields), None)
    if dialect is None:
        return {}

    date_col = field_map[dialect.date_key]
    symbol_col = field_map[dialect.symbol_key]
    series_col = field_map[dialect.series_key]
    open_col, high_col, low_col, close_col, volume_col = (
        field_map[key] for key in dialect.ohlcv_keys
    )

    bars: dict[str, Bar] = {}
    for row in reader:
        series = (row.get(series_col) or "").strip().upper()
        if series not in _SERIES_KEPT:
            continue
        symbol = (row.get(symbol_col) or "").strip()
        day = _parse_date(row.get(date_col) or "", dialect.date_format)
        open_ = Decimal(str(row[open_col]))
        high = Decimal(str(row[high_col]))
        low = Decimal(str(row[low_col]))
        close = Decimal(str(row[close_col]))
        volume = Decimal(str(row[volume_col]))
        bars[symbol] = Bar(
            instrument_id=InstrumentId(symbol, exchange),
            ts=_session_open_ns(day),
            open=open_,
            high=max(high, open_, close),
            low=min(low, open_, close),
            close=close,
            volume=max(volume, Decimal("0")),
        )
    return bars


def _session_open_ns(day: dt.date) -> int:
    """Unix-ns timestamp of a daily bar for ``day`` (session open, as the store has it)."""
    moment = dt.datetime.combine(day, _IST_OPEN_UTC, tzinfo=dt.UTC)
    return int(moment.timestamp()) * 1_000_000_000


def _parse_date(text: str, date_format: str) -> dt.date:
    """``text`` as a calendar date in ``date_format``, or today's IST date when that fails."""
    try:
        return dt.datetime.strptime(text, date_format).date()
    except ValueError:
        return dt.datetime.now(tz=_IST).date()


class NseBhavcopyProvider:
    """Fetches NSE EOD bhavcopy publications and parses them into bars.

    Constructed with the cache directory the downloaded CSVs are kept in. This is the
    provider ``tantra_examples.sip_plan`` pairs with the yfinance provider: it has no
    ``fetch`` method (it is not a full ``MarketDataProvider``), so the service's gap fills
    fall to the yfinance provider while this one owns the bhavcopy cache.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        self.cache_dir = (
            Path(cache_dir) if cache_dir is not None else Path.cwd() / "cache" / "bhavcopy"
        )

    def download_session_bhavcopy(self, session: dt.date) -> dict[str, Bar]:
        """Download one session's NSE EOD bhavcopy and parse it into bars keyed by symbol.

        The CSV for ``session`` is fetched from NSE's archives (``sec_bhavdata_full_``),
        cached under ``self.cache_dir``, and parsed with :func:`parse_bhavcopy_csv`.
        Raises ``RuntimeError`` naming the failing step when the download or parse fails.
        """
        yyyymmdd = session.strftime("%Y%m%d")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        csv_path = self.cache_dir / f"sec_bhavdata_full_{yyyymmdd}.csv"
        if not csv_path.exists():
            url = (
                "https://nsearchives.nseindia.com/products/content/"
                f"sec_bhavdata_full_{yyyymmdd}.csv"
            )
            try:
                with urlopen(url, timeout=30) as response:
                    csv_path.write_bytes(response.read())
            except OSError as exc:
                raise RuntimeError(f"could not download NSE bhavcopy for {session}: {exc}") from exc
        return parse_bhavcopy_csv(csv_path.read_text(encoding="utf-8", errors="replace"))
