"""tantra_data.loaders.yfinance: daily bars for a symbol via the optional ``yfinance`` package.

``yfinance`` is an optional dependency (see
:func:`tantra_data.providers.profile.yfinance_available`): this module imports without it,
and :meth:`YFinanceProvider.fetch` raises a clear ``RuntimeError`` when it is not
importable. The module does not need the ``tantra`` wheel either - it is imported only
inside :meth:`YFinanceProvider.fetch` to construct the wheel's ``Bar`` objects, with a
hint that ``pip install tantra[native]`` (or ``pip install tantra``) provides them.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

from tantra_data.providers.profile import yfinance_available

__all__ = ["YFinanceProvider"]

_IST_OPEN_UTC = dt.time(3, 45)  # 09:15 IST session open, the store's daily-bar stamp

_WHEEL_HINT = (
    "tantra_data.loaders.yfinance needs the optional `tantra` wheel to build bars; install "
    "it with `pip install tantra[native]` (or `pip install tantra`)"
)


def _session_open_ns(day: dt.date) -> int:
    """Unix-ns timestamp of a daily bar for ``day`` (session open, as the store has it)."""
    moment = dt.datetime.combine(day, _IST_OPEN_UTC, tzinfo=dt.UTC)
    return int(moment.timestamp()) * 1_000_000_000


class YFinanceProvider:
    """Fetches daily bars for a symbol from Yahoo Finance via ``yfinance``.

    When ``yfinance`` is not importable, :meth:`fetch` raises a ``RuntimeError`` naming it
    instead of failing obscurely. Prices are exact ``decimal.Decimal`` (zero-floats policy);
    bars are stamped at the 09:15 IST session open the store uses for daily bars.
    """

    name = "yfinance"

    def fetch(
        self, inst: Any, timeframe: str, interval: Any, progress_callback: Any = None
    ) -> list[Any]:
        """Daily ``Bar`` objects for ``inst.symbol`` covering ``interval``.

        ``inst`` is a wheel :class:`tantra.instrument.InstrumentId` and ``interval`` a wheel
        :class:`tantra.coverage.DateInterval`, as the service's ``MarketDataProvider``
        protocol passes them. Only ``1D`` bars are served.
        """
        if not yfinance_available():
            raise RuntimeError(
                "yfinance is not installed; install it with `pip install yfinance` to "
                "fetch daily bars from Yahoo Finance"
            )
        if timeframe.upper() != "1D":
            raise ValueError(f"YFinanceProvider only serves daily ('1D') bars, got {timeframe!r}")
        try:
            from tantra.bar import Bar
        except ImportError as exc:
            raise ImportError(_WHEEL_HINT) from exc

        import yfinance as yf

        data = yf.Ticker(inst.symbol).history(
            start=interval.start.isoformat(),
            end=interval.end.isoformat(),
            interval="1d",
            auto_adjust=False,
        )
        if data is None or data.empty:
            return []
        bars: list[Any] = []
        for ts, row in data.iterrows():
            if not isinstance(ts, dt.datetime):
                raise TypeError(f"Expected datetime index from yfinance, got {type(ts).__name__}")
            bars.append(
                Bar(
                    instrument_id=inst,
                    ts=_session_open_ns(ts.date()),
                    open=Decimal(str(row["Open"])),
                    high=Decimal(str(row["High"])),
                    low=Decimal(str(row["Low"])),
                    close=Decimal(str(row["Close"])),
                    volume=Decimal(str(row["Volume"])),
                )
            )
        return bars
