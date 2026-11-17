"""NSE trading-holiday ingestion CLI for tantra-data (gap 0015).

Fetches the NSE trading-holiday master via the live-with-fixture-fallback
provider (``providers/trading_holidays.py``) and stores it in the DuckDB store
at ``{root}/trading_holidays.duckdb``. Defaults to the current and next
calendar year (holidays are announced in advance, so ingesting next year's
master up front is the point).

When the live feed fails (the sandbox has never been able to verify it — see
``providers/trading_holidays.py``), the provider falls back to the bundled
``source: fixture`` rows and the CLI prints a ``WARNING`` to stderr naming the
precise live failure (via ``fetch_trading_holidays(on_live_error=...)``), so a
fixture fallback is always explicit and never silently fabricated as live data.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from tantra_data.providers.trading_holidays import (
    TradingHolidayLiveError,
    fetch_trading_holidays,
)
from tantra_data.store.trading_holidays import DuckDbTradingHolidayStore

__all__ = ["main"]


def main(argv: list[str] | None = None) -> int:
    """Run the trading-holiday ingestion CLI; returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="ingest-trading-holidays",
        description=(
            "Fetch the NSE trading-holiday master (live with fixture fallback) "
            "and store it in the DuckDB store at {root}/trading_holidays.duckdb"
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Data directory; the store lives at {root}/trading_holidays.duckdb",
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        default=[],
        help="Calendar year(s) to ingest (repeatable). Default: current and next year",
    )
    args = parser.parse_args(argv)

    years = args.year or default_years()
    store = DuckDbTradingHolidayStore(args.root)
    try:
        total = 0
        for year in sorted(set(years)):
            holidays = fetch_trading_holidays(year, on_live_error=_warn_live_error)
            store.put(holidays)
            total += len(holidays)
            print(f"Ingested {len(holidays)} trading holidays for {year}")
        years_text = ", ".join(str(y) for y in sorted(set(years)))
        print(f"Total: {total} trading holidays (years: {years_text})")
        return 0
    except Exception as exc:
        print(f"trading-holiday ingestion failed: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()


def default_years() -> list[int]:
    """Current and next calendar year (UTC) — the default ingestion window."""
    now_year = datetime.now(UTC).year
    return [now_year, now_year + 1]


def _warn_live_error(error: TradingHolidayLiveError) -> None:
    """Surface a precise live-fetch failure to stderr (never fabricate data)."""
    print(f"WARNING trading-holiday live fetch failed: {error}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
