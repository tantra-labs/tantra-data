"""Intraday NSE option-chain ingestion CLI for tantra-data (gap 0016).

Polls the NSE public option-chain feed
(``https://www.nseindia.com/api/option-chain-indices?symbol={symbol}``, with
the live-with-fixture-fallback provider) at a configurable interval during
market hours and upserts point-in-time snapshots into the DuckDB option-chain
store at ``{root}/option_chain.duckdb``.

``as_of`` is stamped in UTC by the provider (the NSE ``records.timestamp`` is
an IST session time, converted to UTC). The CLI maps the current wall clock to
``Asia/Kolkata`` (``datetime.now(ZoneInfo("Asia/Kolkata"))``) purely to gate
polling to the configured market-hours window (default ``9:15-15:30`` IST).

``--dry-run`` fetches a single snapshot and prints it without writing anything;
``--once`` fetches a single snapshot and exits (useful for smoke runs/tests);
the default polls every ``--interval-min`` minutes until interrupted.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from tantra_data.providers.option_chain import fetch_option_chain
from tantra_data.store.option_chain import DuckDbOptionChainStore

IST = ZoneInfo("Asia/Kolkata")

__all__ = ["main"]


def main(argv: list[str] | None = None) -> int:
    """Run the option-chain ingestion CLI; returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="ingest-option-chain",
        description=(
            "Poll the NSE option-chain feed during market hours and upsert "
            "point-in-time snapshots into the DuckDB option-chain store."
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Data directory; the store lives at {root}/option_chain.duckdb",
    )
    parser.add_argument(
        "--symbol",
        default="NIFTY",
        help="Underlying symbol to poll (default: NIFTY)",
    )
    parser.add_argument(
        "--interval-min",
        type=int,
        default=5,
        help="Poll interval in minutes (default: 5)",
    )
    parser.add_argument(
        "--hours",
        default="9:15-15:30",
        help="Market-hours window in IST, HH:MM-HH:MM (default: 9:15-15:30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch a single snapshot and print it without writing anything",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch a single snapshot and exit (default: poll until interrupted)",
    )
    args = parser.parse_args(argv)

    try:
        open_time, close_time = _parse_hours(args.hours)
    except ValueError as exc:
        print(f"invalid --hours: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        snapshot = fetch_option_chain(args.symbol)
        print(
            "DRY-RUN "
            f"{snapshot.as_of.isoformat()} {snapshot.underlying} "
            f"expiry={snapshot.expiry} strikes={len(snapshot.strikes)} "
            f"source={snapshot.source}"
        )
        return 0

    store = DuckDbOptionChainStore(args.root)
    try:
        if args.once:
            _ingest_once(store, args.symbol)
            return 0
        interval = max(args.interval_min, 1) * 60
        print(
            f"Polling {args.symbol} every {args.interval_min} minute(s) during "
            f"{args.hours} IST; Ctrl-C to stop"
        )
        while True:
            now_ist = datetime.now(IST)
            if open_time <= now_ist.time() <= close_time:
                _ingest_once(store, args.symbol)
            else:
                print(f"{now_ist:%H:%M:%S} IST outside market hours ({args.hours}); waiting")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("interrupted")
        return 0
    except Exception as exc:
        print(f"option-chain ingestion failed: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()


def _ingest_once(store: DuckDbOptionChainStore, symbol: str) -> int:
    snapshot = fetch_option_chain(symbol)
    store.upsert_snapshot(snapshot)
    print(
        f"Upserted {len(snapshot.strikes)} strikes for {symbol} at "
        f"{snapshot.as_of.isoformat()} (source={snapshot.source})"
    )
    return len(snapshot.strikes)


def _parse_hours(text: str) -> tuple[dtime, dtime]:
    try:
        open_text, close_text = text.split("-")
        open_time = _parse_clock(open_text)
        close_time = _parse_clock(close_text)
    except ValueError as exc:
        raise ValueError(f"expected HH:MM-HH:MM, got {text!r}") from exc
    if open_time >= close_time:
        raise ValueError(f"market-hours window must have open < close: {text!r}")
    return open_time, close_time


def _parse_clock(text: str) -> dtime:
    hour_text, minute_text = text.split(":")
    hour = int(hour_text)
    minute = int(minute_text)
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError(f"invalid clock time {text!r}")
    return dtime(hour, minute)


if __name__ == "__main__":
    sys.exit(main())
