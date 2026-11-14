"""Live economic-calendar ingestion CLI for tantra-data.

This CLI is the live ingestion entry point for the T5.1 vendor decision:
the ``Economic`` calendar category is served by the Forex Factory public
weekly economic-calendar JSON feed
(``https://nfs.faireconomy.media/ff_calendar_thisweek.json``) instead of the
bundled stubbed rows. This closes docs/gaps/0012-*.md.

The default is a live fetch (network access). Use ``--limit`` to cap the
number of ingested events (e.g. in tests or smoke runs).
"""

from __future__ import annotations

import argparse
import sys

from tantra_data.adapters.duckdb_store import DuckDbCalendarStore
from tantra_data.providers.forex_factory import ingest_forex_factory

_FEED_CHOICES = ("forex-factory",)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest live economic-calendar events into the DuckDB calendar store"
    )
    parser.add_argument(
        "--db",
        default="data/calendar.duckdb",
        help="Path to the DuckDB calendar store (default: data/calendar.duckdb)",
    )
    parser.add_argument(
        "--feed",
        choices=_FEED_CHOICES,
        default="forex-factory",
        help="Live calendar feed vendor (default: forex-factory)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of events to ingest (default: all)",
    )
    args = parser.parse_args(argv)

    store = DuckDbCalendarStore(args.db)
    try:
        count = ingest_forex_factory(store, limit=args.limit)
    except RuntimeError as exc:  # network / non-200 feed failure
        print(f"calendar ingestion failed: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()
    print(f"Ingested {count} calendar events")
    return 0


if __name__ == "__main__":
    sys.exit(main())
