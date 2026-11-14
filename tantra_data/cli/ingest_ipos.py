"""Live NSE IPO ingestion CLI for tantra-data (gap 0022).

Fetches the NSE primary-market feeds (current / upcoming / past issues plus
per-issue detail and subscription data) via a cookie-challenged ``curl_cffi``
session and upserts issue metadata and subscription snapshots into the DuckDB
IPO stores. The default is a live fetch (network access); ``--as-of`` pins the
snapshot date.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from tantra_data.adapters.ipo_store import DuckDbIpoStore
from tantra_data.adapters.ipo_subscription_store import DuckDbIpoSubscriptionStore
from tantra_data.providers.ipos import ingest_ipos


def main(argv: list[str] | None = None) -> int:
    """Run the IPO ingestion CLI; returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="ingest-ipos",
        description=(
            "Ingest live NSE IPO issues and subscription snapshots into the DuckDB IPO stores."
        ),
    )
    parser.add_argument(
        "--db",
        default="data/ipos.duckdb",
        help="Path to the DuckDB IPO store (default: data/ipos.duckdb)",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="Snapshot date YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    issues = DuckDbIpoStore(args.db)
    subscriptions = DuckDbIpoSubscriptionStore(args.db)
    try:
        issue_count, subscription_count = ingest_ipos(issues, subscriptions, as_of)
    except (RuntimeError, OSError, ValueError) as exc:  # network / vendor / parse
        print(f"IPO ingestion failed: {exc}", file=sys.stderr)
        return 1
    finally:
        issues.close()
        subscriptions.close()
    print(f"Ingested {issue_count} issues and {subscription_count} subscription snapshots")
    return 0


if __name__ == "__main__":
    sys.exit(main())
