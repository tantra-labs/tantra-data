"""Live AMFI mutual-fund ingestion CLI for tantra-data (gap 0021).

Fetches the official AMFI daily NAV dump (``https://www.amfiindia.com/spages/
NAVAll.txt``) and upserts scheme metadata, per-scheme NAV history, and derived
CAGR returns into the DuckDB mutual-fund stores. The default is a live fetch
(network access). Fund-holdings data is deliberately NOT ingested: no free
machine-readable holdings source verified live during gap 0021's Python slice
(see ``docs/gaps/0021-*.md``).
"""

from __future__ import annotations

import argparse
import sys

from tantra_data.adapters.mutual_fund_nav_store import DuckDbMutualFundNavStore
from tantra_data.adapters.mutual_fund_return_store import DuckDbMutualFundReturnStore
from tantra_data.adapters.mutual_fund_store import DuckDbMutualFundStore
from tantra_data.providers.mutual_funds import ingest_mutual_funds


def main(argv: list[str] | None = None) -> int:
    """Run the mutual-fund ingestion CLI; returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="ingest-mutual-funds",
        description=(
            "Ingest the live AMFI daily NAV dump into the DuckDB "
            "mutual-fund scheme / NAV / return stores."
        ),
    )
    parser.add_argument(
        "--db",
        default="data/mutual_funds.duckdb",
        help="Path to the DuckDB mutual-fund store (default: data/mutual_funds.duckdb)",
    )
    args = parser.parse_args(argv)

    metas = DuckDbMutualFundStore(args.db)
    navs = DuckDbMutualFundNavStore(args.db)
    returns = DuckDbMutualFundReturnStore(args.db)
    try:
        meta_count, nav_count, return_count = ingest_mutual_funds(metas, navs, returns)
    except (RuntimeError, OSError, ValueError) as exc:  # network / vendor / parse
        print(f"mutual-fund ingestion failed: {exc}", file=sys.stderr)
        return 1
    finally:
        metas.close()
        navs.close()
        returns.close()
    print(
        f"Ingested {meta_count} schemes, {nav_count} NAV rows, and "
        f"{return_count} derived-return records"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
