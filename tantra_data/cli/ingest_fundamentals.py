"""Live company-fundamentals ingestion CLI for tantra-data (gap 0020).

Fetches financial statements (annual + quarterly income / balance-sheet /
cash-flow line items) and a point-in-time ratios snapshot from yfinance
(``yfinance.Ticker("<SYMBOL>.NS")``) and upserts them into the DuckDB
financial-statement and fundamental-ratios stores. The default is a live fetch
(network access); ``--as-of`` pins the point-in-time filing date.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from tantra_data.adapters.fundamental_ratios_store import DuckDbFundamentalRatiosStore
from tantra_data.adapters.fundamental_statement_store import DuckDbFinancialStatementStore
from tantra_data.providers.fundamentals import ingest_fundamentals


def main(argv: list[str] | None = None) -> int:
    """Run the fundamentals ingestion CLI; returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="ingest-fundamentals",
        description=(
            "Ingest live company financial statements and valuation ratios "
            "into the DuckDB fundamentals stores."
        ),
    )
    parser.add_argument(
        "--db",
        default="data/fundamentals.duckdb",
        help="Path to the DuckDB fundamentals store (default: data/fundamentals.duckdb)",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        required=True,
        help="NSE symbol suffix to ingest (e.g. RELIANCE); repeatable for batches",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="Point-in-time filing date YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    statements = DuckDbFinancialStatementStore(args.db)
    ratios = DuckDbFundamentalRatiosStore(args.db)
    total_statements = 0
    total_ratios = 0
    try:
        for symbol in args.symbol:
            try:
                statement_count, ratio_count = ingest_fundamentals(
                    statements, ratios, symbol, as_of
                )
            except (RuntimeError, ImportError) as exc:  # network / vendor / missing dep
                print(f"fundamentals ingestion failed for {symbol}: {exc}", file=sys.stderr)
                return 1
            total_statements += statement_count
            total_ratios += ratio_count
            print(
                f"Ingested {statement_count} statement rows and {ratio_count} "
                f"ratio snapshot for {symbol}"
            )
    finally:
        statements.close()
        ratios.close()
    print(f"Ingested {total_statements} statement rows and {total_ratios} ratio snapshots")
    return 0


if __name__ == "__main__":
    sys.exit(main())
