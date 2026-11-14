"""CLI to export the filings / corporate-actions lakehouse bridge projections.

Mirrors ``cli/coverage.py``: an argparse entry point that reads the shared
``data/bridge.duckdb`` store and writes per-date Parquet projections under the
catalog root via
:class:`~tantra_data.service.bridge_exporter.LakehouseBridgeExporter`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tantra_data.adapters.block_deals_store import DuckDbBlockDealsStore
from tantra_data.adapters.delivery_store import DuckDbDeliveryStore
from tantra_data.adapters.duckdb_store import (
    DuckDbCalendarStore,
    DuckDbCorporateActionsStore,
)
from tantra_data.adapters.filings_store import DuckDbFilingsStore
from tantra_data.adapters.institutional_flow_store import DuckDbInstitutionalFlowStore
from tantra_data.adapters.news_store import DuckDbNewsStore
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter


def main(argv: list[str] | None = None) -> int:
    """Run the bridge export CLI; returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="export-bridge",
        description=(
            "Export filings and corporate-actions Parquet projections from "
            "the shared DuckDB bridge store."
        ),
    )
    parser.add_argument(
        "--db",
        default="data/bridge.duckdb",
        help="Shared DuckDB bridge store path.",
    )
    parser.add_argument(
        "--catalog",
        default="data/catalog",
        help="Catalog root directory that receives the projections.",
    )
    parser.add_argument(
        "--dataset",
        choices=(
            "filings",
            "corporate-actions",
            "block-deals",
            "news",
            "institutional-flows",
            "delivery",
            "economic-calendar",
            "all",
        ),
        default="all",
        help="Which projection(s) to export.",
    )
    args = parser.parse_args(argv)

    db_path = Path(args.db)
    catalog_root = Path(args.catalog)

    filings = DuckDbFilingsStore(str(db_path))
    corporate_actions = DuckDbCorporateActionsStore(str(db_path))
    block_deals = DuckDbBlockDealsStore(str(db_path))
    news = DuckDbNewsStore(str(db_path))
    institutional_flows = DuckDbInstitutionalFlowStore(str(db_path))
    delivery = DuckDbDeliveryStore(str(db_path))
    calendar = DuckDbCalendarStore(str(db_path))
    exporter = LakehouseBridgeExporter(
        filings,
        corporate_actions,
        block_deals,
        news,
        institutional_flows=institutional_flows,
        delivery=delivery,
        calendar=calendar,
    )
    try:
        if args.dataset in ("filings", "all"):
            n_filings = exporter.export_filings(catalog_root)
            print(f"Exported {n_filings} filings files")
        if args.dataset in ("corporate-actions", "all"):
            n_actions = exporter.export_corporate_actions(catalog_root)
            print(f"Exported {n_actions} corporate-action files")
        if args.dataset in ("block-deals", "all"):
            n_deals = exporter.export_block_deals(catalog_root)
            print(f"Exported {n_deals} block-deal files")
        if args.dataset in ("news", "all"):
            n_news = exporter.export_news(catalog_root)
            print(f"Exported {n_news} news files")
        if args.dataset in ("institutional-flows", "all"):
            n_flows = exporter.export_institutional_flows(catalog_root)
            print(f"Exported {n_flows} institutional-flow files")
        if args.dataset in ("delivery", "all"):
            n_delivery = exporter.export_delivery(catalog_root)
            print(f"Exported {n_delivery} delivery files")
        if args.dataset in ("economic-calendar", "all"):
            n_events = exporter.export_economic_calendar(catalog_root)
            print(f"Exported {n_events} economic-calendar files")
        return 0
    except Exception as exc:
        print(f"export failed: {exc}", file=sys.stderr)
        return 1
    finally:
        filings.close()
        corporate_actions.close()
        block_deals.close()
        news.close()
        institutional_flows.close()
        delivery.close()
        calendar.close()


if __name__ == "__main__":
    sys.exit(main())
