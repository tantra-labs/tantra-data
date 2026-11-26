"""Shareholding / pledge / insider-disclosure ingestion CLI for tantra-data (gap 0024).

Fetches the three verified-live governance/disclosure feeds and stores them in
the DuckDB store at ``{root}/shareholding.duckdb``:

* ``shareholding`` — BSE LODR Reg 31 quarterly shareholding patterns
  (``providers/bse_shareholding.py``), per BSE scripcode (default ``500325``);
* ``pledge`` — the NSE promoter-pledge snapshot for every listed company
  (``providers/nse_pledge.py``);
* ``insider`` — SEBI PIT insider-trading disclosures
  (``providers/nse_pit.py``), per NSE symbol (default ``RELIANCE``).

When a live feed fails, the provider falls back to its bundled
``source: fixture`` rows and the CLI prints a ``WARNING`` to stderr naming the
precise live failure (via ``on_live_error=...``), so a fixture fallback is
always explicit and never silently fabricated as live data.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tantra_data.providers.bse_shareholding import (
    BseShareholdingLiveError,
    fetch_shareholding_patterns,
)
from tantra_data.providers.nse_pit import NsePitLiveError, fetch_insider_disclosures
from tantra_data.providers.nse_pledge import NsePledgeLiveError, fetch_pledge_records
from tantra_data.store.shareholding import DuckDbShareholdingStore

__all__ = ["main"]

_DATASETS = ("shareholding", "pledge", "insider")
_DEFAULT_SCRIPCODES = ("500325",)
_DEFAULT_SYMBOLS = ("RELIANCE",)


def main(argv: list[str] | None = None) -> int:
    """Run the shareholding ingestion CLI; returns 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(
        prog="ingest-shareholding",
        description=(
            "Fetch BSE shareholding patterns / NSE promoter pledge / NSE PIT "
            "insider disclosures and store them in the DuckDB store at "
            "{root}/shareholding.duckdb"
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Data directory; the store lives at {root}/shareholding.duckdb",
    )
    parser.add_argument(
        "--dataset",
        choices=(*_DATASETS, "all"),
        default="all",
        help="Which feed(s) to ingest (default: all).",
    )
    parser.add_argument(
        "--scripcode",
        action="append",
        default=[],
        help="BSE scripcode(s) for the shareholding feed (repeatable). "
        f"Default: {', '.join(_DEFAULT_SCRIPCODES)}",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="NSE symbol(s) for the insider feed (repeatable). "
        f"Default: {', '.join(_DEFAULT_SYMBOLS)}",
    )
    args = parser.parse_args(argv)

    scripcodes: list[str] = [str(s) for s in (args.scripcode or _DEFAULT_SCRIPCODES)]
    symbols: list[str] = [str(s) for s in (args.symbol or _DEFAULT_SYMBOLS)]
    datasets = _DATASETS if args.dataset == "all" else (args.dataset,)

    store = DuckDbShareholdingStore(args.root)
    try:
        total = 0
        if "shareholding" in datasets:
            total += _ingest_shareholding(store, scripcodes)
        if "pledge" in datasets:
            total += _ingest_pledge(store)
        if "insider" in datasets:
            total += _ingest_insider(store, symbols)
        print(f"Total: {total} rows (datasets: {', '.join(datasets)})")
        return 0
    except Exception as exc:
        print(f"shareholding ingestion failed: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()


def _ingest_shareholding(store: DuckDbShareholdingStore, scripcodes: list[str]) -> int:
    total = 0
    for scripcode in scripcodes:
        patterns = fetch_shareholding_patterns(scripcode, on_live_error=_warn_live_error)
        store.upsert_patterns(patterns)
        total += len(patterns)
        print(f"Ingested {len(patterns)} shareholding-pattern rows for scripcode {scripcode}")
    return total


def _ingest_pledge(store: DuckDbShareholdingStore) -> int:
    records = fetch_pledge_records(on_live_error=_warn_live_error)
    store.put_pledges(records)
    print(f"Ingested {len(records)} promoter-pledge rows")
    return len(records)


def _ingest_insider(store: DuckDbShareholdingStore, symbols: list[str]) -> int:
    total = 0
    for symbol in symbols:
        disclosures = fetch_insider_disclosures(symbol, on_live_error=_warn_live_error)
        store.upsert_disclosures(disclosures)
        total += len(disclosures)
        print(f"Ingested {len(disclosures)} insider-disclosure rows for {symbol}")
    return total


def _warn_live_error(error: object) -> None:
    """Surface a precise live-fetch failure to stderr (never fabricate data)."""
    prefix = "shareholding/pledge/insider"
    if isinstance(error, BseShareholdingLiveError):
        prefix = "BSE shareholding"
    elif isinstance(error, NsePledgeLiveError):
        prefix = "NSE pledge"
    elif isinstance(error, NsePitLiveError):
        prefix = "NSE PIT insider"
    print(f"WARNING {prefix} live fetch failed: {error}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
