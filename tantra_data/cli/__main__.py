"""Default entrypoint for `python -m tantra_data.cli`.

Without a subcommand this delegates to the coverage tool (backward compatible);
with a subcommand it dispatches to the matching CLI module (``ingest-calendar``,
``ingest-fundamentals``, ``export-bridge``, ``coverage``). Bare coverage options
such as ``python -m tantra_data.cli --catalog data/catalog`` continue to work.
"""

from __future__ import annotations

import sys

from tantra_data.cli.coverage import main as coverage_main
from tantra_data.cli.export_bridge import main as export_bridge_main
from tantra_data.cli.ingest_calendar import main as ingest_calendar_main
from tantra_data.cli.ingest_fundamentals import main as ingest_fundamentals_main

_COMMANDS = {
    "coverage": coverage_main,
    "export-bridge": export_bridge_main,
    "ingest-calendar": ingest_calendar_main,
    "ingest-fundamentals": ingest_fundamentals_main,
}


def main(argv: list[str] | None = None) -> int:
    """Run the CLI, dispatching to a subcommand or the coverage tool."""
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        return coverage_main()
    command, *rest = args
    handler = _COMMANDS.get(command)
    if handler is None:
        return coverage_main(args)
    return handler(rest)


if __name__ == "__main__":
    sys.exit(main())
