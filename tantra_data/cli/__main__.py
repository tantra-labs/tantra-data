"""Default entrypoint for `python -m tantra_data.cli`."""

import sys

from tantra_data.cli.coverage import main

if __name__ == "__main__":
    sys.exit(main())
