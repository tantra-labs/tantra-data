"""Bridge exporter: machine-readable Parquet projections for the Rust runtime.

Implements the Python-side projection side of the one-bridge design (plan 0005):
it serializes the existing DuckDB filings and corporate-actions stores into
per-date Parquet files under ``{root}/catalog/filings/`` and
``{root}/catalog/corporate_actions/`` that the Rust runtime
(``tantra-runtime-api-rest``) can ingest with its existing ``arrow``/``parquet``
deps, filtering ``filing_date <= as_of`` / ``ex_date <= as_of`` for
point-in-time semantics.

Covers gaps 0007 (filings) and 0008 (corporate actions). Values keep their exact
shapes: prices/ratios are preserved as exact ``Decimal`` via
``Decimal128(18, 4)``; no floats are introduced anywhere in the projection.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data.domain.corporate_actions import (
    CorporateAction,
    CorporateActionRecord,
    to_record,
)
from tantra_data.domain.filings import Filing, FilingType

__all__ = ["LakehouseBridgeExporter"]


class FilingsRepository(Protocol):
    """Structural interface for the filings store consumed by the bridge."""

    def query(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_type: FilingType | None = None,
    ) -> list[Filing]: ...

    def close(self) -> None: ...


class CorporateActionsRepository(Protocol):
    """Structural interface for the corporate-actions store consumed by the bridge."""

    def list(
        self,
        instrument: str | None = None,
        as_of: date | None = None,
    ) -> list[CorporateAction]: ...

    def close(self) -> None: ...


class LakehouseBridgeExporter:
    """Writes one Parquet projection file per date for both bridge datasets."""

    def __init__(
        self,
        filings: FilingsRepository,
        corporate_actions: CorporateActionsRepository,
    ) -> None:
        self._filings = filings
        self._corporate_actions = corporate_actions

    def export_filings(self, root: str | Path) -> int:
        """Write ``{root}/catalog/filings/<filing_date>.parquet`` per filing date.

        Returns the number of files written. Existing files are overwritten.
        """
        root = Path(root)
        by_date: dict[date, list[Filing]] = {}
        for filing in self._filings.query():
            by_date.setdefault(filing.filing_date, []).append(filing)

        count = 0
        for filing_date, filings in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "filing_date": pa.array(
                        [f.filing_date for f in filings], type=pa.date32()
                    ),
                    "instrument": pa.array(
                        [f.instrument for f in filings], type=pa.string()
                    ),
                    "filing_type": pa.array(
                        [f.filing_type.value for f in filings], type=pa.string()
                    ),
                    "title": pa.array([f.title for f in filings], type=pa.string()),
                    "source_url": pa.array(
                        [f.source_url for f in filings], type=pa.string()
                    ),
                }
            )
            path = root / "filings" / f"{filing_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count

    def export_corporate_actions(self, root: str | Path) -> int:
        """Write ``{root}/catalog/corporate_actions/<ex_date>.parquet`` per ex-date.

        Returns the number of files written. Existing files are overwritten.
        """
        root = Path(root)
        records = [to_record(action) for action in self._corporate_actions.list()]
        by_date: dict[date, list[CorporateActionRecord]] = {}
        for record in records:
            by_date.setdefault(record.ex_date, []).append(record)

        count = 0
        for ex_date, actions in sorted(by_date.items()):
            table = pa.Table.from_pydict(
                {
                    "ex_date": pa.array(
                        [r.ex_date for r in actions], type=pa.date32()
                    ),
                    "instrument": pa.array(
                        [r.instrument for r in actions], type=pa.string()
                    ),
                    "action_type": pa.array(
                        [r.action_type.value for r in actions], type=pa.string()
                    ),
                    "old_shares": pa.array(
                        [r.old_shares for r in actions], type=pa.int32()
                    ),
                    "new_shares": pa.array(
                        [r.new_shares for r in actions], type=pa.int32()
                    ),
                    "per_share": pa.array(
                        [r.per_share for r in actions], type=pa.decimal128(18, 4)
                    ),
                    "issue_price": pa.array(
                        [r.issue_price for r in actions], type=pa.decimal128(18, 4)
                    ),
                    "currency": pa.array(
                        [r.currency for r in actions], type=pa.string()
                    ),
                }
            )
            path = root / "corporate_actions" / f"{ex_date.isoformat()}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, version="2.6")
            count += 1
        return count
