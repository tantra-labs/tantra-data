"""Python readers for shareholding / pledge / insider disclosures (gap 0024)."""

from __future__ import annotations

from datetime import date

from tantra_data.domain.shareholding import InsiderDisclosure, PledgeRecord, ShareholdingPattern
from tantra_data.store.shareholding import DuckDbShareholdingStore

__all__ = ["PythonInsiderDisclosureReader", "PythonPledgeReader", "PythonShareholdingReader"]


class PythonShareholdingReader:
    """Reader backed by a ``DuckDbShareholdingStore`` (satisfies the
    ``ShareholdingReader`` port)."""

    def __init__(self, store: DuckDbShareholdingStore) -> None:
        self.store = store

    def patterns_as_of(self, instrument: str, as_of: date) -> list[ShareholdingPattern]:
        """Return the pattern rows known at ``as_of`` (no look-ahead)."""
        return self.store.patterns_as_of(instrument, as_of)

    def latest_patterns(self, instrument: str) -> list[ShareholdingPattern]:
        """Return the most recent pattern rows for ``instrument``."""
        return self.store.latest_patterns(instrument)


class PythonPledgeReader:
    """Reader backed by a ``DuckDbShareholdingStore`` (satisfies the
    ``PledgeReader`` port)."""

    def __init__(self, store: DuckDbShareholdingStore) -> None:
        self.store = store

    def pledges_as_of(self, instrument: str, as_of: date) -> list[PledgeRecord]:
        """Return the pledge snapshots known at ``as_of`` (no look-ahead)."""
        return self.store.pledges_as_of(instrument, as_of)

    def latest_pledge(self, instrument: str) -> PledgeRecord | None:
        """Return the latest pledge snapshot for ``instrument``, or ``None``
        when no pledge data exists."""
        return self.store.latest_pledge(instrument)


class PythonInsiderDisclosureReader:
    """Reader backed by a ``DuckDbShareholdingStore`` (satisfies the
    ``InsiderDisclosureReader`` port)."""

    def __init__(self, store: DuckDbShareholdingStore) -> None:
        self.store = store

    def disclosures(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[InsiderDisclosure]:
        """Return insider disclosures, optionally filtered."""
        return self.store.disclosures(instrument, start_date, end_date)
