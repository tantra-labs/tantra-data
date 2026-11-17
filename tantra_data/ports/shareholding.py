"""Shareholding / pledge / insider-disclosure read ports (gap 0024).

Consumers (screener categories, insider-deal alerts) depend on these protocols,
never on a concrete store, so DuckDB, Parquet, or future providers are
swappable without touching consumer code.

All reads are strictly point-in-time: ``as_of`` bounds every row via
``filing_date <= as_of`` / ``broadcast_date <= as_of`` /
``intimation_date <= as_of``, so a backtest at date ``d`` only ever sees the
disclosures that were public at ``d`` — no look-ahead.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from tantra_data.domain.shareholding import InsiderDisclosure, PledgeRecord, ShareholdingPattern

__all__ = ["InsiderDisclosureReader", "PledgeReader", "ShareholdingReader"]


@runtime_checkable
class ShareholdingReader(Protocol):
    """Read point-in-time per-category shareholding patterns."""

    def patterns_as_of(
        self,
        instrument: str,
        as_of: date,
    ) -> list[ShareholdingPattern]:
        """Return the pattern rows known at ``as_of`` (latest ``filing_date <=
        as_of`` per ``(company, quarter_end, category)`` — no look-ahead)."""
        ...

    def latest_patterns(self, instrument: str) -> list[ShareholdingPattern]:
        """Return the most recent pattern rows for ``instrument``."""
        ...


@runtime_checkable
class PledgeReader(Protocol):
    """Read point-in-time promoter-pledge snapshots."""

    def pledges_as_of(
        self,
        instrument: str,
        as_of: date,
    ) -> list[PledgeRecord]:
        """Return the pledge snapshots known at ``as_of`` (latest
        ``broadcast_date <= as_of`` per quarter — no look-ahead)."""
        ...

    def latest_pledge(self, instrument: str) -> PledgeRecord | None:
        """Return the latest pledge snapshot for ``instrument``, or ``None``
        when no pledge data exists."""
        ...


@runtime_checkable
class InsiderDisclosureReader(Protocol):
    """Read insider-trading disclosures."""

    def disclosures(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[InsiderDisclosure]:
        """Return insider disclosures, optionally filtered by instrument and
        an intimation-date window."""
        ...
