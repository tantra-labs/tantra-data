"""Point-in-time option-chain read port (gap 0016).

Consumers (``tantra-strategies``/``tantra-market`` gap 0005) depend on this
protocol, never on a concrete store, so DuckDB, Parquet, or future providers
are swappable without touching strategy code. Both reads are strictly
point-in-time: ``reference_ts`` bounds every row via ``as_of <= reference_ts``,
so a backtest at 10:15 only ever sees the chain known at 10:15.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable

from tantra_data.domain.option_chain import GammaExposure, StrikePositioning

__all__ = ["OptionsChainReader"]


@runtime_checkable
class OptionsChainReader(Protocol):
    """Read point-in-time option-chain snapshots and derived GEX series."""

    def chain_as_of(
        self,
        underlying: str,
        expiry: date,
        reference_ts: datetime,
    ) -> list[StrikePositioning]:
        """Return the chain known at ``reference_ts`` (latest ``as_of <=
        reference_ts`` per strike — no look-ahead)."""
        ...

    def gamma_exposure_as_of(
        self,
        underlying: str,
        expiry: date,
        reference_ts: datetime,
        lot_size: Decimal,
        spot: Decimal,
    ) -> GammaExposure | None:
        """Gamma exposure of the chain known at ``reference_ts``, or ``None``
        when no chain data exists as of that timestamp."""
        ...
