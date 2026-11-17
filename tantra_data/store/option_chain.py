"""DuckDB-backed repository for intraday option-chain snapshots (gap 0016).

The store lives in a single DuckDB database at ``{root}/option_chain.duckdb``.
Snapshots are idempotent on ``(as_of, underlying, expiry, strike,
option_type)``; the point-in-time ``chain_as_of`` read keeps the latest
per-strike row with ``as_of <= reference_ts`` (no look-ahead), which is what
the ``OptionsChainReader`` port (``tantra_data/ports/option_chain.py``)
consumes.

Zero-floats invariant: every financial column is an exact ``DECIMAL(18, 4)``
``decimal.Decimal`` — no ``float`` anywhere.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from tantra_data.domain.option_chain import (
    GammaExposure,
    OptionChainSnapshot,
    StrikePositioning,
    compute_gamma_exposure,
)

__all__ = ["DuckDbOptionChainStore"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS option_chain_snapshots (
    as_of TIMESTAMP WITH TIME ZONE,
    underlying VARCHAR,
    expiry DATE,
    strike DECIMAL(18, 4),
    option_type VARCHAR,
    open_interest DECIMAL(18, 4),
    oi_change DECIMAL(18, 4),
    implied_volatility DECIMAL(18, 4),
    delta DECIMAL(18, 4),
    gamma DECIMAL(18, 4)
)
"""

_COLUMNS = (
    "as_of, underlying, expiry, strike, option_type, open_interest, oi_change, "
    "implied_volatility, delta, gamma"
)


class DuckDbOptionChainStore:
    """DuckDB point-in-time store for intraday option-chain snapshots.

    Mirrors the existing ``institutional_flow_store`` pattern: the table is
    created in ``__init__``, ``upsert_snapshot`` DELETE-then-INSERTs per
    ``(as_of, underlying, expiry, strike, option_type)``, and the read helpers
    return typed domain rows with no look-ahead.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self._root / "option_chain.duckdb"))
        self._connection.execute(CREATE_TABLE_SQL)

    def upsert_snapshot(self, snapshot: OptionChainSnapshot) -> None:
        """Persist ``snapshot`` idempotently on ``(as_of, underlying, expiry,
        strike, option_type)`` — re-upserting a snapshot never duplicates rows."""
        for positioning in snapshot.strikes:
            self._connection.execute(
                "DELETE FROM option_chain_snapshots "
                "WHERE as_of = ? AND underlying = ? AND expiry = ? "
                "AND strike = ? AND option_type = ?",
                [
                    snapshot.as_of,
                    snapshot.underlying,
                    snapshot.expiry,
                    positioning.strike,
                    positioning.option_type,
                ],
            )
            insert_sql = (
                f"INSERT INTO option_chain_snapshots ({_COLUMNS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            self._connection.execute(
                insert_sql,
                [
                    snapshot.as_of,
                    snapshot.underlying,
                    snapshot.expiry,
                    positioning.strike,
                    positioning.option_type,
                    positioning.open_interest,
                    positioning.oi_change,
                    positioning.implied_volatility,
                    positioning.delta,
                    positioning.gamma,
                ],
            )

    def chain_as_of(
        self,
        underlying: str,
        expiry: date,
        reference_ts: datetime,
    ) -> list[StrikePositioning]:
        """Return the option chain known at ``reference_ts`` (no look-ahead).

        For every ``(strike, option_type)`` this keeps the latest row with
        ``as_of <= reference_ts``, ordered by strike then option type. Strikes
        that existed before ``reference_ts`` but were dropped from a later
        snapshot remain visible (carried forward), exactly as a point-in-time
        consumer expects.
        """
        rows = self._connection.execute(
            "SELECT as_of, underlying, expiry, strike, option_type, open_interest, "
            "oi_change, implied_volatility, delta, gamma FROM ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY strike, option_type ORDER BY as_of DESC"
            "  ) AS rn"
            "  FROM option_chain_snapshots"
            "  WHERE underlying = ? AND expiry = ? AND as_of <= ?"
            ") WHERE rn = 1 ORDER BY strike, option_type",
            [underlying, expiry, reference_ts],
        ).fetchall()
        return [self._to_positioning(row) for row in rows]

    def snapshots_between(
        self,
        underlying: str | None = None,
        expiry: date | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[OptionChainSnapshot]:
        """Return full snapshots within the optional filters.

        Every argument is optional; ``snapshots_between()`` returns every
        snapshot. Rows are grouped by their UTC ``as_of`` instant, so each
        returned snapshot matches one ingested chain observation.
        """
        clauses: list[str] = []
        params: list[object] = []
        if underlying is not None:
            clauses.append("underlying = ?")
            params.append(underlying)
        if expiry is not None:
            clauses.append("expiry = ?")
            params.append(expiry)
        if start is not None:
            clauses.append("as_of >= ?")
            params.append(start)
        if end is not None:
            clauses.append("as_of <= ?")
            params.append(end)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM option_chain_snapshots{where_clause} "
            "ORDER BY as_of, strike, option_type",
            params,
        ).fetchall()

        by_as_of: dict[datetime, tuple[str, date, list[StrikePositioning]]] = {}
        for row in rows:
            as_of = self._as_utc(row[0])
            group = by_as_of.get(as_of)
            if group is None:
                group = (row[1], row[2], [])
                by_as_of[as_of] = group
            group[2].append(self._to_positioning(row))
        return [
            OptionChainSnapshot(
                as_of=as_of, underlying=underlying_symbol, expiry=expiry_date, strikes=strikes
            )
            for as_of, (underlying_symbol, expiry_date, strikes) in by_as_of.items()
        ]

    def gamma_exposure_as_of(
        self,
        underlying: str,
        expiry: date,
        reference_ts: datetime,
        lot_size: Decimal,
        spot: Decimal,
    ) -> GammaExposure | None:
        """Gamma exposure of the chain known at ``reference_ts``.

        Returns ``None`` when no chain data exists as of ``reference_ts``.
        """
        strikes = self.chain_as_of(underlying, expiry, reference_ts)
        if not strikes:
            return None
        snapshot = OptionChainSnapshot(
            as_of=reference_ts,
            underlying=underlying,
            expiry=expiry,
            strikes=strikes,
        )
        return compute_gamma_exposure(snapshot, lot_size=lot_size, spot=spot)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        """Normalize a DuckDB ``TIMESTAMP WITH TIME ZONE`` back to UTC.

        DuckDB returns tz-aware datetimes in the session timezone; the instant
        is preserved, so converting to UTC makes keys and filenames
        deterministic regardless of the host timezone.
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @staticmethod
    def _to_positioning(row: tuple[object, ...]) -> StrikePositioning:
        return StrikePositioning(
            strike=row[3],
            option_type=row[4],
            open_interest=row[5],
            oi_change=row[6],
            implied_volatility=row[7],
            delta=row[8],
            gamma=row[9],
        )
