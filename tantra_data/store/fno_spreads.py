"""DuckDB point-in-time store for F&O futures basis and option bid-ask spreads.

Persists snapshots under ``{root}/fno_spreads.duckdb`` using ``DECIMAL(18, 4)``
columns (zero floats) and provides point-in-time queries with ``as_of <= reference_ts``
so historical studies never suffer look-ahead bias.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from tantra_data.domain.fno_spreads import FuturesBasisSnapshot, OptionSpreadSnapshot

__all__ = ["DuckDbFnoSpreadStore"]

CREATE_FUTURES_BASIS_SQL = """
CREATE TABLE IF NOT EXISTS futures_basis_snapshots (
    as_of TIMESTAMP WITH TIME ZONE,
    underlying VARCHAR,
    spot_price DECIMAL(18, 4),
    near_expiry DATE,
    near_futures_price DECIMAL(18, 4),
    far_expiry DATE,
    far_futures_price DECIMAL(18, 4),
    spot_basis_abs DECIMAL(18, 4),
    spot_basis_bps DECIMAL(18, 4),
    calendar_spread_abs DECIMAL(18, 4),
    calendar_spread_bps DECIMAL(18, 4),
    source VARCHAR
)
"""

CREATE_OPTION_SPREAD_SQL = """
CREATE TABLE IF NOT EXISTS option_spread_snapshots (
    as_of TIMESTAMP WITH TIME ZONE,
    underlying VARCHAR,
    expiry DATE,
    strike DECIMAL(18, 4),
    option_type VARCHAR,
    bid DECIMAL(18, 4),
    ask DECIMAL(18, 4),
    spread_abs DECIMAL(18, 4),
    spread_bps DECIMAL(18, 4),
    implied_volatility DECIMAL(18, 4),
    open_interest DECIMAL(18, 4),
    source VARCHAR
)
"""

_FUTURES_COLS = (
    "as_of, underlying, spot_price, near_expiry, near_futures_price, "
    "far_expiry, far_futures_price, spot_basis_abs, spot_basis_bps, "
    "calendar_spread_abs, calendar_spread_bps, source"
)

_OPTION_COLS = (
    "as_of, underlying, expiry, strike, option_type, bid, ask, "
    "spread_abs, spread_bps, implied_volatility, open_interest, source"
)


class DuckDbFnoSpreadStore:
    """Point-in-time DuckDB store for F&O rollover basis and option spread telemetry."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self._root / "fno_spreads.duckdb"))
        self._connection.execute(CREATE_FUTURES_BASIS_SQL)
        self._connection.execute(CREATE_OPTION_SPREAD_SQL)

    def upsert_futures_basis(self, snapshot: FuturesBasisSnapshot) -> None:
        """Idempotently persist one futures basis observation keyed on
        ``(as_of, underlying, near_expiry)``."""
        self._connection.execute(
            "DELETE FROM futures_basis_snapshots "
            "WHERE as_of = ? AND underlying = ? AND near_expiry = ?",
            [snapshot.as_of, snapshot.underlying, snapshot.near_expiry],
        )
        self._connection.execute(
            f"INSERT INTO futures_basis_snapshots ({_FUTURES_COLS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                snapshot.as_of,
                snapshot.underlying,
                snapshot.spot_price,
                snapshot.near_expiry,
                snapshot.near_futures_price,
                snapshot.far_expiry,
                snapshot.far_futures_price,
                snapshot.spot_basis_abs,
                snapshot.spot_basis_bps,
                snapshot.calendar_spread_abs,
                snapshot.calendar_spread_bps,
                snapshot.source,
            ],
        )

    def upsert_option_spreads(self, snapshots: list[OptionSpreadSnapshot]) -> None:
        """Idempotently persist option bid-ask spread observations keyed on
        ``(as_of, underlying, expiry, strike, option_type)``."""
        for snap in snapshots:
            self._connection.execute(
                "DELETE FROM option_spread_snapshots "
                "WHERE as_of = ? AND underlying = ? AND expiry = ? "
                "AND strike = ? AND option_type = ?",
                [snap.as_of, snap.underlying, snap.expiry, snap.strike, snap.option_type],
            )
            self._connection.execute(
                f"INSERT INTO option_spread_snapshots ({_OPTION_COLS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    snap.as_of,
                    snap.underlying,
                    snap.expiry,
                    snap.strike,
                    snap.option_type,
                    snap.bid,
                    snap.ask,
                    snap.spread_abs,
                    snap.spread_bps,
                    snap.implied_volatility,
                    snap.open_interest,
                    snap.source,
                ],
            )

    def futures_basis_as_of(
        self,
        underlying: str,
        near_expiry: date,
        reference_ts: datetime,
    ) -> FuturesBasisSnapshot | None:
        """Return latest futures basis known at ``reference_ts`` (no look-ahead)."""
        row = self._connection.execute(
            f"SELECT {_FUTURES_COLS} FROM futures_basis_snapshots "
            "WHERE underlying = ? AND near_expiry = ? AND as_of <= ? "
            "ORDER BY as_of DESC LIMIT 1",
            [underlying.strip().upper(), near_expiry, reference_ts],
        ).fetchone()
        if row is None:
            return None
        return self._to_futures_snapshot(row)

    def futures_basis_series(
        self,
        underlying: str,
        near_expiry: date | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[FuturesBasisSnapshot]:
        """Return chronological futures basis snapshots matching optional filters."""
        clauses = ["underlying = ?"]
        params: list[object] = [underlying.strip().upper()]
        if near_expiry is not None:
            clauses.append("near_expiry = ?")
            params.append(near_expiry)
        if start is not None:
            clauses.append("as_of >= ?")
            params.append(start)
        if end is not None:
            clauses.append("as_of <= ?")
            params.append(end)
        where_sql = " AND ".join(clauses)
        rows = self._connection.execute(
            f"SELECT {_FUTURES_COLS} FROM futures_basis_snapshots "
            f"WHERE {where_sql} ORDER BY as_of ASC",
            params,
        ).fetchall()
        return [self._to_futures_snapshot(r) for r in rows]

    def option_spreads_as_of(
        self,
        underlying: str,
        expiry: date,
        reference_ts: datetime,
    ) -> list[OptionSpreadSnapshot]:
        """Return latest per-strike option bid-ask spreads known at ``reference_ts``."""
        rows = self._connection.execute(
            f"SELECT {_OPTION_COLS} FROM ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY strike, option_type ORDER BY as_of DESC"
            "  ) AS rn "
            "  FROM option_spread_snapshots "
            "  WHERE underlying = ? AND expiry = ? AND as_of <= ?"
            ") WHERE rn = 1 ORDER BY strike, option_type",
            [underlying.strip().upper(), expiry, reference_ts],
        ).fetchall()
        return [self._to_option_snapshot(r) for r in rows]

    def median_spread_bps_as_of(
        self,
        underlying: str,
        expiry: date,
        reference_ts: datetime,
        option_type: str | None = None,
    ) -> Decimal | None:
        """Compute exact ``Decimal`` median bid-ask spread in bps as of ``reference_ts``."""
        snaps = self.option_spreads_as_of(underlying, expiry, reference_ts)
        if option_type is not None:
            want = option_type.strip().upper()
            snaps = [s for s in snaps if s.option_type == want]
        if not snaps:
            return None
        bps_vals = sorted(s.spread_bps for s in snaps)
        n = len(bps_vals)
        mid = n // 2
        if n % 2 == 1:
            return bps_vals[mid]
        return ((bps_vals[mid - 1] + bps_vals[mid]) / Decimal("2")).quantize(Decimal("0.0001"))

    def close(self) -> None:
        """Close the DuckDB database connection."""
        self._connection.close()

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def _to_futures_snapshot(cls, row: tuple[object, ...]) -> FuturesBasisSnapshot:
        return FuturesBasisSnapshot(
            as_of=cls._as_utc(row[0]),  # type: ignore[arg-type]
            underlying=str(row[1]),
            spot_price=row[2],  # type: ignore[arg-type]
            near_expiry=row[3],  # type: ignore[arg-type]
            near_futures_price=row[4],  # type: ignore[arg-type]
            far_expiry=row[5],  # type: ignore[arg-type]
            far_futures_price=row[6],  # type: ignore[arg-type]
            source=str(row[11]),
        )

    @classmethod
    def _to_option_snapshot(cls, row: tuple[object, ...]) -> OptionSpreadSnapshot:
        return OptionSpreadSnapshot(
            as_of=cls._as_utc(row[0]),  # type: ignore[arg-type]
            underlying=str(row[1]),
            expiry=row[2],  # type: ignore[arg-type]
            strike=row[3],  # type: ignore[arg-type]
            option_type=str(row[4]),
            bid=row[5],  # type: ignore[arg-type]
            ask=row[6],  # type: ignore[arg-type]
            implied_volatility=row[9],  # type: ignore[arg-type]
            open_interest=row[10],  # type: ignore[arg-type]
            source=str(row[11]),
        )
