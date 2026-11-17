"""DuckDB-backed repository for shareholding / pledge / insider disclosures (gap 0024).

The store lives in a single DuckDB database at ``{root}/shareholding.duckdb``
with three tables:

* ``shareholding_patterns`` — one row per (company, quarter, SEBI category),
  idempotent on the ``(instrument, exchange, quarter_end, category,
  filing_date)`` key; the point-in-time ``patterns_as_of`` read keeps the
  latest per-(quarter, category) row with ``filing_date <= as_of``.
* ``pledge_records`` — one snapshot per (company, quarter), idempotent on
  ``(instrument, quarter_end)``.
* ``insider_disclosures`` — one row per exchange disclosure id, idempotent on
  ``disclosure_id``.

Zero-floats invariant: every financial column is an exact ``DECIMAL(18, 4)``
``decimal.Decimal`` — no ``float`` anywhere.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from tantra_data.domain.shareholding import InsiderDisclosure, PledgeRecord, ShareholdingPattern

__all__ = ["DuckDbShareholdingStore"]

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS shareholding_patterns (
        instrument VARCHAR,
        company_name VARCHAR,
        exchange VARCHAR,
        filing_date DATE,
        quarter_end DATE,
        category VARCHAR,
        no_of_shares BIGINT,
        shareholding_pct DECIMAL(18, 4),
        source VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pledge_records (
        instrument VARCHAR,
        company_name VARCHAR,
        quarter_end DATE,
        broadcast_date DATE,
        promoter_holding_pct DECIMAL(18, 4),
        pledged_shares BIGINT,
        pledged_pct DECIMAL(18, 4),
        promoter_shares BIGINT,
        total_issued_shares BIGINT,
        source VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS insider_disclosures (
        instrument VARCHAR,
        company_name VARCHAR,
        disclosure_id BIGINT,
        trade_date DATE,
        intimation_date DATE,
        name VARCHAR,
        person_category VARCHAR,
        transaction_type VARCHAR,
        acq_mode VARCHAR,
        acquired_shares BIGINT,
        buy_quantity BIGINT,
        sell_quantity BIGINT,
        value DECIMAL(18, 4),
        before_shares BIGINT,
        before_pct DECIMAL(18, 4),
        after_shares BIGINT,
        after_pct DECIMAL(18, 4),
        source VARCHAR
    )
    """,
]

_PATTERN_COLUMNS = (
    "instrument, company_name, exchange, filing_date, quarter_end, category, "
    "no_of_shares, shareholding_pct, source"
)
_PLEDGE_COLUMNS = (
    "instrument, company_name, quarter_end, broadcast_date, promoter_holding_pct, "
    "pledged_shares, pledged_pct, promoter_shares, total_issued_shares, source"
)
_DISCLOSURE_COLUMNS = (
    "instrument, company_name, disclosure_id, trade_date, intimation_date, name, "
    "person_category, transaction_type, acq_mode, acquired_shares, buy_quantity, "
    "sell_quantity, value, before_shares, before_pct, after_shares, after_pct, source"
)

_PCT_SCALE = Decimal("0.0001")


class DuckDbShareholdingStore:
    """DuckDB point-in-time store for the gap-0024 disclosure datasets.

    Mirrors the ``store/trading_holidays.py`` / ``store/option_chain.py``
    pattern: tables are created in ``__init__``, upserts are DELETE-then-
    INSERT per their idempotency key, and the read helpers return typed domain
    rows with no look-ahead.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self._root / "shareholding.duckdb"))
        for create_sql in CREATE_TABLES_SQL:
            self._connection.execute(create_sql)

    # -- writers ---------------------------------------------------------

    def upsert_patterns(self, patterns: list[ShareholdingPattern]) -> None:
        """Persist ``patterns`` idempotently on ``(instrument, exchange,
        quarter_end, category, filing_date)`` — re-upserting a filing never
        duplicates rows."""
        for pattern in patterns:
            self._connection.execute(
                "DELETE FROM shareholding_patterns WHERE instrument = ? AND exchange = ? "
                "AND quarter_end = ? AND category = ? AND filing_date = ?",
                [
                    pattern.instrument,
                    pattern.exchange,
                    pattern.quarter_end,
                    pattern.category,
                    pattern.filing_date,
                ],
            )
            self._connection.execute(
                f"INSERT INTO shareholding_patterns ({_PATTERN_COLUMNS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    pattern.instrument,
                    pattern.company_name,
                    pattern.exchange,
                    pattern.filing_date,
                    pattern.quarter_end,
                    pattern.category,
                    pattern.no_of_shares,
                    _pct(pattern.shareholding_pct),
                    pattern.source,
                ],
            )

    def put_pledges(self, records: list[PledgeRecord]) -> None:
        """Persist ``records`` idempotently per ``(instrument, quarter_end)`` —
        re-ingesting the same quarter's snapshot replaces it, never duplicates."""
        for record in records:
            self._connection.execute(
                "DELETE FROM pledge_records WHERE instrument = ? AND quarter_end = ?",
                [record.instrument, record.quarter_end],
            )
            self._connection.execute(
                f"INSERT INTO pledge_records ({_PLEDGE_COLUMNS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    record.instrument,
                    record.company_name,
                    record.quarter_end,
                    record.broadcast_date,
                    _pct(record.promoter_holding_pct),
                    record.pledged_shares,
                    _pct(record.pledged_pct),
                    record.promoter_shares,
                    record.total_issued_shares,
                    record.source,
                ],
            )

    def upsert_disclosures(self, disclosures: list[InsiderDisclosure]) -> None:
        """Persist ``disclosures`` idempotently on ``disclosure_id``."""
        for disclosure in disclosures:
            self._connection.execute(
                "DELETE FROM insider_disclosures WHERE disclosure_id = ?",
                [disclosure.disclosure_id],
            )
            self._connection.execute(
                f"INSERT INTO insider_disclosures ({_DISCLOSURE_COLUMNS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    disclosure.instrument,
                    disclosure.company_name,
                    disclosure.disclosure_id,
                    disclosure.trade_date,
                    disclosure.intimation_date,
                    disclosure.name,
                    disclosure.person_category,
                    disclosure.transaction_type,
                    disclosure.acq_mode,
                    disclosure.acquired_shares,
                    disclosure.buy_quantity,
                    disclosure.sell_quantity,
                    _pct(disclosure.value),
                    disclosure.before_shares,
                    _pct(disclosure.before_pct),
                    disclosure.after_shares,
                    _pct(disclosure.after_pct),
                    disclosure.source,
                ],
            )

    # -- shareholding reads ----------------------------------------------

    def patterns_as_of(self, instrument: str, as_of: date) -> list[ShareholdingPattern]:
        """Return the pattern rows known at ``as_of`` (no look-ahead).

        For every ``(quarter_end, category)`` this keeps the latest row with
        ``filing_date <= as_of``, ordered by quarter then category.
        """
        rows = self._connection.execute(
            f"SELECT {_PATTERN_COLUMNS} FROM ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY quarter_end, category ORDER BY filing_date DESC"
            "  ) AS rn"
            "  FROM shareholding_patterns"
            "  WHERE instrument = ? AND filing_date <= ?"
            ") WHERE rn = 1 ORDER BY quarter_end, category",
            [instrument, as_of],
        ).fetchall()
        return [self._to_pattern(row) for row in rows]

    def latest_patterns(self, instrument: str) -> list[ShareholdingPattern]:
        """Return the most recent pattern rows for ``instrument`` (the latest
        filing's categories)."""
        rows = self._connection.execute(
            f"SELECT {_PATTERN_COLUMNS} FROM ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY quarter_end, category ORDER BY filing_date DESC"
            "  ) AS rn, MAX(filing_date) OVER () AS latest_filing"
            "  FROM shareholding_patterns"
            "  WHERE instrument = ?"
            ") WHERE rn = 1 AND filing_date = latest_filing ORDER BY category",
            [instrument],
        ).fetchall()
        return [self._to_pattern(row) for row in rows]

    def all_patterns(self) -> list[ShareholdingPattern]:
        """Return every stored pattern row, ordered by filing then category."""
        rows = self._connection.execute(
            f"SELECT {_PATTERN_COLUMNS} FROM shareholding_patterns "
            "ORDER BY filing_date, quarter_end, category"
        ).fetchall()
        return [self._to_pattern(row) for row in rows]

    # -- pledge reads -----------------------------------------------------

    def pledges_as_of(self, instrument: str, as_of: date) -> list[PledgeRecord]:
        """Return the pledge snapshots known at ``as_of`` (no look-ahead).

        For every ``quarter_end`` this keeps the latest row with
        ``broadcast_date <= as_of``, ordered by quarter.
        """
        rows = self._connection.execute(
            f"SELECT {_PLEDGE_COLUMNS} FROM ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY quarter_end ORDER BY broadcast_date DESC"
            "  ) AS rn"
            "  FROM pledge_records"
            "  WHERE instrument = ? AND broadcast_date <= ?"
            ") WHERE rn = 1 ORDER BY quarter_end",
            [instrument, as_of],
        ).fetchall()
        return [self._to_pledge(row) for row in rows]

    def latest_pledge(self, instrument: str) -> PledgeRecord | None:
        """Return the latest pledge snapshot for ``instrument``, or ``None``
        when no pledge data exists."""
        rows = self._connection.execute(
            f"SELECT {_PLEDGE_COLUMNS} FROM pledge_records "
            "WHERE instrument = ? ORDER BY broadcast_date DESC, quarter_end DESC LIMIT 1",
            [instrument],
        ).fetchall()
        return self._to_pledge(rows[0]) if rows else None

    def all_pledges(self) -> list[PledgeRecord]:
        """Return every stored pledge row, ordered by broadcast then quarter."""
        rows = self._connection.execute(
            f"SELECT {_PLEDGE_COLUMNS} FROM pledge_records "
            "ORDER BY broadcast_date, quarter_end, instrument"
        ).fetchall()
        return [self._to_pledge(row) for row in rows]

    # -- insider-disclosure reads -----------------------------------------

    def disclosures(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[InsiderDisclosure]:
        """Return insider disclosures, optionally filtered by instrument and an
        intimation-date window, ordered by intimation date."""
        clauses: list[str] = []
        params: list[object] = []
        if instrument is not None:
            clauses.append("instrument = ?")
            params.append(instrument)
        if start_date is not None:
            clauses.append("intimation_date >= ?")
            params.append(start_date)
        if end_date is not None:
            clauses.append("intimation_date <= ?")
            params.append(end_date)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_DISCLOSURE_COLUMNS} FROM insider_disclosures{where_clause} "
            "ORDER BY intimation_date, disclosure_id",
            params,
        ).fetchall()
        return [self._to_disclosure(row) for row in rows]

    def all_disclosures(self) -> list[InsiderDisclosure]:
        """Return every stored insider disclosure, ordered by intimation date."""
        rows = self._connection.execute(
            f"SELECT {_DISCLOSURE_COLUMNS} FROM insider_disclosures "
            "ORDER BY intimation_date, disclosure_id"
        ).fetchall()
        return [self._to_disclosure(row) for row in rows]

    # -- misc --------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    @staticmethod
    def _to_pattern(row: tuple[object, ...]) -> ShareholdingPattern:
        filing_date = _as_date(row[3])
        quarter_end = _as_date(row[4])
        return ShareholdingPattern(
            instrument=str(row[0]),
            company_name=str(row[1]),
            exchange=str(row[2]),
            filing_date=filing_date,
            quarter_end=quarter_end,
            category=str(row[5]),
            no_of_shares=_as_int(row[6]),
            shareholding_pct=_as_decimal(row[7]),
            source=str(row[8]),
        )

    @staticmethod
    def _to_pledge(row: tuple[object, ...]) -> PledgeRecord:
        return PledgeRecord(
            instrument=str(row[0]),
            company_name=str(row[1]),
            quarter_end=_as_date(row[2]),
            broadcast_date=_as_date(row[3]),
            promoter_holding_pct=_as_decimal(row[4]),
            pledged_shares=_as_int(row[5]),
            pledged_pct=_as_decimal(row[6]),
            promoter_shares=_as_int(row[7]),
            total_issued_shares=_as_int(row[8]),
            source=str(row[9]),
        )

    @staticmethod
    def _to_disclosure(row: tuple[object, ...]) -> InsiderDisclosure:
        return InsiderDisclosure(
            instrument=str(row[0]),
            company_name=str(row[1]),
            disclosure_id=_as_int(row[2]),
            trade_date=_as_date(row[3]),
            intimation_date=_as_date(row[4]),
            name=str(row[5]),
            person_category=str(row[6]),
            transaction_type=str(row[7]),
            acq_mode=str(row[8]),
            acquired_shares=_as_int(row[9]),
            buy_quantity=_as_int(row[10]),
            sell_quantity=_as_int(row[11]),
            value=_as_decimal(row[12]),
            before_shares=_as_int(row[13]),
            before_pct=_as_decimal(row[14]),
            after_shares=_as_int(row[15]),
            after_pct=_as_decimal(row[16]),
            source=str(row[17]),
        )


def _pct(value: Decimal) -> Decimal:
    """Quantize an exact ``Decimal`` to the store's 4-dp ``DECIMAL(18, 4)``
    scale so round-trips are exact (no silent DuckDB rounding)."""
    return value.quantize(_PCT_SCALE)


def _as_date(value: object) -> date:
    if not isinstance(value, date):
        raise ValueError(f"expected a DATE column value, got {value!r}")
    return value


def _as_int(value: object) -> int:
    if isinstance(value, int):
        return value
    raise ValueError(f"expected an integer column value, got {value!r}")


def _as_decimal(value: object) -> Decimal:
    if not isinstance(value, Decimal):
        raise ValueError(f"expected a DECIMAL column value, got {value!r}")
    return value
