"""DuckDB-backed repository for financial-statement line items (gap 0020)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.fundamentals import (
    FinancialStatement,
    PeriodType,
    StatementType,
)
from tantra_data.ports.repositories import FinancialStatementRepository

__all__ = ["DuckDbFinancialStatementStore"]

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS financial_statements (
    instrument VARCHAR,
    statement_type VARCHAR,
    period_type VARCHAR,
    period_end DATE,
    filing_date DATE,
    line_item VARCHAR,
    value DECIMAL(38, 10),
    currency VARCHAR
)
"""

_COLUMNS = (
    "instrument, statement_type, period_type, period_end, filing_date, line_item, value, currency"
)


class DuckDbFinancialStatementStore(FinancialStatementRepository):
    """DuckDB-backed statement-line-item store.

    Mirrors the ``delivery_store``/``institutional_flow_store`` pattern:
    ``__init__`` creates the table, ``upsert`` DELETE-then-INSERTs per
    instrument + statement_type + period_type + period_end + filing_date +
    line_item, and ``query`` returns typed domain rows with ``filing_date <=
    as_of`` point-in-time semantics.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Open a DuckDB connection at ``path`` and ensure the table exists."""
        self._connection = duckdb.connect(path)
        self._connection.execute(_CREATE_TABLE)

    def upsert(self, statement: FinancialStatement) -> None:
        """Insert or replace the line item for its full disclosure key."""
        self._connection.execute(
            "DELETE FROM financial_statements"
            " WHERE instrument = ? AND statement_type = ? AND period_type = ?"
            " AND period_end = ? AND filing_date = ? AND line_item = ?",
            [
                statement.instrument,
                statement.statement_type.value,
                statement.period_type.value,
                statement.period_end_date,
                statement.filing_date,
                statement.line_item,
            ],
        )
        self._connection.execute(
            f"INSERT INTO financial_statements ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                statement.instrument,
                statement.statement_type.value,
                statement.period_type.value,
                statement.period_end_date,
                statement.filing_date,
                statement.line_item,
                str(statement.value),
                statement.currency,
            ],
        )

    def query(
        self,
        instrument: str | None = None,
        statement_type: StatementType | None = None,
        period_type: PeriodType | None = None,
        as_of: date | None = None,
        line_item: str | None = None,
    ) -> list[FinancialStatement]:
        """Query line items, optionally filtered; ``as_of`` is ``filing_date <=``."""
        clauses: list[str] = []
        params: list[object] = []
        if instrument is not None:
            clauses.append("instrument = ?")
            params.append(instrument)
        if statement_type is not None:
            clauses.append("statement_type = ?")
            params.append(statement_type.value)
        if period_type is not None:
            clauses.append("period_type = ?")
            params.append(period_type.value)
        if as_of is not None:
            clauses.append("filing_date <= ?")
            params.append(as_of)
        if line_item is not None:
            clauses.append("line_item = ?")
            params.append(line_item)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM financial_statements{where_clause}"
            " ORDER BY period_end, line_item",
            params,
        ).fetchall()
        return [
            FinancialStatement(
                instrument=row[0],
                statement_type=StatementType(row[1]),
                period_type=PeriodType(row[2]),
                period_end_date=row[3],
                filing_date=row[4],
                line_item=row[5],
                value=Decimal(str(row[6])),
                currency=row[7],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()
