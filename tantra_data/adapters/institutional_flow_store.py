"""DuckDB-backed repository for institutional FII/DII net flows (gap 0013)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from tantra_data.domain.institutional_flow import InstitutionalFlow

__all__ = ["DuckDbInstitutionalFlowStore"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS institutional_flows (
    trade_date DATE,
    participant VARCHAR,
    segment VARCHAR,
    gross_buy DECIMAL(38, 10),
    gross_sell DECIMAL(38, 10),
    net DECIMAL(38, 10),
    source VARCHAR
)
"""

_COLUMNS = "trade_date, participant, segment, gross_buy, gross_sell, net, source"


class DuckDbInstitutionalFlowStore:
    """DuckDB-backed implementation of the ``InstitutionalFlowRepository`` port.

    Mirrors the existing ``filings_store``/``macro_store`` pattern: the table is
    created in ``__init__``, ``upsert`` DELETE-then-INSERTs per
    trade_date + participant + segment, and ``query`` returns typed domain rows.
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._connection = duckdb.connect(path)
        self._connection.execute(CREATE_TABLE_SQL)

    def upsert(self, flow: InstitutionalFlow) -> None:
        self._connection.execute(
            "DELETE FROM institutional_flows "
            "WHERE trade_date = ? AND participant = ? AND segment = ?",
            [flow.trade_date, flow.participant, flow.segment],
        )
        self._connection.execute(
            f"INSERT INTO institutional_flows ({_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                flow.trade_date,
                flow.participant,
                flow.segment,
                str(flow.gross_buy),
                str(flow.gross_sell),
                str(flow.net),
                flow.source,
            ],
        )

    def query(
        self,
        trade_date: date | None = None,
        participant: str | None = None,
        segment: str | None = None,
    ) -> list[InstitutionalFlow]:
        clauses: list[str] = []
        params: list[object] = []
        if trade_date is not None:
            clauses.append("trade_date = ?")
            params.append(trade_date)
        if participant is not None:
            clauses.append("participant = ?")
            params.append(participant)
        if segment is not None:
            clauses.append("segment = ?")
            params.append(segment)
        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            f"SELECT {_COLUMNS} FROM institutional_flows{where_clause} "
            "ORDER BY trade_date, participant"
        )
        rows = self._connection.execute(sql, params).fetchall()
        return [
            InstitutionalFlow(
                trade_date=row[0],
                participant=row[1],
                segment=row[2],
                gross_buy=Decimal(str(row[3])),
                gross_sell=Decimal(str(row[4])),
                net=Decimal(str(row[5])),
                source=row[6],
            )
            for row in rows
        ]

    def close(self) -> None:
        self._connection.close()
