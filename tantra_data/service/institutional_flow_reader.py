"""Polars reader for institutional FII/DII net flows (gap 0013)."""

from __future__ import annotations

from datetime import date

import polars as pl

from tantra_data.ports.repositories import InstitutionalFlowRepository

__all__ = ["PythonInstitutionalFlowReader"]

FLOW_SCHEMA: dict[str, pl.DataType] = {
    "trade_date": pl.Date(),
    "participant": pl.String(),
    "segment": pl.String(),
    "gross_buy": pl.Decimal(18, 4),
    "gross_sell": pl.Decimal(18, 4),
    "net": pl.Decimal(18, 4),
    "source": pl.String(),
}


class PythonInstitutionalFlowReader:
    """In-process :class:`InstitutionalFlowReader` backed by a repository."""

    def __init__(self, repository: InstitutionalFlowRepository) -> None:
        self._repository = repository

    def get_flows(
        self,
        trade_date: date | None = None,
        participant: str | None = None,
        segment: str | None = None,
    ) -> pl.DataFrame:
        flows = self._repository.query(
            trade_date=trade_date,
            participant=participant,
            segment=segment,
        )
        if not flows:
            return pl.DataFrame(schema=FLOW_SCHEMA)
        return pl.DataFrame(
            {
                "trade_date": [flow.trade_date for flow in flows],
                "participant": [flow.participant for flow in flows],
                "segment": [flow.segment for flow in flows],
                "gross_buy": [flow.gross_buy for flow in flows],
                "gross_sell": [flow.gross_sell for flow in flows],
                "net": [flow.net for flow in flows],
                "source": [flow.source for flow in flows],
            },
            schema=FLOW_SCHEMA,
        )
