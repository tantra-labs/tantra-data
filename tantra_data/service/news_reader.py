"""Polars news reader service (gap 0006)."""

from __future__ import annotations

from datetime import date

import polars as pl

from tantra_data.ports.repositories import NewsRepository

__all__ = ["PythonNewsReader"]


class PythonNewsReader:
    """NewsReader that searches a NewsRepository into Polars frames."""

    def __init__(self, repository: NewsRepository) -> None:
        self._repository = repository

    def search_news(
        self,
        query: str | None = None,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Search the repository and return a canonical Polars DataFrame.

        Returns an empty frame with the canonical schema when nothing matches.
        """
        articles = self._repository.query(
            query=query,
            instrument=instrument,
            start_date=start_date,
            end_date=end_date,
        )
        frame = pl.DataFrame(
            {
                "article_id": [a.article_id for a in articles],
                "title": [a.title for a in articles],
                "source": [a.source for a in articles],
                "url": [a.url for a in articles],
                "published_at": [a.published_at for a in articles],
                "summary": [a.summary for a in articles],
                "instruments": [",".join(a.instruments) for a in articles],
            },
            schema={
                "article_id": pl.String,
                "title": pl.String,
                "source": pl.String,
                "url": pl.String,
                "published_at": pl.Datetime("us", time_zone="UTC"),
                "summary": pl.String,
                "instruments": pl.String,
            },
        )
        if limit is not None:
            frame = frame.head(limit)
        return frame
