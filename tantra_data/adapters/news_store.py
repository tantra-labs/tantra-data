"""DuckDB-backed repository for financial news articles (gap 0006)."""

from __future__ import annotations

from datetime import UTC, date
from typing import Any

import duckdb

from tantra_data.domain.news import NewsArticle

__all__ = ["DuckDbNewsStore"]


class DuckDbNewsStore:
    """NewsRepository backed by a DuckDB table of news articles.

    Articles are keyed by URL: ``upsert`` is idempotent on url.
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._conn = duckdb.connect(path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_articles (
                article_id VARCHAR,
                title VARCHAR,
                source VARCHAR,
                url VARCHAR,
                published_at TIMESTAMP,
                summary VARCHAR,
                instruments VARCHAR
            )
            """
        )

    def upsert(self, article: NewsArticle) -> None:
        """Insert or replace an article, keyed idempotently by url."""
        self._conn.execute("DELETE FROM news_articles WHERE url = ?", [article.url])
        # Store naive UTC so the TIMESTAMP column round-trips exactly,
        # regardless of the connection's session time zone.
        published_at = article.published_at.astimezone(UTC).replace(tzinfo=None)
        self._conn.execute(
            """
            INSERT INTO news_articles (
                article_id, title, source, url,
                published_at, summary, instruments
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                article.article_id,
                article.title,
                article.source,
                article.url,
                published_at,
                article.summary,
                ",".join(article.instruments),
            ],
        )

    def query(
        self,
        instrument: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        query: str | None = None,
    ) -> list[NewsArticle]:
        """Return matching articles, newest first, honoring all filters."""
        clauses: list[str] = []
        params: list[Any] = []
        if start_date is not None:
            clauses.append("CAST(published_at AS DATE) >= ?")
            params.append(start_date)
        if end_date is not None:
            clauses.append("CAST(published_at AS DATE) <= ?")
            params.append(end_date)
        if query is not None:
            clauses.append("(title ILIKE ? OR summary ILIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
        if instrument is not None:
            clauses.append("instruments LIKE ?")
            params.append(f"%{instrument}%")
        sql = (
            "SELECT article_id, title, source, url, published_at,"
            + " summary, instruments FROM news_articles"
        )
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY published_at DESC"
        rows = self._conn.execute(sql, params).fetchall()
        articles: list[NewsArticle] = []
        for row in rows:
            (
                article_id,
                title,
                source,
                url,
                published_at,
                summary,
                instruments,
            ) = row
            if published_at.tzinfo is None:
                published_at = published_at.replace(tzinfo=UTC)
            instruments_tuple: tuple[str, ...] = ()
            if instruments:
                instruments_tuple = tuple(instruments.split(","))
            articles.append(
                NewsArticle(
                    article_id=article_id,
                    title=title,
                    source=source,
                    url=url,
                    published_at=published_at,
                    summary=summary,
                    instruments=instruments_tuple,
                )
            )
        return articles

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._conn.close()
