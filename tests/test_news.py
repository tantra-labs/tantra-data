"""Tests for the financial news article ingestion pipeline (gap 0006)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from tantra_data.adapters.news_store import DuckDbNewsStore
from tantra_data.domain.news import NewsArticle
from tantra_data.ports.data_feeds import NewsReader
from tantra_data.ports.repositories import NewsRepository
from tantra_data.providers.news import NewsRow, parse_news, rows_to_articles
from tantra_data.service.news_reader import PythonNewsReader

SAMPLE = """<?xml version="1.0" encoding="ISO-8859-1"?>
<rss version="2.0">
  <channel>
    <title>Moneycontrol Business News</title>
    <link>https://www.moneycontrol.com/news/</link>
    <description>Business news feed</description>
    <item>
      <title>Sensex ends higher on IT rally</title>
      <link>https://www.moneycontrol.com/news/business/markets/sensex-ends-higher-on-it-rally.html</link>
      <guid>https://www.moneycontrol.com/news/business/markets/sensex-ends-higher-on-it-rally.html</guid>
      <description>Indian equity benchmarks &quot;closed&quot; in the green today.</description>
      <pubDate>Tue, 23 Apr 2024 22:36:32 +0530</pubDate>
    </item>
    <item>
      <title>RBI keeps repo rate unchanged</title>
      <link>https://www.moneycontrol.com/news/business/rbi-keeps-repo-rate-unchanged.html</link>
      <pubDate>Wed, 24 Apr 2024 10:15:00 +0530</pubDate>
    </item>
  </channel>
</rss>
"""


def _article(
    article_id: str,
    title: str,
    url: str,
    published_at: datetime,
    summary: str | None = None,
    instruments: tuple[str, ...] = (),
) -> NewsArticle:
    return NewsArticle(
        article_id=article_id,
        title=title,
        source="moneycontrol",
        url=url,
        published_at=published_at,
        summary=summary,
        instruments=instruments,
    )


def test_parse_news() -> None:
    rows = parse_news(SAMPLE)
    assert len(rows) == 2
    first, second = rows
    assert isinstance(first, NewsRow)
    assert first.article_id == (
        "https://www.moneycontrol.com/news/business/markets/"
        "sensex-ends-higher-on-it-rally.html"
    )
    assert first.title == "Sensex ends higher on IT rally"
    assert first.source == "moneycontrol"
    assert first.url == (
        "https://www.moneycontrol.com/news/business/markets/"
        "sensex-ends-higher-on-it-rally.html"
    )
    assert first.published_at == datetime(2024, 4, 23, 17, 6, 32, tzinfo=UTC)
    assert first.summary == (
        'Indian equity benchmarks "closed" in the green today.'
    )
    assert second.article_id == (
        "https://www.moneycontrol.com/news/business/"
        "rbi-keeps-repo-rate-unchanged.html"
    )
    assert second.summary is None
    assert second.published_at == datetime(2024, 4, 24, 4, 45, 0, tzinfo=UTC)
    # Byte input is decoded with ISO-8859-1.
    assert parse_news(SAMPLE.encode("ISO-8859-1")) == rows


def test_rows_to_articles() -> None:
    articles = rows_to_articles(parse_news(SAMPLE))
    assert len(articles) == 2
    assert all(isinstance(a, NewsArticle) for a in articles)
    assert all(a.source == "moneycontrol" for a in articles)
    assert all(a.instruments == () for a in articles)
    assert articles[0].published_at == datetime(
        2024, 4, 23, 17, 6, 32, tzinfo=UTC
    )


def test_duckdb_news_store_round_trip() -> None:
    store = DuckDbNewsStore()
    try:
        article_a = _article(
            article_id="a1",
            title="Sensex ends higher on IT rally",
            url="https://www.moneycontrol.com/news/a1.html",
            published_at=datetime(2024, 4, 23, 17, 6, 32, tzinfo=UTC),
            summary="Indian benchmarks close in the green",
            instruments=("NSE:SENSEX-INDEX",),
        )
        article_b = _article(
            article_id="b1",
            title="RBI keeps repo rate unchanged",
            url="https://www.moneycontrol.com/news/b1.html",
            published_at=datetime(2024, 4, 24, 4, 45, 0, tzinfo=UTC),
        )
        store.upsert(article_a)
        store.upsert(article_b)
        # Idempotent re-upsert on the same URL keeps a single row.
        store.upsert(article_a)
        articles = store.query()
        assert len(articles) == 2
        by_url = {a.url: a for a in articles}
        restored = by_url[article_a.url]
        assert restored.article_id == "a1"
        assert restored.title == article_a.title
        assert restored.source == "moneycontrol"
        assert restored.summary == article_a.summary
        assert restored.instruments == ("NSE:SENSEX-INDEX",)
        assert restored.published_at == article_a.published_at
        assert restored.published_at.tzinfo is not None
        # Text query filters match title and summary case-insensitively.
        assert [a.url for a in store.query(query="rbi")] == [article_b.url]
        assert [a.url for a in store.query(query="green")] == [article_a.url]
        # Date-window filters.
        assert [a.url for a in store.query(start_date=date(2024, 4, 24))] == [
            article_b.url
        ]
        assert [a.url for a in store.query(end_date=date(2024, 4, 23))] == [
            article_a.url
        ]
        # Instrument filter.
        assert [a.url for a in store.query(instrument="SENSEX")] == [
            article_a.url
        ]
    finally:
        store.close()


def test_news_reader_search_news() -> None:
    store = DuckDbNewsStore()
    try:
        store.upsert(
            _article(
                article_id="a1",
                title="Sensex ends higher on IT rally",
                url="https://www.moneycontrol.com/news/a1.html",
                published_at=datetime(2024, 4, 23, 17, 6, 32, tzinfo=UTC),
            )
        )
        store.upsert(
            _article(
                article_id="b1",
                title="RBI keeps repo rate unchanged",
                url="https://www.moneycontrol.com/news/b1.html",
                published_at=datetime(2024, 4, 24, 4, 45, 0, tzinfo=UTC),
            )
        )
        reader = PythonNewsReader(store)
        frame = reader.search_news()
        assert frame.columns == [
            "article_id",
            "title",
            "source",
            "url",
            "published_at",
            "summary",
            "instruments",
        ]
        assert frame.height == 2
        assert frame.schema["article_id"] == pl.String
        assert frame.schema["published_at"] == pl.Datetime(
            "us", time_zone="UTC"
        )
        # Newest first (ORDER BY published_at DESC).
        assert frame["title"].to_list() == [
            "RBI keeps repo rate unchanged",
            "Sensex ends higher on IT rally",
        ]
        limited = reader.search_news(limit=1)
        assert limited.height == 1
        assert limited["title"].to_list() == ["RBI keeps repo rate unchanged"]
        # No matches -> canonical empty frame with the same schema.
        empty = reader.search_news(query="no-such-article")
        assert empty.height == 0
        assert empty.columns == frame.columns
        assert empty.schema["published_at"] == pl.Datetime(
            "us", time_zone="UTC"
        )
    finally:
        store.close()


def test_news_protocol_checks() -> None:
    store = DuckDbNewsStore()
    try:
        reader = PythonNewsReader(store)
        assert isinstance(store, NewsRepository)
        assert isinstance(reader, NewsReader)
    finally:
        store.close()
