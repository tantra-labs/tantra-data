"""Financial news article ingestion provider (gap 0006).

Parses the public Moneycontrol RSS business-news feed deterministically,
without network I/O. The optional network fetcher is guarded behind a lazy
``requests`` import so the parsing pipeline stays fully offline.

Zero-floats invariant: no floating-point arithmetic is performed anywhere in
this module; prices, quantities, and money are never represented as ``float``.
All timestamps are timezone-aware :class:`datetime` objects.
"""

from __future__ import annotations

import email.utils
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from tantra_data.domain.news import NewsArticle

MONEYCONTROL_FEED_URL: Final[str] = "https://www.moneycontrol.com/rss/business.xml"

__all__ = [
    "NewsRow",
    "fetch_moneycontrol_news",
    "parse_news",
    "read_news",
    "rows_to_articles",
]


@dataclass(frozen=True)
class NewsRow:
    """A single parsed RSS news item, prior to domain mapping."""

    article_id: str
    title: str
    source: str
    url: str
    published_at: datetime
    summary: str | None


def _local_name(tag: str) -> str:
    """Strip the XML namespace prefix from an ElementTree tag."""
    return tag.rsplit("}", 1)[1] if "}" in tag else tag


def parse_news(text: str | bytes) -> list[NewsRow]:
    """Parse RSS 2.0 XML text (or raw bytes) into a list of NewsRow.

    Byte input is decoded with ISO-8859-1 (the encoding of the Moneycontrol
    feed). Items missing a title or link are skipped. Unparseable pubDate
    values raise ValueError.
    """
    if isinstance(text, bytes):
        text = text.decode("ISO-8859-1")
    root = ET.fromstring(text)
    channel = next((child for child in root if _local_name(child.tag) == "channel"), None)
    if channel is None:
        return []
    rows: list[NewsRow] = []
    for item in channel:
        if _local_name(item.tag) != "item":
            continue
        fields: dict[str, str] = {}
        for element in item:
            name = _local_name(element.tag)
            fields.setdefault(name, (element.text or "").strip())
        title = fields.get("title", "")
        link = fields.get("link", "")
        if not title or not link:
            continue
        article_id = fields.get("guid") or link
        pubdate_raw = fields.get("pubDate", "")
        try:
            published_at = email.utils.parsedate_to_datetime(pubdate_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"could not parse pubDate {pubdate_raw!r}") from exc
        if published_at is None:
            raise ValueError(f"could not parse pubDate {pubdate_raw!r}")
        published_at = published_at.astimezone(UTC)
        description = fields.get("description") or None
        rows.append(
            NewsRow(
                article_id=article_id,
                title=title,
                source="moneycontrol",
                url=link,
                published_at=published_at,
                summary=description,
            )
        )
    return rows


def read_news(path: str | Path) -> list[NewsRow]:
    """Read an RSS XML file from disk and parse it into NewsRow records."""
    return parse_news(Path(path).read_bytes())


def rows_to_articles(rows: list[NewsRow], source: str = "moneycontrol") -> list[NewsArticle]:
    """Map parsed news rows to domain NewsArticle values (instruments=())."""
    return [
        NewsArticle(
            article_id=row.article_id,
            title=row.title,
            source=source,
            url=row.url,
            published_at=row.published_at,
            summary=row.summary,
        )
        for row in rows
    ]


def fetch_moneycontrol_news() -> list[NewsArticle]:
    """Fetch the latest Moneycontrol business news feed over HTTP."""
    import requests

    response = requests.get(MONEYCONTROL_FEED_URL, timeout=15)
    if response.status_code != 200:
        raise RuntimeError(f"Moneycontrol RSS feed returned HTTP {response.status_code}")
    return rows_to_articles(parse_news(response.content))
