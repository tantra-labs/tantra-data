"""Financial news article value objects.

A news article carries an id, title, source/publisher, publication timestamp,
summary, article URL, and the instruments it mentions. This is the canonical
shape behind the chat agent's ``news_search`` tool.

Zero-floats invariant: articles carry no float quantities; every value is a
string, datetime, or ``decimal.Decimal``. Value objects are frozen, self-
validating dataclasses that compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from tantra_data._compat import to_decimal


@dataclass(frozen=True, slots=True)
class NewsArticle:
    """One financial news article."""

    article_id: str
    """Stable article identifier (e.g. the RSS GUID or URL)."""

    title: str
    """Article headline."""

    source: str
    """Publisher / feed name (e.g. ``"moneycontrol"``)."""

    url: str
    """Article URL."""

    published_at: datetime
    """Publication timestamp (timezone-aware UTC)."""

    summary: str | None = None
    """Article summary / description."""

    instruments: tuple[str, ...] = ()
    """Instruments the article mentions (canonical ``InstrumentId``-style
    symbols, e.g. ``"NSE:RELIANCE-EQ"``)."""

    def __post_init__(self) -> None:
        if not self.article_id:
            raise ValueError("article_id must be non-empty")
        if not self.title:
            raise ValueError("title must be non-empty")
        if not self.source:
            raise ValueError("source must be non-empty")
        if not self.url:
            raise ValueError("url must be non-empty")
        if self.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware")
        object.__setattr__(self, "summary", self.summary or None)
        object.__setattr__(self, "instruments", tuple(self.instruments))


__all__ = ["NewsArticle", "to_decimal"]
