"""NSE trading-holiday reference data (gap 0015).

A ``TradingHoliday`` is one declared NSE market closure (or special session)
date in the capital-market (equity/F&O) segment: a full-day closure such as
Republic Day, or a Diwali Laxmi Pujan day that hosts the one-hour **Muhurat
trading** session in addition to the closure of the regular session.

``kind`` distinguishes the two cases the runtime consumer
(``tantra-runtime-backtest::NseTradingCalendar``) needs:

- ``"holiday"`` — the regular session is closed all day; the date is a
  clearing holiday and never a ``is_trading_day`` except for weekends.
- ``"muhurat"`` — Diwali Laxmi Pujan: the regular session is closed (so the
  date is also a clearing holiday) *and* a special evening/afternoon Muhurat
  session makes it a trading day with the special session window.

This is intentionally distinct from ``CalendarEvent`` (scheduled economic
releases): a trading holiday is *reference* data for the exchange calendar,
not a forward-looking announcement, and it carries no values.

Zero-floats invariant: dates, descriptions and kinds are all non-numeric —
no ``float`` is ever involved.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

_VALID_KINDS = ("holiday", "muhurat")


@dataclass(frozen=True, slots=True)
class TradingHoliday:
    """One declared NSE trading holiday or Muhurat session date."""

    date: date
    """Calendar date of the closure / special session (IST)."""

    description: str
    """Exchange description (e.g. ``"Republic Day"``, ``"Diwali Laxmi Pujan"``)."""

    kind: str
    """Session kind: ``"holiday"`` (full-day closure) or ``"muhurat"``
    (Diwali Laxmi Pujan — closed regular session plus a Muhurat session)."""

    def __post_init__(self) -> None:
        if self.kind not in _VALID_KINDS:
            raise ValueError(
                f"invalid trading-holiday kind {self.kind!r}; must be one of {_VALID_KINDS}"
            )
        if not self.description.strip():
            raise ValueError("trading-holiday description must be non-empty")


__all__ = ["TradingHoliday"]
