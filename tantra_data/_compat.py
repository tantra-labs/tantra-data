"""Compatibility and decimal helpers for the lakehouse bounded context.

Zero-floats invariant: every financial quantity is a ``decimal.Decimal``. The
``tantra`` native wheel is optional at runtime; when present it provides
canonical wire primitives, otherwise the pure-Python ``Decimal`` fallbacks are
used so the package stays fully offline-testable.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

TantraDecimal: Any
try:  # pragma: no cover - exercised only when the native wheel is absent
    from tantra import Decimal as _TantraDecimal

    TantraDecimal = _TantraDecimal
    VYAAPAAR_AVAILABLE = True
except ImportError:  # pragma: no cover
    TantraDecimal = Decimal
    VYAAPAAR_AVAILABLE = False

D = Decimal


def to_decimal(value: Decimal | int | str) -> Decimal:
    """Coerce an int/str/Decimal into an exact ``Decimal`` (never a float)."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
