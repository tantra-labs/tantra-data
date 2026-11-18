"""Lakehouse v3 manifest, ref, and symbology-alias value objects.

Implements ``docs/plans/0009`` Phase A / ADR ``docs/design/0004``: the
git-style content-addressed lakehouse needs (1) refs that pin a catalog view
to a commit (or track the live head), (2) commit records forming a
parent-linked immutable history, (3) manifest entries mapping logical
``(domain, dataset, exchange, asset_class, timeframe, time_bucket)``
coordinates to immutable content-addressed objects, and (4) instrument
aliases that keep historical series continuous across symbol renames
(e.g. ``ZOMATO`` -> ``ETERNAL``).

Zero-floats invariant: every field is a ``str``, ``int``, ``date``, or
timezone-aware ``datetime`` — there is no ``float`` anywhere on the manifest
surface (monetary/price math lives in the bar schemas as exact ``Decimal``).
Value objects are frozen, self-validating dataclasses that compare by value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

__all__ = [
    "CatalogRef",
    "CommitRecord",
    "InstrumentAlias",
    "ManifestEntry",
]


def _require_non_empty(value: str, field_name: str) -> None:
    if not value:
        raise ValueError(f"{field_name} must be non-empty")


def _require_utc(value: datetime, field_name: str) -> datetime:
    if value is None or value.tzinfo is None:
        raise ValueError(f"{field_name} must be a timezone-aware datetime (UTC)")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class CatalogRef:
    """A catalog ref name and optional commit pin.

    ``commit_sha=None`` tracks the live head (``refs/heads/<name>``); a pinned
    ref records the exact ``commit_sha`` it resolved to.
    """

    name: str
    """Ref name (e.g. ``"main"``, ``"staging/daily"``, a tag name)."""

    commit_sha: str | None = None
    """Pinned commit the ref resolves to; ``None`` means the live head."""

    def __post_init__(self) -> None:
        _require_non_empty(self.name, "name")
        if self.commit_sha is not None:
            _require_non_empty(self.commit_sha, "commit_sha")


@dataclass(frozen=True, slots=True)
class CommitRecord:
    """One immutable catalog commit (``commits/<sha>.json``).

    ``parent_sha`` links to the prior commit, forming the immutable history
    chain; ``tree_hash`` pins the manifest tree the commit introduces.
    """

    commit_sha: str
    """Commit SHA-256 hex digest (``commits/<sha>.json``)."""

    parent_sha: str | None
    """Parent commit SHA; ``None`` for the genesis commit."""

    tree_hash: str
    """Hash of the manifest tree this commit pins."""

    created_at_utc: datetime
    """Commit creation timestamp (timezone-aware UTC)."""

    as_of_ist: date
    """IST session boundary the commit is dated to."""

    schema_version: int
    """Manifest/tree schema version written by this commit."""

    summary: str
    """Human-readable commit summary (e.g. pipeline + date)."""

    def __post_init__(self) -> None:
        _require_non_empty(self.commit_sha, "commit_sha")
        _require_non_empty(self.tree_hash, "tree_hash")
        _require_non_empty(self.summary, "summary")
        if self.as_of_ist is None:
            raise ValueError("as_of_ist must be set")
        object.__setattr__(
            self, "created_at_utc", _require_utc(self.created_at_utc, "created_at_utc")
        )


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """One logical-coordinate to content-addressed-object manifest row.

    ``time_bucket`` is the right-sized IST partition bucket
    (e.g. ``"year=2024"``, ``"date=2024-11-17"``); ``sort_key`` is the
    clustering key the object is sorted by (e.g. ``"(instrument_uid, ts)"``)
    and drives row-group pruning.
    """

    domain: str
    """Logical domain (e.g. ``"market"``, ``"derivatives"``, ``"reference"``)."""

    dataset: str
    """Dataset name within the domain (e.g. ``"bars"``, ``"aliases"``)."""

    exchange: str | None
    """Exchange code (e.g. ``"NSE"``) or ``None`` for exchange-agnostic
    datasets."""

    asset_class: str | None
    """Asset class (e.g. ``"EQ"``, ``"FUT"``) or ``None``."""

    timeframe: str | None
    """Bar timeframe (e.g. ``"1d"``, ``"5m"``) or ``None`` when not
    timeframed."""

    time_bucket: str
    """Right-sized IST partition bucket (e.g. ``"year=2024"``)."""

    object_sha: str
    """Content-addressed object SHA (``objects/sha256/<ab>/<sha>.parquet``)."""

    min_ts: datetime
    """Minimum timestamp covered by the object (timezone-aware UTC)."""

    max_ts: datetime
    """Maximum timestamp covered by the object (timezone-aware UTC)."""

    row_count: int
    """Number of rows in the object."""

    sort_key: str
    """Clustering key the object is sorted by (e.g. ``"(instrument_uid, ts)"``)."""

    def __post_init__(self) -> None:
        _require_non_empty(self.domain, "domain")
        _require_non_empty(self.dataset, "dataset")
        _require_non_empty(self.time_bucket, "time_bucket")
        _require_non_empty(self.object_sha, "object_sha")
        _require_non_empty(self.sort_key, "sort_key")
        if self.row_count < 0:
            raise ValueError("row_count must be non-negative")
        object.__setattr__(self, "min_ts", _require_utc(self.min_ts, "min_ts"))
        object.__setattr__(self, "max_ts", _require_utc(self.max_ts, "max_ts"))
        if self.max_ts < self.min_ts:
            raise ValueError("max_ts must be >= min_ts")


@dataclass(frozen=True, slots=True)
class InstrumentAlias:
    """Point-in-time ticker alias mapping to one canonical ``instrument_uid``.

    A symbol rename is two aliases over the same ``instrument_uid`` with
    adjacent ``[valid_from, valid_to]`` windows, so historical series stay
    continuous (``ZOMATO`` -> ``ETERNAL`` on their ex-date).
    """

    exchange: str
    """Exchange code (e.g. ``"NSE"``)."""

    symbol: str
    """Ticker symbol as traded at the time (e.g. ``"ZOMATO"`` or ``"ETERNAL"``)."""

    series: str | None
    """Series (e.g. ``"EQ"``) or ``None``."""

    valid_from: date | None
    """First day this alias applies (inclusive)."""

    valid_to: date | None
    """Last day this alias applies (inclusive); ``None`` = still valid."""

    instrument_uid: str
    """Canonical immutable instrument identity for every alias of the series."""

    def __post_init__(self) -> None:
        _require_non_empty(self.exchange, "exchange")
        _require_non_empty(self.symbol, "symbol")
        _require_non_empty(self.instrument_uid, "instrument_uid")
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_to < self.valid_from
        ):
            raise ValueError("valid_to must be >= valid_from")
