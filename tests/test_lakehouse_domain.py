"""Lakehouse v3 domain types + CatalogRepository ref surface (plan 0009 Phase A).

Covers the four Phase A value objects (``CatalogRef``, ``CommitRecord``,
``ManifestEntry``, ``InstrumentAlias``) and the backward-compatible
``CatalogRepository`` extension (``as_of_ref`` keyword defaults plus the
``resolve_ref`` / ``commit`` / ``tag`` manifest surface).
"""

from __future__ import annotations

import inspect
from dataclasses import fields
from datetime import UTC, date, datetime
from typing import get_args, get_type_hints

import polars as pl
import pytest

from tantra_data.domain import CatalogRef, CommitRecord, InstrumentAlias, ManifestEntry
from tantra_data.ports.repositories import CatalogRepository


def utc(y: int, m: int, d: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(y, m, d, hour, minute, tzinfo=UTC)


def _commit(
    sha: str, *, created: datetime, parent: str | None = None, summary: str = "ingest"
) -> CommitRecord:
    return CommitRecord(
        commit_sha=sha,
        parent_sha=parent,
        tree_hash="tree-" + sha[:16],
        created_at_utc=created,
        as_of_ist=created.date(),
        schema_version=3,
        summary=summary,
    )


def _entry(**overrides: object) -> ManifestEntry:
    base: dict[str, object] = {
        "domain": "market",
        "dataset": "bars",
        "exchange": "NSE",
        "asset_class": "EQ",
        "timeframe": "1d",
        "time_bucket": "year=2024",
        "object_sha": "ab/" + "b" * 62,
        "min_ts": utc(2024, 1, 1),
        "max_ts": utc(2024, 12, 31),
        "row_count": 250_000,
        "sort_key": "(instrument_uid, ts)",
    }
    base.update(overrides)
    return ManifestEntry(**base)


def _alias(**overrides: object) -> InstrumentAlias:
    base: dict[str, object] = {
        "exchange": "NSE",
        "symbol": "ZOMATO",
        "series": "EQ",
        "valid_from": date(2021, 7, 23),
        "valid_to": date(2024, 9, 13),
        "instrument_uid": "INE999Z01019",
    }
    base.update(overrides)
    return InstrumentAlias(**base)


def _resolve_uid(
    aliases: list[InstrumentAlias], exchange: str, symbol: str, as_of: date
) -> str | None:
    """Pure Phase-A stand-in for the Phase-B alias resolver: pick the alias
    whose ``[valid_from, valid_to]`` window contains ``as_of``."""
    for alias in aliases:
        if alias.exchange != exchange or alias.symbol != symbol:
            continue
        if alias.valid_from is not None and as_of < alias.valid_from:
            continue
        if alias.valid_to is not None and as_of > alias.valid_to:
            continue
        return alias.instrument_uid
    return None


# ---------------------------------------------------------------------------
# Domain types: construction, round-trip, exact (float-free) field types.
# ---------------------------------------------------------------------------


def test_lakehouse_types_construct_and_round_trip() -> None:
    ref = CatalogRef(name="main", commit_sha="a" * 64)
    commit = _commit("a" * 64, created=utc(2026, 11, 17))
    entry = _entry()
    alias = _alias()
    assert ref == CatalogRef(name="main", commit_sha="a" * 64)
    assert commit == _commit("a" * 64, created=utc(2026, 11, 17))
    assert entry == _entry()
    assert alias == _alias()
    for obj in (ref, commit, entry, alias):
        assert repr(obj).startswith(f"{type(obj).__name__}(")


def test_lakehouse_types_declare_no_floats() -> None:
    for cls in (CatalogRef, CommitRecord, ManifestEntry, InstrumentAlias):
        hints = get_type_hints(cls)
        for field_ in fields(cls):
            hint = hints[field_.name]
            assert hint is not float and float not in get_args(hint)


# ---------------------------------------------------------------------------
# CatalogRef: commit_sha=None resolves as the live head.
# ---------------------------------------------------------------------------


def test_catalog_ref_default_commit_sha_is_live_head() -> None:
    genesis = _commit("1" * 64, created=utc(2026, 11, 15))
    head = _commit("2" * 64, created=utc(2026, 11, 17))
    by_sha = {genesis.commit_sha: genesis, head.commit_sha: head}

    def resolve(ref: CatalogRef) -> CommitRecord | None:
        # Phase-A type contract only: the real resolver lands in Phase B.
        if ref.commit_sha is not None:
            return by_sha.get(ref.commit_sha)
        return max(by_sha.values(), key=lambda c: c.created_at_utc)

    live = CatalogRef(name="main")
    assert live.commit_sha is None
    assert live == CatalogRef(name="main", commit_sha=None)
    assert resolve(live) == head
    assert resolve(CatalogRef(name="pin", commit_sha=genesis.commit_sha)) == genesis
    assert resolve(CatalogRef(name="pin", commit_sha="0" * 64)) is None


def test_catalog_ref_rejects_empty_name_and_commit_sha() -> None:
    with pytest.raises(ValueError):
        CatalogRef(name="")
    with pytest.raises(ValueError):
        CatalogRef(name="main", commit_sha="")


# ---------------------------------------------------------------------------
# CommitRecord: parent-linked history, as_of_ist / schema_version carried.
# ---------------------------------------------------------------------------


def test_commit_record_chain_parent_sha_links_to_prior() -> None:
    genesis = _commit("1" * 64, created=utc(2026, 11, 15), summary="genesis")
    second = _commit(
        "2" * 64, created=utc(2026, 11, 16), parent=genesis.commit_sha, summary="second"
    )
    third = _commit("3" * 64, created=utc(2026, 11, 17), parent=second.commit_sha, summary="third")

    assert genesis.parent_sha is None
    assert second.parent_sha == genesis.commit_sha
    assert third.parent_sha == second.commit_sha

    # Walk the chain from the head back to genesis via parent_sha.
    by_sha = {c.commit_sha: c for c in (genesis, second, third)}
    walk: list[str] = []
    sha: str | None = third.commit_sha
    while sha is not None:
        walk.append(sha)
        sha = by_sha[sha].parent_sha
    assert walk == [third.commit_sha, second.commit_sha, genesis.commit_sha]


def test_commit_record_carries_as_of_ist_and_schema_version() -> None:
    commit = _commit(
        "c" * 64,
        created=utc(2026, 11, 17, 6, 30),  # 12:00 IST
        summary="ist boundary",
    )
    assert commit.as_of_ist == date(2026, 11, 17)
    assert commit.schema_version == 3
    assert commit.created_at_utc == utc(2026, 11, 17, 6, 30)
    assert commit == _commit(
        "c" * 64,
        created=utc(2026, 11, 17, 6, 30),
        summary="ist boundary",
    )


def test_commit_record_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError):
        CommitRecord(
            commit_sha="c" * 64,
            parent_sha=None,
            tree_hash="d" * 64,
            created_at_utc=datetime(2026, 11, 17, 6, 30),  # naive
            as_of_ist=date(2026, 11, 17),
            schema_version=3,
            summary="naive",
        )


# ---------------------------------------------------------------------------
# ManifestEntry: time_bucket / sort_key invariants, exact int counts.
# ---------------------------------------------------------------------------


def test_manifest_entry_sort_key_and_time_bucket_invariants() -> None:
    entry = _entry()
    assert entry.time_bucket == "year=2024"
    assert entry.sort_key == "(instrument_uid, ts)"
    assert isinstance(entry.row_count, int)
    assert entry.row_count >= 0
    assert entry.max_ts > entry.min_ts
    for field_ in fields(ManifestEntry):
        value = getattr(entry, field_.name)
        assert value is None or isinstance(value, (str, int, datetime))


def test_manifest_entry_rejects_bad_bounds_and_naive_datetimes() -> None:
    with pytest.raises(ValueError):
        _entry(min_ts=utc(2024, 1, 2), max_ts=utc(2024, 1, 1))
    with pytest.raises(ValueError):
        _entry(min_ts=datetime(2024, 1, 1), max_ts=utc(2024, 1, 2))
    with pytest.raises(ValueError):
        _entry(row_count=-1)


# ---------------------------------------------------------------------------
# InstrumentAlias: rename continuity across the ZOMATO -> ETERNAL ex-date.
# ---------------------------------------------------------------------------


def test_instrument_alias_rename_continuity_zomato_to_eternal() -> None:
    uid = "INE999Z01019"
    aliases = [
        _alias(symbol="ZOMATO", valid_from=date(2021, 7, 23), valid_to=date(2024, 9, 13)),
        _alias(symbol="ETERNAL", valid_from=date(2024, 9, 14), valid_to=None),
    ]
    # The old ticker resolves up to its last valid day; the new ticker from
    # its first — both point at the same canonical instrument_uid.
    assert _resolve_uid(aliases, "NSE", "ZOMATO", date(2024, 9, 13)) == uid
    assert _resolve_uid(aliases, "NSE", "ETERNAL", date(2024, 9, 14)) == uid
    # At no point do both aliases cover the same date (no data duplication).
    assert _resolve_uid(aliases, "NSE", "ZOMATO", date(2024, 9, 14)) is None
    assert _resolve_uid(aliases, "NSE", "ETERNAL", date(2024, 9, 13)) is None
    # Every resolvable (symbol, date) maps to the single canonical identity.
    resolved: set[str] = set()
    for symbol in ("ZOMATO", "ETERNAL"):
        for day in (date(2024, 9, 13), date(2024, 9, 14)):
            hit = _resolve_uid(aliases, "NSE", symbol, day)
            if hit is not None:
                resolved.add(hit)
    assert resolved == {uid}


def test_instrument_alias_rejects_reversed_window_and_empty_symbol() -> None:
    with pytest.raises(ValueError):
        _alias(valid_from=date(2024, 9, 14), valid_to=date(2024, 9, 13))
    with pytest.raises(ValueError):
        _alias(symbol="")


# ---------------------------------------------------------------------------
# CatalogRepository: backward-compatible protocol surface.
# ---------------------------------------------------------------------------


class _OriginalShapeCatalog:
    """A pre-v3 CatalogRepository implementation (the original two methods)."""

    def write_bars(self, instrument: str, bars: pl.DataFrame) -> None:
        del instrument, bars

    def read_bars(
        self,
        instrument: str,
        start: date | None = None,
        end: date | None = None,
        interval: str = "1d",
    ) -> pl.DataFrame:
        del instrument, start, end, interval
        return pl.DataFrame(schema={"ts": pl.Datetime("us")})


class _V3ShapeCatalog:
    """A full v3 CatalogRepository implementation (original + ref surface)."""

    def write_bars(self, instrument: str, bars: pl.DataFrame, *, as_of_ref: str = "main") -> None:
        del instrument, bars, as_of_ref

    def read_bars(
        self,
        instrument: str,
        start: date | None = None,
        end: date | None = None,
        interval: str = "1d",
        *,
        as_of_ref: str = "main",
    ) -> pl.DataFrame:
        del instrument, start, end, interval, as_of_ref
        return pl.DataFrame(schema={"ts": pl.Datetime("us")})

    def resolve_ref(self, ref: str) -> CommitRecord | None:
        del ref
        return None

    def commit(self, summary: str, *, as_of_ist: date, schema_version: int) -> CommitRecord:
        del summary, as_of_ist, schema_version
        return _commit("9" * 64, created=utc(2026, 11, 17), summary="stub")

    def tag(self, tag: str, ref: str = "main") -> None:
        del tag, ref


def test_catalog_repository_remains_runtime_checkable_full_v3_shape() -> None:
    # The @runtime_checkable decorator is kept; the full v3 surface is a
    # runtime match (original two methods + the manifest/ref surface).
    assert isinstance(_V3ShapeCatalog(), CatalogRepository)


def test_catalog_repository_original_shape_runtime_semantics() -> None:
    # ``@runtime_checkable`` isinstance() requires *every* protocol member to
    # be present, so adding required members means a two-method stub is no
    # longer a runtime match. Back-compat for pre-existing implementations and
    # call-sites is preserved through the ``as_of_ref`` keyword defaults, not
    # through isinstance().
    assert not isinstance(_OriginalShapeCatalog(), CatalogRepository)


def test_catalog_repository_as_of_ref_keyword_defaults() -> None:
    write_sig = inspect.signature(CatalogRepository.write_bars)
    read_sig = inspect.signature(CatalogRepository.read_bars)
    assert write_sig.parameters["as_of_ref"].default == "main"
    assert read_sig.parameters["as_of_ref"].default == "main"
    assert write_sig.parameters["as_of_ref"].kind is inspect.Parameter.KEYWORD_ONLY
    assert read_sig.parameters["as_of_ref"].kind is inspect.Parameter.KEYWORD_ONLY
    # Pre-existing positional call-shapes are untouched.
    assert list(write_sig.parameters)[:2] == ["self", "instrument"]


def test_catalog_repository_manifest_surface_present() -> None:
    assert {"resolve_ref", "commit", "tag"}.issubset(CatalogRepository.__protocol_attrs__)
