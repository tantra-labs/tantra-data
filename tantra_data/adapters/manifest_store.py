"""DuckDB-backed manifest and ref manager for the v3 content-addressed lakehouse.

Implements ADR 0004 §2.1 / plan 0009 Phase B and the v3 snapshot contract
(``docs/design/lakehouse-v3-snapshot-contract.md``): ``{root}/catalog.duckdb``
holds four parameterized tables — ``commits``, ``manifest_entries``, ``refs``,
and ``instrument_aliases`` — mirroring the immutable ``commits/<sha>.json``,
``trees/<tree_hash>.json``, and ``refs/{heads,tags}/*`` files on disk.

Commit protocol:
1. Validate the incoming entries (Zero-Floats, monotonic ``(min_ts, max_ts)``,
   schema version) *before* touching any state.
2. Unless ``entries_replace_only=True``, merge the ``parent`` commit's entries
   keyed by ``(domain, dataset, exchange, asset_class, timeframe, time_bucket)``
   so new entries replace matching keys while unrelated datasets stay intact.
3. Write the immutable ``trees/<tree_hash>.json`` and ``commits/<sha>.json``.
4. Insert the ``manifest_entries`` rows for the full snapshot.
5. Atomically fast-forward ``refs/heads/main`` (or
   ``refs/heads/staging/<pipeline>`` then ``main``).

Any validation failure raises and leaves refs and commit history untouched —
readers never observe a partial commit.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import re
import uuid
from dataclasses import asdict, fields
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from tantra_data.domain.lakehouse import CommitRecord, InstrumentAlias, ManifestEntry

#: Manifest/tree schema version this store reads and writes (v3 lakehouse).
SCHEMA_VERSION = 3

EntryKey = tuple[str, str, str | None, str | None, str | None, str]

_COMMITS_DDL = """
CREATE TABLE IF NOT EXISTS commits (
    commit_sha     VARCHAR PRIMARY KEY,
    parent_sha     VARCHAR,
    tree_hash      VARCHAR,
    created_at_utc TIMESTAMP,
    as_of_ist      DATE,
    schema_version INTEGER,
    summary        VARCHAR
)
"""

_MANIFEST_ENTRIES_DDL = """
CREATE TABLE IF NOT EXISTS manifest_entries (
    commit_sha   VARCHAR,
    domain       VARCHAR,
    dataset      VARCHAR,
    exchange     VARCHAR,
    asset_class  VARCHAR,
    timeframe    VARCHAR,
    time_bucket  VARCHAR,
    object_sha   VARCHAR,
    min_ts       TIMESTAMP,
    max_ts       TIMESTAMP,
    row_count    BIGINT,
    sort_key     VARCHAR
)
"""

_REFS_DDL = """
CREATE TABLE IF NOT EXISTS refs (
    name       VARCHAR PRIMARY KEY,
    commit_sha VARCHAR,
    kind       VARCHAR
)
"""

_ALIASES_DDL = """
CREATE TABLE IF NOT EXISTS instrument_aliases (
    exchange       VARCHAR,
    symbol         VARCHAR,
    series         VARCHAR,
    valid_from     DATE,
    valid_to       DATE,
    instrument_uid VARCHAR
)
"""

_VALID_REF = re.compile(r"^refs/(heads|tags)/[A-Za-z0-9._/+-]+$")
_SHA = re.compile(r"^[0-9a-f]{64}$")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _to_naive_utc(value: datetime) -> datetime:
    """Strip tzinfo for TIMESTAMP columns (all stored times are UTC)."""
    return _utc(value).replace(tzinfo=None)


def _entry_key(entry: ManifestEntry) -> EntryKey:
    """Logical partition coordinate identifying one entry slot in a snapshot."""
    return (
        entry.domain,
        entry.dataset,
        entry.exchange,
        entry.asset_class,
        entry.timeframe,
        entry.time_bucket,
    )


def _entry_sort_key(
    entry: ManifestEntry,
) -> tuple[datetime, datetime, str, str, str, str, str, str, str]:
    """Deterministic ordering for snapshot trees and DuckDB rows."""
    return (
        entry.min_ts,
        entry.max_ts,
        entry.domain,
        entry.dataset,
        entry.exchange or "",
        entry.asset_class or "",
        entry.timeframe or "",
        entry.time_bucket,
        entry.object_sha,
    )


def _commit_payload(
    *,
    parent_sha: str | None,
    tree_hash: str,
    created_at_utc: datetime,
    as_of_ist: date,
    schema_version: int,
    summary: str,
) -> dict[str, object]:
    return {
        "parent_sha": parent_sha,
        "tree_hash": tree_hash,
        "created_at_utc": _utc(created_at_utc).isoformat(),
        "as_of_ist": as_of_ist.isoformat(),
        "schema_version": schema_version,
        "summary": summary,
    }


def _tree_payload(entries: list[ManifestEntry]) -> list[dict[str, object]]:
    return [
        {
            k: (v.isoformat() if isinstance(v, (date, datetime)) else v)
            for k, v in asdict(entry).items()
        }
        for entry in sorted(entries, key=_entry_sort_key)
    ]


def _validate_entries(entries: list[ManifestEntry], schema_version: int) -> None:
    """Zero-Floats + monotonic ``(min_ts, max_ts)`` + schema-version gate."""
    if schema_version != SCHEMA_VERSION:
        raise ValueError(
            f"schema_version mismatch: got {schema_version}, store supports {SCHEMA_VERSION}"
        )
    for entry in entries:
        for field in fields(entry):
            value = getattr(entry, field.name)
            if isinstance(value, float):
                raise ValueError(
                    f"zero-floats: ManifestEntry.{field.name} is a float on "
                    f"{entry.domain}/{entry.dataset}/{entry.object_sha}"
                )
    for prev, current in itertools.pairwise(entries):
        if (current.min_ts, current.max_ts) < (prev.min_ts, prev.max_ts):
            raise ValueError(
                "non-monotonic (min_ts, max_ts): manifest entries must be "
                "ordered by min_ts with non-decreasing max_ts"
            )


class ManifestStore:
    """DuckDB manifest engine over ``{root}/catalog.duckdb``.

    Ref pointers live both as git-style files (``{root}/refs/...``) and as
    rows in the ``refs`` table; commits live both as immutable JSON files
    (``{root}/commits/<sha>.json`` and ``{root}/trees/<tree_hash>.json``)
    and as rows in the ``commits`` table. The files are authoritative for
    ref and tree resolution so external tools (the Rust consumers) can read
    the exact same contract without needing DuckDB.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(self.root / "catalog.duckdb"))
        self._con.execute(_COMMITS_DDL)
        self._con.execute(_MANIFEST_ENTRIES_DDL)
        self._con.execute(_REFS_DDL)
        self._con.execute(_ALIASES_DDL)

    # ------------------------------------------------------------------
    # refs / commits
    # ------------------------------------------------------------------

    def resolve_ref(self, ref: str) -> CommitRecord | None:
        """Resolve ``ref`` (branch or tag name) to its pinned commit record."""
        sha = self._ref_sha(ref)
        if sha is None:
            return None
        return self._load_commit(sha)

    def entries_for_commit(self, commit_sha: str) -> list[ManifestEntry]:
        """Return all manifest entries in the snapshot for ``commit_sha``.

        Reads the immutable ``trees/<tree_hash>.json`` file when present and
        falls back to DuckDB ``manifest_entries`` rows otherwise.
        """
        commit = self._load_commit(commit_sha)
        if commit is not None:
            tree_file = self.root / "trees" / f"{commit.tree_hash}.json"
            if tree_file.exists():
                payload = json.loads(tree_file.read_text())
                return [
                    ManifestEntry(
                        domain=item["domain"],
                        dataset=item["dataset"],
                        exchange=item.get("exchange"),
                        asset_class=item.get("asset_class"),
                        timeframe=item.get("timeframe"),
                        time_bucket=item["time_bucket"],
                        object_sha=item["object_sha"],
                        min_ts=_utc(datetime.fromisoformat(item["min_ts"])),
                        max_ts=_utc(datetime.fromisoformat(item["max_ts"])),
                        row_count=int(item["row_count"]),
                        sort_key=item["sort_key"],
                    )
                    for item in payload
                ]
        rows = self._con.execute(
            "SELECT domain, dataset, exchange, asset_class, timeframe, time_bucket, "
            "object_sha, min_ts, max_ts, row_count, sort_key "
            "FROM manifest_entries WHERE commit_sha = ? ORDER BY min_ts, max_ts",
            [commit_sha],
        ).fetchall()
        return [
            ManifestEntry(
                domain=r[0],
                dataset=r[1],
                exchange=r[2],
                asset_class=r[3],
                timeframe=r[4],
                time_bucket=r[5],
                object_sha=r[6],
                min_ts=_utc(r[7]),
                max_ts=_utc(r[8]),
                row_count=r[9],
                sort_key=r[10],
            )
            for r in rows
        ]

    def commit(
        self,
        summary: str,
        *,
        as_of_ist: date,
        schema_version: int,
        entries: list[ManifestEntry],
        parent: CommitRecord | None = None,
        pipeline: str | None = None,
        entries_replace_only: bool = False,
    ) -> CommitRecord:
        """Create an immutable snapshot commit, then atomically fast-forward main."""
        _validate_entries(entries, schema_version)

        if parent is not None and not entries_replace_only:
            merged: dict[EntryKey, ManifestEntry] = {
                _entry_key(item): item for item in self.entries_for_commit(parent.commit_sha)
            }
            for item in entries:
                merged[_entry_key(item)] = item
            effective_entries = sorted(merged.values(), key=_entry_sort_key)
        else:
            effective_entries = sorted(entries, key=_entry_sort_key)

        tree_payload = _tree_payload(effective_entries)
        tree_canonical = json.dumps(tree_payload, sort_keys=True, separators=(",", ":"))
        tree_hash = hashlib.sha256(tree_canonical.encode("utf-8")).hexdigest()

        created_at_utc = datetime.now(UTC)
        parent_sha = parent.commit_sha if parent is not None else None
        payload = _commit_payload(
            parent_sha=parent_sha,
            tree_hash=tree_hash,
            created_at_utc=created_at_utc,
            as_of_ist=as_of_ist,
            schema_version=schema_version,
            summary=summary,
        )
        commit_sha = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        record = CommitRecord(
            commit_sha=commit_sha,
            parent_sha=parent_sha,
            tree_hash=tree_hash,
            created_at_utc=created_at_utc,
            as_of_ist=as_of_ist,
            schema_version=schema_version,
            summary=summary,
        )

        # 1. Immutable tree JSON + commit JSON (atomic tmp -> rename).
        tree_file = self.root / "trees" / f"{tree_hash}.json"
        tree_existed = tree_file.exists()
        if not tree_existed:
            tree_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_tree = tree_file.with_name(f".{uuid.uuid4().hex}.json")
            tmp_tree.write_text(tree_canonical + "\n")
            os.replace(tmp_tree, tree_file)

        commit_file = self.root / "commits" / f"{commit_sha}.json"
        commit_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_json = commit_file.with_name(f".{uuid.uuid4().hex}.json")
        tmp_json.write_text(json.dumps(payload, sort_keys=True) + "\n")
        os.replace(tmp_json, commit_file)

        # 2. DuckDB rows (commits + manifest entries) in one transaction.
        try:
            self._con.execute("BEGIN")
            self._con.execute(
                "INSERT OR REPLACE INTO commits VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    commit_sha,
                    parent_sha,
                    tree_hash,
                    _to_naive_utc(created_at_utc),
                    as_of_ist,
                    schema_version,
                    summary,
                ],
            )
            for entry in effective_entries:
                self._con.execute(
                    "INSERT INTO manifest_entries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        commit_sha,
                        entry.domain,
                        entry.dataset,
                        entry.exchange,
                        entry.asset_class,
                        entry.timeframe,
                        entry.time_bucket,
                        entry.object_sha,
                        _to_naive_utc(entry.min_ts),
                        _to_naive_utc(entry.max_ts),
                        entry.row_count,
                        entry.sort_key,
                    ],
                )
            self._con.execute("COMMIT")
        except Exception:
            self._con.execute("ROLLBACK")
            commit_file.unlink(missing_ok=True)
            if not tree_existed:
                tree_file.unlink(missing_ok=True)
            raise

        # 3. Atomically fast-forward staging/<pipeline> (optional) then main.
        if pipeline is not None:
            self._set_ref(f"refs/heads/staging/{pipeline}", commit_sha, kind="head")
        self._set_ref("refs/heads/main", commit_sha, kind="head")
        return record

    def tag(self, tag: str, ref: str = "main") -> None:
        """Pin ``refs/tags/<tag>`` to the commit ``ref`` currently resolves to."""
        sha = self._ref_sha(ref)
        if sha is None:
            raise ValueError(f"cannot tag {tag!r}: ref {ref!r} does not resolve")
        self._set_ref(f"refs/tags/{tag}", sha, kind="tag")

    def _set_ref(self, name: str, sha: str, *, kind: str) -> None:
        """Write a ref pointer (file + table) atomically."""
        if not _VALID_REF.match(name):
            raise ValueError(f"invalid ref name: {name!r}")
        ref_file = self.root / name
        ref_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = ref_file.with_name(f".{uuid.uuid4().hex}.ref")
        tmp_file.write_text(sha + "\n")
        os.replace(tmp_file, ref_file)
        self._con.execute(
            "INSERT OR REPLACE INTO refs (name, commit_sha, kind) VALUES (?, ?, ?)",
            [name, sha, kind],
        )

    def _ref_sha(self, ref: str) -> str | None:
        """Resolve a ref name to a commit sha (file authoritative, table fallback).

        A short name (e.g. ``"backtest-2026-11-17"``) is tried as a head first,
        then as a tag, so both ``resolve_ref("refs/tags/<tag>")`` and
        ``resolve_ref("<tag>")`` pin the same commit.
        """
        if _SHA.match(ref):
            return ref
        candidates = [ref] if ref.startswith("refs/") else [f"refs/heads/{ref}", f"refs/tags/{ref}"]
        for name in candidates:
            ref_file = self.root / name
            if ref_file.exists():
                content = ref_file.read_text().strip()
                if _SHA.match(content):
                    return content
            row = self._con.execute("SELECT commit_sha FROM refs WHERE name = ?", [name]).fetchone()
            if row:
                return str(row[0])
        return None

    def _load_commit(self, sha: str) -> CommitRecord | None:
        commit_file = self.root / "commits" / f"{sha}.json"
        if commit_file.exists():
            payload = json.loads(commit_file.read_text())
            return CommitRecord(
                commit_sha=sha,
                parent_sha=payload.get("parent_sha"),
                tree_hash=payload["tree_hash"],
                created_at_utc=datetime.fromisoformat(payload["created_at_utc"]),
                as_of_ist=date.fromisoformat(payload["as_of_ist"]),
                schema_version=payload["schema_version"],
                summary=payload["summary"],
            )
        row = self._con.execute(
            "SELECT commit_sha, parent_sha, tree_hash, created_at_utc, as_of_ist, "
            "schema_version, summary FROM commits WHERE commit_sha = ?",
            [sha],
        ).fetchone()
        if row is None:
            return None
        created_at_utc = row[3].replace(tzinfo=UTC) if row[3].tzinfo is None else row[3]
        return CommitRecord(
            commit_sha=row[0],
            parent_sha=row[1],
            tree_hash=row[2],
            created_at_utc=created_at_utc,
            as_of_ist=row[4],
            schema_version=row[5],
            summary=row[6],
        )

    # ------------------------------------------------------------------
    # partition resolution
    # ------------------------------------------------------------------

    def resolve_partition(
        self,
        domain: str,
        dataset: str,
        exchange: str | None = None,
        asset_class: str | None = None,
        timeframe: str | None = None,
        time_bucket: str | None = None,
        *,
        ref: str = "main",
    ) -> list[ManifestEntry]:
        """Return the manifest entries for a logical coordinate at ``ref``."""
        commit = self.resolve_ref(ref)
        if commit is None:
            return []
        sql = (
            "SELECT domain, dataset, exchange, asset_class, timeframe, time_bucket, "
            "object_sha, min_ts, max_ts, row_count, sort_key "
            "FROM manifest_entries WHERE commit_sha = ? AND domain = ? AND dataset = ?"
        )
        params: list[object] = [commit.commit_sha, domain, dataset]
        for column, value in (
            ("exchange", exchange),
            ("asset_class", asset_class),
            ("timeframe", timeframe),
            ("time_bucket", time_bucket),
        ):
            if value is not None:
                sql += f" AND {column} = ?"
                params.append(value)
        sql += " ORDER BY min_ts, max_ts"
        rows = self._con.execute(sql, params).fetchall()
        return [
            ManifestEntry(
                domain=r[0],
                dataset=r[1],
                exchange=r[2],
                asset_class=r[3],
                timeframe=r[4],
                time_bucket=r[5],
                object_sha=r[6],
                min_ts=_utc(r[7]),
                max_ts=_utc(r[8]),
                row_count=r[9],
                sort_key=r[10],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # instrument aliases
    # ------------------------------------------------------------------

    def write_alias(self, alias: InstrumentAlias) -> None:
        """Upsert one point-in-time ticker alias (key: exchange+symbol+series)."""
        self._con.execute(
            "DELETE FROM instrument_aliases WHERE exchange = ? AND symbol = ? "
            "AND series IS NOT DISTINCT FROM ?",
            [alias.exchange, alias.symbol, alias.series],
        )
        self._con.execute(
            "INSERT INTO instrument_aliases VALUES (?, ?, ?, ?, ?, ?)",
            [
                alias.exchange,
                alias.symbol,
                alias.series,
                alias.valid_from,
                alias.valid_to,
                alias.instrument_uid,
            ],
        )

    def resolve_alias(
        self,
        exchange: str,
        symbol: str,
        *,
        as_of: date | None = None,
    ) -> str | None:
        """Resolve ``(exchange, symbol)`` to its canonical ``instrument_uid``.

        With ``as_of`` set, only aliases whose ``[valid_from, valid_to]``
        window covers that day match (ex-date boundary continuity). Without
        it, the most recent alias wins (the live ticker).
        """
        if as_of is None:
            row = self._con.execute(
                "SELECT instrument_uid FROM instrument_aliases "
                "WHERE exchange = ? AND symbol = ? "
                "ORDER BY valid_from IS NULL, valid_from DESC NULLS LAST "
                "LIMIT 1",
                [exchange, symbol],
            ).fetchone()
            return row[0] if row else None
        row = self._con.execute(
            "SELECT instrument_uid FROM instrument_aliases "
            "WHERE exchange = ? AND symbol = ? "
            "AND (valid_from IS NULL OR valid_from <= ?) "
            "AND (valid_to IS NULL OR valid_to >= ?) "
            "ORDER BY valid_from IS NULL, valid_from DESC NULLS LAST "
            "LIMIT 1",
            [exchange, symbol, as_of, as_of],
        ).fetchone()
        return row[0] if row else None

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._con.close()

    def __enter__(self) -> ManifestStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


__all__ = ["SCHEMA_VERSION", "ManifestStore"]
