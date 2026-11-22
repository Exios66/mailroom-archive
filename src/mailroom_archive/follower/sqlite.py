"""SQLite snapshots and bounded keyset pages (design §7.6).

No pipeline imports or schema initialization: even a missing database is an error,
never an invitation to create one. Each page and its attempts share a snapshot;
the connection closes before any network work, so readers cannot pin a write lock.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

Position = int | tuple[str, str]
JSON_COLUMNS = frozenset({"detail", "scores", "extracted_data"})


class SourceChanged(RuntimeError):
    """Migration committed between pages; restart from acknowledged checkpoints."""


@dataclass(frozen=True)
class Stream:
    name: str
    table: str
    keys: tuple[str, ...]
    legacy: bool = False


LEGACY = Stream("legacy", "audit_log", ("rowid",), legacy=True)
MIGRATED_LEGACY = Stream("legacy_migrated", "audit_log_legacy", ("rowid",), legacy=True)
UNIFIED = Stream("audit_log", "audit_log", ("seq",))
ADDENDA = Stream("audit_addenda", "audit_addenda", ("timestamp", "addendum_id"))
DOCUMENTS = Stream("documents", "documents", ("updated_at", "doc_id"))


def overlap_start(position: Position | None, seconds: int) -> Position | None:
    if not isinstance(position, tuple) or seconds == 0:
        return position
    stamp = position[0]
    dt = datetime.fromisoformat(stamp) - timedelta(seconds=seconds)
    # Preserve the upstream SQLite timestamp representation, including naive UTC,
    # separator, precision and timezone. Keyset ordering uses that representation.
    cutoff = dt.isoformat(sep=" " if " " in stamp else "T", timespec="microseconds")
    if stamp.endswith("Z"):
        cutoff = cutoff.removesuffix("+00:00") + "Z"
    return cutoff, ""


class SQLiteSource:
    def __init__(self, path: Path):
        self.path = path.resolve()

    @contextmanager
    def snapshot(self) -> Iterator[Snapshot]:
        db = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=2)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA query_only = ON")
            db.execute("BEGIN")
            yield Snapshot(db)
        finally:
            db.rollback()
            db.close()


class Snapshot:
    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self.tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        self.migrated = (
            "audit_meta" in self.tables
            and db.execute('SELECT 1 FROM audit_meta WHERE "key" = ?', ("migration",)).fetchone() is not None
        )

    def streams(self) -> list[Stream]:
        if not self.migrated:
            streams = [LEGACY]
        else:
            streams = [MIGRATED_LEGACY, UNIFIED, ADDENDA]
        streams.append(DOCUMENTS)
        return [s for s in streams if s.table in self.tables]

    def end_position(self, stream: Stream) -> Position | None:
        keys = ", ".join(stream.keys)
        order = ", ".join(f"{key} DESC" for key in stream.keys)
        row = self.db.execute(f"SELECT {keys} FROM {stream.table} ORDER BY {order} LIMIT 1").fetchone()
        if row is None:
            return None
        return row[0] if len(row) == 1 else tuple(row)

    def pages(
        self,
        stream: Stream,
        position: Position | None,
        *,
        limit: int,
        overlap_seconds: int,
        through: Position | None = None,
    ) -> Iterator[list[tuple[Position, dict[str, Any]]]]:
        if stream not in self.streams():
            raise SourceChanged("source schema changed between follower pages")
        after = overlap_start(position, overlap_seconds)
        keys = ", ".join(stream.keys)
        # Identifiers come exclusively from the fixed stream definitions above.
        while True:
            params: list[Any] = []
            predicates = []
            if after is not None:
                values = after if isinstance(after, tuple) else (after,)
                predicates.append(f"({keys}) > ({', '.join('?' for _ in values)})")
                params.extend(values)
            if through is not None:
                values = through if isinstance(through, tuple) else (through,)
                predicates.append(f"({keys}) <= ({', '.join('?' for _ in values)})")
                params.extend(values)
            where = "WHERE " + " AND ".join(predicates) if predicates else ""
            params.append(limit)
            rows = self.db.execute(
                f"SELECT rowid AS _archive_rowid, * FROM {stream.table} {where} ORDER BY {keys} LIMIT ?",
                params,
            ).fetchall()
            if not rows:
                return
            page = []
            for row in rows:
                raw = dict(row)
                values = tuple(raw["_archive_rowid" if k == "rowid" else k] for k in stream.keys)
                pos = values[0] if len(values) == 1 else values
                raw.pop("_archive_rowid")
                page.append((pos, self.decode(raw)))
            yield page
            after = page[-1][0]

    @staticmethod
    def decode(row: dict[str, Any]) -> dict[str, Any]:
        for key in JSON_COLUMNS & row.keys():
            if isinstance(row[key], str):
                row[key] = json.loads(row[key])
        return row

    def attempts(self, parent_id: str) -> Iterator[dict[str, Any]]:
        if "audit_attempts" not in self.tables:
            return
        rows = self.db.execute(
            "SELECT * FROM audit_attempts WHERE parent_id = ? ORDER BY attempt_no, attempt_id", (parent_id,)
        )
        for row in rows:
            yield self.decode(dict(row))
