"""Durable local fallback (spec §7.4): events Cassandra could not take, kept unsequenced.

SQLite in WAL mode with synchronous=FULL, so an acknowledged `spooled` survives a crash.
Events are sequenced and hashed only when they drain through the normal write path.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from .core.models import EventIn

log = structlog.get_logger()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS spooled (
    id          INTEGER PRIMARY KEY,
    shard       INT  NOT NULL,
    source      TEXT NOT NULL,
    source_ref  TEXT NOT NULL,
    event_json  TEXT NOT NULL,
    spooled_us  INT  NOT NULL,
    UNIQUE (source, source_ref)
);
CREATE INDEX IF NOT EXISTS spooled_by_shard ON spooled (shard, id);
CREATE TABLE IF NOT EXISTS dead_letters (
    id          INTEGER PRIMARY KEY,
    shard       INT  NOT NULL,
    source      TEXT NOT NULL,
    source_ref  TEXT NOT NULL,
    event_json  TEXT NOT NULL,
    spooled_us  INT  NOT NULL,
    error       TEXT NOT NULL,
    failed_us   INT  NOT NULL
);
"""


@dataclass(frozen=True)
class SpoolStats:
    events: int
    oldest_us: int | None


def _is_under(path: Path, root: str | None) -> bool:
    return bool(root) and path.is_relative_to(Path(root).resolve())


class Spool:
    def __init__(self, path: Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path.resolve()
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(_SCHEMA)
        data_dir = os.environ.get("MAILROOM_ARCHIVE_DATA_DIR")
        self.ephemeral = _is_under(self.path, tempfile.gettempdir()) or not _is_under(self.path, data_dir)
        if self.ephemeral:
            log.warning("archive_spool_ephemeral", path=str(self.path))

    def append(self, shard: int, ev: EventIn) -> bool:
        cur = self.db.execute(
            "INSERT OR IGNORE INTO spooled (shard, source, source_ref, event_json, spooled_us) "
            "VALUES (?, ?, ?, ?, ?)",
            (shard, ev.source, ev.source_ref, ev.model_dump_json(), time.time_ns() // 1000),
        )
        return cur.rowcount == 1

    def pending_shards(self) -> set[int]:
        return {r[0] for r in self.db.execute("SELECT DISTINCT shard FROM spooled")}

    def has_pending(self, shard: int) -> bool:
        row = self.db.execute("SELECT 1 FROM spooled WHERE shard = ? LIMIT 1", (shard,)).fetchone()
        return row is not None

    def peek(self, shard: int, limit: int = 100) -> list[tuple[int, EventIn]]:
        rows = self.db.execute(
            "SELECT id, event_json FROM spooled WHERE shard = ? ORDER BY id LIMIT ?", (shard, limit)
        )
        return [(i, EventIn.model_validate_json(j)) for i, j in rows]

    def ack(self, ids: list[int]) -> None:
        if ids:
            self.db.executemany("DELETE FROM spooled WHERE id = ?", [(i,) for i in ids])

    def dead_letter(self, spool_id: int, error: str) -> None:
        """Move an event that can never be written out of the drain path, keeping it for inspection."""
        with self.db:
            self.db.execute("BEGIN")
            self.db.execute(
                "INSERT INTO dead_letters "
                "(id, shard, source, source_ref, event_json, spooled_us, error, failed_us) "
                "SELECT id, shard, source, source_ref, event_json, spooled_us, ?, ? "
                "FROM spooled WHERE id = ?",
                (error, time.time_ns() // 1000, spool_id),
            )
            self.db.execute("DELETE FROM spooled WHERE id = ?", (spool_id,))

    def dead_letters(self) -> int:
        return self.db.execute("SELECT count(*) FROM dead_letters").fetchone()[0]

    def stats(self) -> SpoolStats:
        n, oldest = self.db.execute("SELECT count(*), min(spooled_us) FROM spooled").fetchone()
        return SpoolStats(events=n, oldest_us=oldest)
