"""doc_timeline and its companion tables (spec §6.4, queries Q1–Q3, Q5, Q8)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from cassandra.cluster import Session
from cassandra.query import BatchStatement, BatchType, PreparedStatement

from ..core.ids import event_id_us
from ..core.models import StoredEvent
from ..core.state import Head, StateUpdate
from .session import run

# static columns Q8 may set from a StateUpdate (head_seq/head_hash are always set)
STATE_COLUMNS = frozenset({
    "matter_id", "content_sha256", "channel", "original_filename", "doc_type", "doc_subclass",
    "stage", "stage_at", "audit_entry_hash", "extracted_data", "overrides", "created_at", "sim_run_id"})

_EVENT_COLS = ("doc_id", "seq", "event_id", "kind", "actor", "source", "source_ref", "occurred_us",
               "payload", "payload_ref", "payload_sha256", "prev_hash", "event_hash", "corrects_seq")


@dataclass(frozen=True)
class IdemRecord:
    doc_id: str
    seq: int
    event_hash: bytes
    event_id: UUID
    payload_sha256: bytes
    prev_hash: bytes


def _dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


def _us(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1_000_000)


class TimelineStore:
    def __init__(self, session: Session, keyspace: str):
        self.s, self.ks = session, keyspace
        p = session.prepare
        self._head = p(f"SELECT head_seq, head_hash, matter_id, doc_type, doc_subclass, stage, stage_at, "
                       f"content_sha256, channel, original_filename, extracted_data, overrides, "
                       f"audit_entry_hash FROM {keyspace}.doc_timeline WHERE doc_id = ? LIMIT 1")
        self._insert = p(f"INSERT INTO {keyspace}.doc_timeline ({', '.join(_EVENT_COLS)}) "
                         f"VALUES ({', '.join('?' * len(_EVENT_COLS))})")
        cols = ", ".join(c for c in _EVENT_COLS if c != "doc_id")
        self._all = p(f"SELECT {cols} FROM {keyspace}.doc_timeline WHERE doc_id = ? AND seq > ?")
        self._page = p(f"SELECT {cols} FROM {keyspace}.doc_timeline WHERE doc_id = ? AND seq > ? LIMIT ?")
        self._get_idem = p(f"SELECT doc_id, seq, event_hash, event_id, payload_sha256, prev_hash "
                           f"FROM {keyspace}.idempotency WHERE source = ? AND source_ref = ?")
        self._put_idem = p(f"INSERT INTO {keyspace}.idempotency (source, source_ref, doc_id, seq, "
                           f"event_hash, event_id, payload_sha256, prev_hash) "
                           f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        self._feed = p(f"INSERT INTO {keyspace}.events_by_time (bucket, shard, event_id, doc_id, seq, "
                       f"occurred_us, kind, channel, doc_type, stage, event_hash, sim_run_id) "
                       f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        self._matter_add = p(f"INSERT INTO {keyspace}.docs_by_matter (matter_id, year, created_at, doc_id, "
                             f"doc_type, content_sha256) VALUES (?, ?, ?, ?, ?, ?)")
        self._matter_q = p(f"SELECT created_at, doc_id, doc_type FROM {keyspace}.docs_by_matter "
                           f"WHERE matter_id = ? AND year = ? LIMIT ?")
        self._updates: dict[tuple[str, ...], PreparedStatement] = {}

    # Q1
    async def get_head(self, doc_id: str) -> Head:
        rows = await run(self.s, self._head, (doc_id,))
        if not rows or rows[0]["head_seq"] is None:
            return Head.empty()
        r = rows[0]
        return Head(seq=r["head_seq"], hash=r["head_hash"] or b"", matter_id=r["matter_id"],
                    doc_type=r["doc_type"], doc_subclass=r["doc_subclass"], stage=r["stage"],
                    stage_us=_us(r["stage_at"]), content_sha256=r["content_sha256"], channel=r["channel"],
                    original_filename=r["original_filename"],
                    extracted_data=json.loads(r["extracted_data"]) if r["extracted_data"] else None,
                    overrides=dict(r["overrides"] or {}), audit_entry_hash=r["audit_entry_hash"])

    def _update_stmt(self, names: tuple[str, ...]) -> PreparedStatement:
        if (stmt := self._updates.get(names)) is None:
            sets = ", ".join(f"{n} = ?" for n in (*names, "head_seq", "head_hash"))
            stmt = self.s.prepare(f"UPDATE {self.ks}.doc_timeline SET {sets} WHERE doc_id = ?")
            self._updates[names] = stmt
        return stmt

    # Q8: history row + current state in one single-partition batch (atomic and isolated)
    async def write_event(self, ev: StoredEvent, upd: StateUpdate) -> None:
        unknown = set(upd.fields) - STATE_COLUMNS
        if unknown:
            raise ValueError(f"not a state column: {sorted(unknown)}")
        names = tuple(sorted(upd.fields))
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch.add(self._insert, tuple(getattr(ev, c) if c != "kind" else str(ev.kind) for c in _EVENT_COLS))
        batch.add(self._update_stmt(names),
                  (*(upd.fields[n] for n in names), ev.seq, ev.event_hash, ev.doc_id))
        await run(self.s, batch)

    # Q2 / Q3
    async def timeline(self, doc_id: str, after_seq: int = 0, limit: int | None = None) -> list[StoredEvent]:
        if limit is None:
            rows = await run(self.s, self._all, (doc_id, after_seq))
        else:
            rows = await run(self.s, self._page, (doc_id, after_seq, limit))
        return [StoredEvent(doc_id=doc_id, **{**r, "payload": r["payload"] or b"",
                                              "prev_hash": r["prev_hash"] or b""})
                for r in rows if r["seq"] is not None]

    async def get_idem(self, source: str, source_ref: str) -> IdemRecord | None:
        rows = await run(self.s, self._get_idem, (source, source_ref))
        if not rows:
            return None
        r = rows[0]
        return IdemRecord(**{**r, "prev_hash": r["prev_hash"] or b""})

    async def put_idem(self, source: str, source_ref: str, rec: IdemRecord) -> None:
        await run(self.s, self._put_idem, (source, source_ref, rec.doc_id, rec.seq, rec.event_hash,
                                           rec.event_id, rec.payload_sha256, rec.prev_hash))

    @staticmethod
    def bucket_of(event_id: UUID) -> str:
        return _dt(event_id_us(event_id)).strftime("%Y%m%d%H")

    async def write_feed(self, ev: StoredEvent, *, shard16: int, channel: str | None, doc_type: str | None,
                         stage: str | None, sim_run_id: str | None) -> None:
        await run(self.s, self._feed, (self.bucket_of(ev.event_id), shard16, ev.event_id, ev.doc_id, ev.seq,
                                       ev.occurred_us, str(ev.kind), channel, doc_type, stage, ev.event_hash,
                                       sim_run_id))

    async def add_to_matter(self, matter_id: str, created_us: int, doc_id: str, doc_type: str | None,
                            content_sha256: bytes | None) -> None:
        created = _dt(created_us)
        await run(self.s, self._matter_add, (matter_id, created.year, created, doc_id, doc_type,
                                             content_sha256))

    # Q5
    async def matter_docs(self, matter_id: str, year: int, limit: int = 500) -> list[dict]:
        return await run(self.s, self._matter_q, (matter_id, year, limit))
