"""Arrivals, duplicate sightings and blob rows (spec §6.4, query Q4)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime

from cassandra.cluster import Session
from cassandra.util import uuid_from_time

from ..core.canonical import canonical_bytes
from ..core.ids import new_arrival_id
from .session import run


@dataclass(frozen=True)
class Arrival:
    arrival_id: str
    duplicate_of: str | None


def _dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


def _months_back(us: int, months: int) -> list[int]:
    """yyyymm for the month containing `us` and the months - 1 before it, newest first."""
    d = _dt(us)
    idx = d.year * 12 + d.month - 1
    return [(i // 12) * 100 + i % 12 + 1 for i in range(idx, idx - months, -1)]


class ArrivalStore:
    def __init__(self, session: Session, keyspace: str, blobs):
        self.s, self.blobs = session, blobs
        p = session.prepare
        self._blob = p(f"INSERT INTO {keyspace}.blobs (content_sha256, size_bytes, mime, storage_uri, "
                       f"first_seen) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS")
        self._arrival = p(f"INSERT INTO {keyspace}.arrivals (arrival_id, content_sha256, size_bytes, mime, "
                          f"channel, original_filename, intake_meta, arrived_us, sim_run_id) "
                          f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        self._sighting = p(f"INSERT INTO {keyspace}.sightings_by_sha (content_sha256, month, seen_at, "
                           f"arrival_id, channel, original_filename) VALUES (?, ?, ?, ?, ?, ?)")
        self._sightings = p(f"SELECT seen_at, arrival_id, channel, original_filename "
                            f"FROM {keyspace}.sightings_by_sha "
                            f"WHERE content_sha256 = ? AND month = ? LIMIT ?")
        self._link = p(f"UPDATE {keyspace}.arrivals SET doc_id = ?, linked_us = ? WHERE arrival_id = ?")
        self._get = p(f"SELECT * FROM {keyspace}.arrivals WHERE arrival_id = ?")
        self._blob_get = p(f"SELECT content_sha256, size_bytes, mime, storage_uri, first_seen, verified_at "
                           f"FROM {keyspace}.blobs WHERE content_sha256 = ?")

    async def _sightings_from(self, content_sha256: bytes, base_us: int, months: int,
                              limit: int) -> list[dict]:
        rows: list[dict] = []
        for month in _months_back(base_us, months):   # one partition per query, merged here (Q4)
            rows += await run(self.s, self._sightings, (content_sha256, month, limit))
        rows.sort(key=lambda r: r["seen_at"].time, reverse=True)
        return rows[:limit]

    async def sightings(self, content_sha256: bytes, months: int = 2, limit: int = 50) -> list[dict]:
        return await self._sightings_from(content_sha256, time.time_ns() // 1000, months, limit)

    async def record(self, *, content: bytes | None, content_sha256: bytes, size_bytes: int, mime: str | None,
                     channel: str, original_filename: str | None, intake_meta: dict, arrived_us: int,
                     sim_run_id: str | None = None) -> Arrival:
        if content is not None:
            info = self.blobs.put(content, content_sha256)
            await run(self.s, self._blob, (content_sha256, size_bytes, mime, info.storage_uri,
                                           _dt(arrived_us)))
        earlier = await self._sightings_from(content_sha256, arrived_us, 2, 1)
        duplicate_of = earlier[0]["arrival_id"] if earlier else None
        meta = {**intake_meta, "duplicate_of": duplicate_of} if duplicate_of else dict(intake_meta)
        arrival_id = new_arrival_id()
        await run(self.s, self._arrival, (arrival_id, content_sha256, size_bytes, mime, channel,
                                          original_filename, canonical_bytes(meta), arrived_us, sim_run_id))
        seen_at = uuid_from_time(arrived_us / 1_000_000)
        month = _months_back(arrived_us, 1)[0]
        await run(self.s, self._sighting, (content_sha256, month, seen_at, arrival_id, channel,
                                           original_filename))
        return Arrival(arrival_id=arrival_id, duplicate_of=duplicate_of)

    async def link(self, arrival_id: str, doc_id: str, linked_us: int) -> None:
        await run(self.s, self._link, (doc_id, linked_us, arrival_id))

    async def get(self, arrival_id: str) -> dict | None:
        rows = await run(self.s, self._get, (arrival_id,))
        return rows[0] if rows else None

    async def blob_row(self, content_sha256: bytes) -> dict | None:
        rows = await run(self.s, self._blob_get, (content_sha256,))
        return rows[0] if rows else None
