"""At-least-once producer: acknowledge every derived event before checkpointing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx

from ..core.canonical import canonical_bytes
from ..core.models import EventIn, WriteResult
from .events import row_events
from .sqlite import SQLiteSource, overlap_start

MAX_EVENTS = 1000
MAX_BYTES = 4 * 1024 * 1024


class ArchiveClient:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def submit(self, events: list[EventIn]) -> None:
        batch: list[EventIn] = []
        size = 2  # JSON array brackets
        for ev in events:
            n = len(canonical_bytes(ev.model_dump(mode="json")))
            if n + 2 > MAX_BYTES:
                raise ValueError("one follower event exceeds the archive's 4 MiB request limit")
            if batch and (len(batch) == MAX_EVENTS or size + 1 + n > MAX_BYTES):
                await self._send(batch)
                batch, size = [], 2
            size += n + bool(batch)
            batch.append(ev)
        if batch:
            await self._send(batch)

    async def _send(self, events: list[EventIn]) -> None:
        body = canonical_bytes([e.model_dump(mode="json") for e in events])
        response = await self.client.post(
            "/v1/events", content=body, headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        results = [WriteResult.model_validate(r) for r in response.json()]
        if len(results) != len(events) or any(
            result.doc_id != ev.doc_id for result, ev in zip(results, events, strict=True)
        ):
            raise ValueError("archive acknowledgement does not match the submitted batch")
        # written, duplicate and spooled all acknowledge durable intake (§7.4).


@dataclass
class PollResult:
    rows: int = 0
    events: int = 0


class Follower:
    def __init__(
        self,
        source: SQLiteSource,
        archive: Any,
        cursors: Any,
        *,
        name: str,
        page_size: int = 500,
        overlap_seconds: int = 600,
    ):
        if not name.strip() or not 1 <= page_size <= 500 or overlap_seconds < 0:
            raise ValueError("name required, page_size must be 1..500, overlap_seconds must be nonnegative")
        self.source, self.archive, self.cursors = source, archive, cursors
        self.name, self.page_size, self.overlap_seconds = name, page_size, overlap_seconds

    async def poll_once(self) -> PollResult:
        result = PollResult()
        # Bound this poll before accepting any new upstream work. Snapshot only
        # while reading SQL; never hold the pipeline's read locks across HTTP/CQL.
        with self.source.snapshot() as snapshot:
            streams = [(stream, snapshot.end_position(stream)) for stream in snapshot.streams()]
        if not streams:
            raise ValueError("source has neither a mailroom audit log nor a document catalog")
        for stream, end in streams:
            if end is None:
                continue
            key = f"follower:{self.name}:{stream.name}"
            saved = await self.cursors.get(key)
            position = json.loads(saved) if saved is not None else None
            if isinstance(position, list):
                position = tuple(position)
            high = position
            after = overlap_start(position, self.overlap_seconds)
            while True:
                with self.source.snapshot() as snapshot:
                    page = next(
                        snapshot.pages(stream, after, limit=self.page_size, overlap_seconds=0, through=end),
                        [],
                    )
                    pending = [
                        (pos, list(row_events(snapshot, stream, row, f"follower:{self.name}")))
                        for pos, row in page
                    ]
                if not pending:
                    break
                for pos, events in pending:
                    await self.archive.submit(events)
                    # Save only after ALL children are acknowledged. If either side
                    # fails, replay has identical source refs and payloads.
                    high = pos if high is None else max(high, pos)
                    await self.cursors.put(key, json.dumps(high))
                    result.rows += 1
                    result.events += len(events)
                after = page[-1][0]
        return result
