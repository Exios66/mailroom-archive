"""Shard writers (spec §7.2): one asyncio task per shard, so each document has a single writer.

Per event: idempotency check, head (cache or Q1), sequence and hash, idempotency row, Q8,
feed row, matter index, cache. Conflicts the event causes are written next, same shard.
When Cassandra fails past the retries, the event goes to the spool unsequenced, and so does
every later event for that shard until the drainer has replayed the spool (spec §7.4).
"""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import structlog

from ..core.canonical import canonical_bytes
from ..core.hashing import event_hash, sha256
from ..core.ids import new_event_id
from ..core.models import EventIn, StoredEvent, WriteResult
from ..core.state import Head, plan_state
from ..store.session import StoreUnavailable
from ..store.timeline import IdemRecord
from .shards import feed_shard, shard_of
from .spill import spill_payload

log = structlog.get_logger()

RETRY_DELAYS = (0.1, 0.4, 1.6)
CACHE_ENTRIES = 10_000


class Backpressure(Exception):
    def __init__(self, retry_after: float = 1.0):
        super().__init__(f"shard queue full; retry after {retry_after}s")
        self.retry_after = retry_after


class _HeadCache:
    def __init__(self, size: int = CACHE_ENTRIES):
        self.size, self.d = size, OrderedDict[str, Head]()

    def get(self, doc_id: str) -> Head | None:
        if (h := self.d.get(doc_id)) is not None:
            self.d.move_to_end(doc_id)
        return h

    def put(self, doc_id: str, head: Head) -> None:
        self.d[doc_id] = head
        self.d.move_to_end(doc_id)
        if len(self.d) > self.size:
            self.d.popitem(last=False)

    def drop(self, doc_id: str) -> None:
        self.d.pop(doc_id, None)


class ShardPool:
    def __init__(
        self,
        store: Any,
        spool: Any | None,
        *,
        shards: int = 64,
        queue_size: int = 2000,
        blobs: Any | None = None,
        retry_delays: Sequence[float] = RETRY_DELAYS,
        health: Callable[[], Awaitable[bool]] | None = None,
        drain_interval: float = 1.0,
    ):
        self.store, self.spool, self.blobs = store, spool, blobs
        self.health, self.drain_interval = health, drain_interval
        self.locks = [asyncio.Lock() for _ in range(shards)]
        self.shards, self.retry_delays = shards, tuple(retry_delays)
        self.queues = [asyncio.Queue[tuple[EventIn, asyncio.Future]](queue_size) for _ in range(shards)]
        self.caches = [_HeadCache() for _ in range(shards)]
        self.tasks: list[asyncio.Task] = []
        self.retries = 0  # exported as archive_write_retries_total
        self.conflicts = 0  # exported as archive_conflicts_total

    async def start(self) -> None:
        self.tasks = [asyncio.create_task(self._worker(i), name=f"shard-{i}") for i in range(self.shards)]
        if self.spool is not None:
            self.tasks.append(asyncio.create_task(self._drainer(), name="spool-drainer"))

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []

    def shard(self, doc_id: str) -> int:
        return shard_of(doc_id, self.shards)

    async def wait_drained(self, timeout: float) -> None:
        async with asyncio.timeout(timeout):
            while self.spool is not None and self.spool.stats().events:
                await asyncio.sleep(0.02)

    async def _drainer(self) -> None:
        while True:
            await asyncio.sleep(self.drain_interval)
            if self.health is not None and not await self.health():
                continue
            for shard in sorted(self.spool.pending_shards()):
                async with self.locks[shard]:
                    if not await self._drain_shard(shard):
                        break

    async def _drain_shard(self, shard: int) -> bool:
        """Replay one shard's spool oldest first; False if Cassandra failed again."""
        while batch := self.spool.peek(shard):
            for spool_id, ev in batch:
                try:
                    await self._write_direct(shard, ev)
                except StoreUnavailable:
                    return False
                self.spool.ack([spool_id])
        return True

    async def submit_batch(self, events: list[EventIn]) -> list[WriteResult]:
        targets = [shard_of(e.doc_id, self.shards) for e in events]
        need: dict[int, int] = {}
        for s in targets:
            need[s] = need.get(s, 0) + 1
        for s, n in need.items():
            q = self.queues[s]
            if q.maxsize - q.qsize() < n:
                raise Backpressure(retry_after=1.0)
        loop = asyncio.get_running_loop()
        futures = []
        for e, s in zip(events, targets, strict=True):
            fut = loop.create_future()
            self.queues[s].put_nowait((e, fut))
            futures.append(fut)
        return list(await asyncio.gather(*futures))

    async def _worker(self, shard: int) -> None:
        q = self.queues[shard]
        while True:
            ev, fut = await q.get()
            try:
                async with self.locks[shard]:
                    result = await self._process(shard, ev)
            except Exception as e:  # delivered to the caller, the shard keeps running
                if not fut.done():
                    fut.set_exception(e)
            else:
                if not fut.done():
                    fut.set_result(result)
            finally:
                q.task_done()

    async def _retrying(self, shard: int, doc_id: str, step):
        for delay in (*self.retry_delays, None):
            try:
                return await step()
            except StoreUnavailable:
                self.caches[shard].drop(doc_id)
                if delay is None:
                    raise
                self.retries += 1
                await asyncio.sleep(delay)

    async def _head(self, shard: int, doc_id: str) -> Head:
        cache = self.caches[shard]
        if (h := cache.get(doc_id)) is None:
            h = await self.store.get_head(doc_id)
            cache.put(doc_id, h)
        return h

    async def _process(self, shard: int, ev: EventIn) -> WriteResult:
        if self.spool is not None and self.spool.has_pending(shard):
            return self._to_spool(shard, ev)
        try:
            return await self._write_direct(shard, ev)
        except StoreUnavailable:
            if self.spool is None:
                raise
            return self._to_spool(shard, ev)

    def _to_spool(self, shard: int, ev: EventIn) -> WriteResult:
        status = "spooled" if self.spool.append(shard, ev) else "duplicate"
        log.warning("archive_spooled", doc_id=ev.doc_id, shard=shard, source_ref=ev.source_ref, status=status)
        return WriteResult(doc_id=ev.doc_id, status=status)

    async def _write_direct(self, shard: int, ev: EventIn) -> WriteResult:
        mine: dict[str, IdemRecord] = {}  # the record this delivery planned, kept across retries
        result, conflicts = await self._retrying(shard, ev.doc_id, lambda: self._write_one(shard, ev, mine))
        for c in conflicts:
            self.conflicts += 1
            log.info(
                "archive_conflict",
                doc_id=c.doc_id,
                shard=shard,
                source_ref=c.source_ref,
                field_path=c.payload["field_path"],
            )
            await self._process(shard, c)
        return result

    async def _write_one(
        self, shard: int, ev: EventIn, mine: dict[str, IdemRecord]
    ) -> tuple[WriteResult, list[EventIn]]:
        head = await self._head(shard, ev.doc_id)
        body = canonical_bytes(ev.payload)
        rec = await self.store.get_idem(ev.source, ev.source_ref)
        if rec is not None:
            own = rec == mine.get("rec")  # our own earlier attempt, not an earlier delivery
            conflicts: list[EventIn] = []
            if rec.seq == head.seq + 1:  # idempotency row landed, Q8 did not: finish it
                conflicts = await self._apply(shard, ev, head, body, rec, want_conflicts=True)
            return WriteResult(
                doc_id=ev.doc_id,
                status="written" if own else "duplicate",
                seq=rec.seq,
                event_hash=rec.event_hash.hex(),
            ), conflicts if own else []
        seq = head.seq + 1
        psha = sha256(body)
        h = event_hash(
            doc_id=ev.doc_id,
            seq=seq,
            kind=str(ev.kind),
            occurred_us=ev.occurred_us,
            actor=ev.actor,
            source=ev.source,
            source_ref=ev.source_ref,
            payload_sha256=psha,
            prev_hash=head.hash,
        )
        rec = IdemRecord(
            doc_id=ev.doc_id,
            seq=seq,
            event_hash=h,
            event_id=new_event_id(),
            payload_sha256=psha,
            prev_hash=head.hash,
        )
        await self.store.put_idem(ev.source, ev.source_ref, rec)
        mine["rec"] = rec
        conflicts = await self._apply(shard, ev, head, body, rec, want_conflicts=True)
        return WriteResult(doc_id=ev.doc_id, status="written", seq=seq, event_hash=h.hex()), conflicts

    async def _apply(
        self, shard: int, ev: EventIn, head: Head, body: bytes, rec: IdemRecord, want_conflicts: bool = False
    ):
        upd, conflicts = plan_state(head, ev, rec.seq)
        payload, ref = spill_payload(body, self.blobs)
        stored = StoredEvent(
            doc_id=ev.doc_id,
            seq=rec.seq,
            event_id=rec.event_id,
            kind=ev.kind,
            actor=ev.actor,
            source=ev.source,
            source_ref=ev.source_ref,
            occurred_us=ev.occurred_us,
            payload=payload,
            payload_ref=ref,
            payload_sha256=rec.payload_sha256,
            prev_hash=rec.prev_hash,
            event_hash=rec.event_hash,
            corrects_seq=ev.payload.get("corrects_seq"),
        )
        await self.store.write_event(stored, upd)
        f = upd.fields

        def now(name: str):
            return f.get(name, getattr(head, name))

        await self.store.write_feed(
            stored,
            shard16=feed_shard(ev.doc_id),
            channel=now("channel"),
            doc_type=now("doc_type"),
            stage=now("stage"),
            sim_run_id=ev.sim_run_id,
        )
        if head.matter_id is None and f.get("matter_id"):
            await self.store.add_to_matter(
                f["matter_id"], ev.occurred_us, ev.doc_id, now("doc_type"), now("content_sha256")
            )
        new_head = head.replace(
            seq=rec.seq, hash=rec.event_hash, **{k: v for k, v in f.items() if k in _HEAD_FIELDS}
        )
        if "extracted_data" in f:
            new_head = new_head.replace(extracted_data=json.loads(f["extracted_data"]))
        self.caches[shard].put(ev.doc_id, new_head)
        return conflicts if want_conflicts else new_head


_HEAD_FIELDS = frozenset(
    {
        "matter_id",
        "doc_type",
        "doc_subclass",
        "stage",
        "content_sha256",
        "channel",
        "original_filename",
        "overrides",
        "audit_entry_hash",
    }
)
