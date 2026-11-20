"""In-memory stand-ins with the same async surface as the Cassandra stores."""

import asyncio
import hashlib
import json
from collections import defaultdict

from mailroom_archive.core.models import StoredEvent
from mailroom_archive.core.state import Head, StateUpdate
from mailroom_archive.store.session import StoreUnavailable
from mailroom_archive.store.timeline import IdemRecord, TimelineStore


class FakeStore:
    bucket_of = staticmethod(TimelineStore.bucket_of)

    def __init__(self):
        self.rows: dict[str, dict[int, StoredEvent]] = defaultdict(dict)
        self.statics: dict[str, dict] = defaultdict(dict)
        self.idem: dict[tuple[str, str], IdemRecord] = {}
        self.feed: list[tuple[int, StoredEvent]] = []
        self.matters: dict[str, list[dict]] = defaultdict(list)
        self._fail: dict[str, int] = {}
        self._writes_left: int | None = None
        self._down = False

    def fail_after_writes(self, n: int) -> None:
        """Let n more write_event calls succeed, then fail every call until recover()."""
        self._writes_left = n

    def recover(self) -> None:
        self._writes_left, self._down = None, False

    def fail_next(self, method: str, times: int = 1) -> None:
        self._fail[method] = times

    def _maybe_fail(self, method: str) -> None:
        if self._down:
            raise StoreUnavailable("store down")
        if self._fail.get(method, 0) > 0:
            self._fail[method] -= 1
            raise StoreUnavailable(f"injected failure in {method}")

    def timeline_sync(self, doc_id: str) -> list[StoredEvent]:
        return [self.rows[doc_id][s] for s in sorted(self.rows[doc_id])]

    async def get_head(self, doc_id: str) -> Head:
        self._maybe_fail("get_head")
        st = self.statics.get(doc_id)
        if not st or "head_seq" not in st:
            return Head.empty()
        ed = st.get("extracted_data")
        return Head(seq=st["head_seq"], hash=st["head_hash"], matter_id=st.get("matter_id"),
                    doc_type=st.get("doc_type"), doc_subclass=st.get("doc_subclass"), stage=st.get("stage"),
                    content_sha256=st.get("content_sha256"), channel=st.get("channel"),
                    original_filename=st.get("original_filename"),
                    extracted_data=json.loads(ed) if ed else None, overrides=dict(st.get("overrides") or {}),
                    audit_entry_hash=st.get("audit_entry_hash"))

    async def write_event(self, ev: StoredEvent, upd: StateUpdate) -> None:
        self._maybe_fail("write_event")
        if self._writes_left is not None:
            if self._writes_left == 0:
                self._down = True
                raise StoreUnavailable("store down")
            self._writes_left -= 1
        self.rows[ev.doc_id][ev.seq] = ev
        self.statics[ev.doc_id].update(upd.fields, head_seq=ev.seq, head_hash=ev.event_hash)

    async def timeline(self, doc_id: str, after_seq: int = 0, limit: int | None = None) -> list[StoredEvent]:
        evs = [e for e in self.timeline_sync(doc_id) if e.seq > after_seq]
        return evs[:limit] if limit is not None else evs

    async def get_idem(self, source: str, source_ref: str) -> IdemRecord | None:
        self._maybe_fail("get_idem")
        return self.idem.get((source, source_ref))

    async def put_idem(self, source: str, source_ref: str, rec: IdemRecord) -> None:
        self._maybe_fail("put_idem")
        self.idem[(source, source_ref)] = rec

    async def write_feed(self, ev: StoredEvent, *, shard16: int, channel, doc_type, stage,
                         sim_run_id) -> None:
        self._maybe_fail("write_feed")
        self.feed.append((shard16, ev))

    async def add_to_matter(self, matter_id, created_us, doc_id, doc_type, content_sha256) -> None:
        self.matters[matter_id].append({"doc_id": doc_id, "doc_type": doc_type})

    async def matter_docs(self, matter_id: str, year: int, limit: int = 500) -> list[dict]:
        return self.matters[matter_id][:limit]


class SlowFakeStore(FakeStore):
    async def write_event(self, ev, upd):
        await asyncio.sleep(0.05)
        await super().write_event(ev, upd)


class FakeBlobs:
    def __init__(self):
        self.data: dict[bytes, bytes] = {}

    def put(self, data: bytes, expected_sha256: bytes | None = None):
        self.data[hashlib.sha256(data).digest()] = data

    def get(self, sha256: bytes) -> bytes:
        return self.data[sha256]

    def exists(self, sha256: bytes) -> bool:
        return sha256 in self.data
