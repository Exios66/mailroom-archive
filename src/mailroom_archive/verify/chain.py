"""Walk a document's chain from seq 1 and recompute every hash (spec §7.5)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from ..core.hashing import event_hash, sha256
from ..core.models import StoredEvent

CHECKS = ("seq_contiguous", "prev_link", "payload_hash", "event_hash", "payload_available")


class Check(BaseModel):
    name: str
    ok: bool
    detail: str | None = None
    seq: int | None = None


class VerifyReport(BaseModel):
    doc_id: str
    ok: bool
    events: int
    checks: list[Check]


def _payload(ev: StoredEvent, blobs: Any | None) -> bytes | None:
    if not ev.payload_ref:
        return ev.payload
    digest = bytes.fromhex(ev.payload_ref.split("/", 1)[1])
    if blobs is None or not blobs.exists(digest):
        return None
    return blobs.get(digest)


def verify_events(doc_id: str, events: list[StoredEvent], blobs: Any | None) -> VerifyReport:
    if not events:
        return VerifyReport(doc_id=doc_id, ok=False, events=0,
                            checks=[Check(name="seq_contiguous", ok=False, detail="no events")])
    failed: list[Check] = []
    prev_seq, prev_hash = 0, b""
    for ev in sorted(events, key=lambda e: e.seq):
        if ev.seq != prev_seq + 1:
            failed.append(Check(name="seq_contiguous", ok=False, seq=ev.seq,
                                detail=f"expected seq {prev_seq + 1}"))
        if ev.prev_hash != prev_hash:
            failed.append(Check(name="prev_link", ok=False, seq=ev.seq,
                                detail="prev_hash does not match the previous event_hash"))
        body = _payload(ev, blobs)
        if body is None:
            failed.append(Check(name="payload_available", ok=False, seq=ev.seq,
                                detail=f"spilled payload {ev.payload_ref} not in the blob store"))
        elif sha256(body) != ev.payload_sha256:
            failed.append(Check(name="payload_hash", ok=False, seq=ev.seq))
        recomputed = event_hash(doc_id=doc_id, seq=ev.seq, kind=str(ev.kind), occurred_us=ev.occurred_us,
                                actor=ev.actor, source=ev.source, source_ref=ev.source_ref,
                                payload_sha256=ev.payload_sha256, prev_hash=ev.prev_hash)
        if recomputed != ev.event_hash:
            failed.append(Check(name="event_hash", ok=False, seq=ev.seq))
        prev_seq, prev_hash = ev.seq, ev.event_hash
    names_failed = {c.name for c in failed}
    passed = [Check(name=n, ok=True) for n in CHECKS if n not in names_failed]
    return VerifyReport(doc_id=doc_id, ok=not failed, events=len(events), checks=failed + passed)
