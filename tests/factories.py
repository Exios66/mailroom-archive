"""Builders for test data with real hashes."""

from mailroom_archive.core.canonical import canonical_bytes
from mailroom_archive.core.hashing import event_hash, sha256
from mailroom_archive.core.ids import new_event_id
from mailroom_archive.core.models import StoredEvent


def stored(
    *,
    doc_id="d",
    seq=1,
    kind="annotation",
    payload=None,
    occurred_us=1_763_650_000_000_000,
    prev_hash=b"",
    actor="a",
    source="s",
    source_ref=None,
) -> StoredEvent:
    body = canonical_bytes(payload if payload is not None else {})
    psha = sha256(body)
    ref = source_ref or f"t:{doc_id}:{seq}"
    h = event_hash(
        doc_id=doc_id,
        seq=seq,
        kind=kind,
        occurred_us=occurred_us,
        actor=actor,
        source=source,
        source_ref=ref,
        payload_sha256=psha,
        prev_hash=prev_hash,
    )
    return StoredEvent(
        doc_id=doc_id,
        seq=seq,
        event_id=new_event_id(),
        kind=kind,
        actor=actor,
        source=source,
        source_ref=ref,
        occurred_us=occurred_us,
        payload=body,
        payload_sha256=psha,
        prev_hash=prev_hash,
        event_hash=h,
    )
