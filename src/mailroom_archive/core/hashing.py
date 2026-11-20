"""Event hashing (spec §6.1). The payload is hashed separately so it can spill to blobs."""

import hashlib
from typing import Final

from .canonical import canonical_bytes

HASH_VERSION: Final = "a1"


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def event_hash(*, doc_id: str, seq: int, kind: str, occurred_us: int, actor: str, source: str,
               source_ref: str, payload_sha256: bytes, prev_hash: bytes) -> bytes:
    return sha256(canonical_bytes({
        "hash_version": HASH_VERSION,
        "doc_id": doc_id,
        "seq": seq,
        "kind": kind,
        "occurred_us": occurred_us,
        "actor": actor,
        "source": source,
        "source_ref": source_ref,
        "payload_sha256": payload_sha256.hex(),
        "prev_hash": prev_hash.hex(),
    }))
