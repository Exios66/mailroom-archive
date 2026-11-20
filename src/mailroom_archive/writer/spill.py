"""Payloads over 16 KiB move to the blob store; the chain hashes the full bytes either way."""

from __future__ import annotations

import hashlib
from typing import Any

SPILL_BYTES = 16 * 1024


def spill_payload(payload_bytes: bytes, blobs: Any | None) -> tuple[bytes, str | None]:
    if blobs is None or len(payload_bytes) <= SPILL_BYTES:
        return payload_bytes, None
    digest = hashlib.sha256(payload_bytes).digest()
    blobs.put(payload_bytes, expected_sha256=digest)
    return b"", "payload/" + digest.hex()
