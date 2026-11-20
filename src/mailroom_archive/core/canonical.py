"""Canonical JSON: the exact bytes every hash in the archive is computed over."""

import json
from typing import Any


def canonical_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")
