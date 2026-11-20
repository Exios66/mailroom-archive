"""Content-addressed blob store. Bytes never go into Cassandra (spec §6.4 `blobs`)."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class HashMismatch(ValueError):
    pass


@dataclass(frozen=True)
class BlobInfo:
    sha256: bytes
    size_bytes: int
    storage_uri: str


class BlobStore(Protocol):
    def put(self, data: bytes, expected_sha256: bytes | None = None) -> BlobInfo: ...
    def get(self, sha256: bytes) -> bytes: ...
    def exists(self, sha256: bytes) -> bool: ...


class FsBlobStore:
    """Files at root/ab/cd/<hex>, written to a temp file and renamed so readers never see a partial blob."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, sha256: bytes) -> Path:
        h = sha256.hex()
        return self.root / h[:2] / h[2:4] / h

    def put(self, data: bytes, expected_sha256: bytes | None = None) -> BlobInfo:
        digest = hashlib.sha256(data).digest()
        if expected_sha256 is not None and expected_sha256 != digest:
            raise HashMismatch(f"expected {expected_sha256.hex()}, got {digest.hex()}")
        path = self._path(digest)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, path)
            except BaseException:
                Path(tmp).unlink(missing_ok=True)
                raise
        return BlobInfo(sha256=digest, size_bytes=len(data), storage_uri=path.resolve().as_uri())

    def get(self, sha256: bytes) -> bytes:
        return self._path(sha256).read_bytes()

    def exists(self, sha256: bytes) -> bool:
        return self._path(sha256).is_file()
