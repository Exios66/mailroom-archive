import hashlib

import pytest

from mailroom_archive.blobs import FsBlobStore, HashMismatch


def test_put_get_and_mismatch(tmp_path):
    bs = FsBlobStore(tmp_path)
    data = b"claim form"
    info = bs.put(data, hashlib.sha256(data).digest())
    assert bs.get(info.sha256) == data and info.storage_uri.endswith(hashlib.sha256(data).hexdigest())
    assert info.size_bytes == len(data) and bs.exists(info.sha256)
    with pytest.raises(HashMismatch):
        bs.put(b"other", info.sha256)


def test_put_is_idempotent_and_sharded_path(tmp_path):
    bs = FsBlobStore(tmp_path)
    a, b = bs.put(b"same"), bs.put(b"same")
    h = a.sha256.hex()
    assert a == b and (tmp_path / h[:2] / h[2:4] / h).read_bytes() == b"same"
    assert not list(tmp_path.rglob("*.tmp"))


def test_missing_blob(tmp_path):
    bs = FsBlobStore(tmp_path)
    assert not bs.exists(b"\0" * 32)
    with pytest.raises(FileNotFoundError):
        bs.get(b"\0" * 32)
