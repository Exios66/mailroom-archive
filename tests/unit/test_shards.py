import hashlib

from fakes import FakeBlobs

from mailroom_archive.writer.shards import feed_shard, shard_of
from mailroom_archive.writer.spill import spill_payload


def test_shard_of_stable_and_in_range():
    assert shard_of("fixture-doc-a") == shard_of("fixture-doc-a")
    assert all(0 <= shard_of(f"d{i}") < 64 for i in range(1000))
    assert len({shard_of(f"d{i}") for i in range(10000)}) == 64


def test_feed_shard_in_range():
    assert {feed_shard(f"d{i}") for i in range(5000)} == set(range(16))


def test_spill_threshold():
    small, big = b"x" * 16384, b"x" * 16385
    assert spill_payload(small, FakeBlobs()) == (small, None)
    blobs = FakeBlobs()
    body, ref = spill_payload(big, blobs)
    assert body == b"" and ref == "payload/" + hashlib.sha256(big).hexdigest()
    assert blobs.get(hashlib.sha256(big).digest()) == big
