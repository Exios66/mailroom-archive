import pytest

from mailroom_archive.blobs import FsBlobStore
from mailroom_archive.core.hashing import sha256
from mailroom_archive.core.models import EventIn
from mailroom_archive.writer.pool import ShardPool

pytestmark = pytest.mark.integration


@pytest.fixture
def blobs(tmp_path):
    return FsBlobStore(tmp_path)


@pytest.fixture
async def pool_cassandra(store, blobs):
    p = ShardPool(store, None, shards=4, blobs=blobs)
    await p.start()
    yield p
    await p.stop()


async def test_large_payload_spills_and_verifies(pool_cassandra, blobs):
    payload = {"text": "é" * 20000}
    ev = EventIn(doc_id="big", kind="annotation", actor="a", source="it", source_ref="big:b",
                 occurred_us=1_763_650_000_000_000, payload=payload)
    r = (await pool_cassandra.submit_batch([ev]))[0]
    stored = (await pool_cassandra.store.timeline("big"))[0]
    assert r.status == "written"
    assert stored.payload == b"" and stored.payload_ref.startswith("payload/")
    assert sha256(blobs.get(bytes.fromhex(stored.payload_ref.split("/")[1]))) == stored.payload_sha256
