import asyncio

import pytest

from mailroom_archive.core.models import EventIn
from mailroom_archive.writer.pool import ShardPool

pytestmark = pytest.mark.integration


def ev(doc_id, kind, ref):
    return EventIn(doc_id=doc_id, kind=kind, actor="a", source="it", source_ref=ref,
                   occurred_us=1_763_650_000_000_000, payload={})


@pytest.fixture
async def pool(store):
    p = ShardPool(store, None, shards=8)
    await p.start()
    yield p
    await p.stop()


async def test_batch_for_one_doc_chains_in_order(pool, store):
    res = await pool.submit_batch([ev("pc-1", "arrived", f"pc1:{i}") for i in range(5)])
    assert [r.seq for r in res] == [1, 2, 3, 4, 5]
    tl = await store.timeline("pc-1")
    assert tl[0].prev_hash == b"" and all(tl[i].prev_hash == tl[i - 1].event_hash for i in range(1, 5))


async def test_concurrent_submits_same_doc_form_one_chain(pool, store):
    await asyncio.gather(*[pool.submit_batch([ev("pc-2", "annotation", f"pc2:{i}")]) for i in range(50)])
    assert [e.seq for e in await store.timeline("pc-2")] == list(range(1, 51))
