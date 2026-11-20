import asyncio

import pytest
from fakes import FakeStore, SlowFakeStore

from mailroom_archive.core.models import EventIn
from mailroom_archive.store.session import StoreUnavailable
from mailroom_archive.writer.pool import Backpressure, ShardPool

NO_WAIT = (0, 0, 0)


def ev(doc_id, kind, payload=None, ref="r", **kw):
    return EventIn(doc_id=doc_id, kind=kind, actor="a", source="s", source_ref=ref,
                   occurred_us=1_763_650_000_000_000, payload=payload or {}, **kw)


@pytest.fixture
def fake_store():
    return FakeStore()


@pytest.fixture
async def pool(fake_store):
    p = ShardPool(fake_store, None, shards=8, retry_delays=NO_WAIT)
    await p.start()
    yield p
    await p.stop()


async def test_batch_for_one_doc_chains_in_order(pool, fake_store):
    res = await pool.submit_batch([ev("d", "arrived", ref=f"r{i}") for i in range(5)])
    assert [r.seq for r in res] == [1, 2, 3, 4, 5]
    tl = fake_store.timeline_sync("d")
    assert all(tl[i].prev_hash == tl[i - 1].event_hash for i in range(1, 5)) and tl[0].prev_hash == b""


async def test_results_keep_input_order_across_shards(pool):
    res = await pool.submit_batch([ev(f"d{i}", "arrived", ref=f"r{i}") for i in range(20)])
    assert [r.doc_id for r in res] == [f"d{i}" for i in range(20)]
    assert all(r.status == "written" and r.seq == 1 for r in res)


async def test_concurrent_submits_same_doc_form_one_chain(pool, fake_store):
    await asyncio.gather(*[pool.submit_batch([ev("d", "annotation", ref=f"r{i}")]) for i in range(50)])
    tl = fake_store.timeline_sync("d")
    assert [e.seq for e in tl] == list(range(1, 51))
    assert all(tl[i].prev_hash == tl[i - 1].event_hash for i in range(1, 50))


async def test_duplicate_returns_original(pool):
    a = (await pool.submit_batch([ev("d", "arrived", ref="same")]))[0]
    b = (await pool.submit_batch([ev("d", "arrived", ref="same")]))[0]
    assert b.status == "duplicate" and (b.seq, b.event_hash) == (a.seq, a.event_hash)


async def test_redelivery_after_crash_reissues_identical_write(pool, fake_store):
    fake_store.fail_next("write_event", times=4)   # idem row written, every Q8 attempt lost
    with pytest.raises(StoreUnavailable):
        await pool.submit_batch([ev("d", "arrived", ref="x")])
    assert fake_store.timeline_sync("d") == []
    r = (await pool.submit_batch([ev("d", "arrived", ref="x")]))[0]
    assert r.status == "duplicate" and r.seq == 1 and len(fake_store.timeline_sync("d")) == 1
    assert fake_store.timeline_sync("d")[0].event_hash.hex() == r.event_hash


async def test_late_redelivery_does_not_rewind_head(pool, fake_store):
    await pool.submit_batch([ev("d", "arrived", ref="a"), ev("d", "annotation", ref="b")])
    r = (await pool.submit_batch([ev("d", "arrived", ref="a")]))[0]
    assert r.status == "duplicate" and r.seq == 1
    assert (await fake_store.get_head("d")).seq == 2


async def test_transient_failure_is_retried(pool, fake_store):
    fake_store.fail_next("write_event", times=2)
    r = (await pool.submit_batch([ev("d", "arrived", ref="x")]))[0]
    assert r.status == "written" and len(fake_store.timeline_sync("d")) == 1


async def test_conflict_written_after_triggering_event(pool, fake_store):
    await pool.submit_batch([ev("d", "correction", {"origin": "archive", "field_path": "doc_type",
                                                    "new_value": "x", "reason": "r", "reviewer": "jb"},
                                ref="c1")])
    await pool.submit_batch([ev("d", "stage_changed", {"to_stage": "archived", "doc_type": "y"}, ref="s1")])
    assert [e.kind for e in fake_store.timeline_sync("d")] == ["correction", "stage_changed", "conflict"]


async def test_matter_indexed_once(pool, fake_store):
    await pool.submit_batch([ev("d", "arrived", ref=f"r{i}", matter_id="M1") for i in range(3)])
    assert [m["doc_id"] for m in fake_store.matters["M1"]] == ["d"]


async def test_large_payload_spills_without_blobs_stays_inline(pool, fake_store):
    await pool.submit_batch([ev("d", "annotation", {"note": "x" * 20000}, ref="big")])
    assert fake_store.timeline_sync("d")[0].payload_ref is None


async def test_backpressure_when_queue_full():
    pool = ShardPool(SlowFakeStore(), None, shards=1, queue_size=2, retry_delays=NO_WAIT)
    await pool.start()
    try:
        with pytest.raises(Backpressure) as exc:
            await asyncio.gather(*[pool.submit_batch([ev(f"d{i}", "arrived", ref=f"r{i}")])
                                   for i in range(10)])
        assert exc.value.retry_after > 0
    finally:
        await pool.stop()
