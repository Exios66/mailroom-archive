import asyncio

from fakes import FakeStore

from mailroom_archive.core.models import EventIn
from mailroom_archive.spool import Spool
from mailroom_archive.writer.pool import ShardPool

NO_WAIT = (0, 0, 0)


def ev(doc_id, kind="annotation", ref="r"):
    return EventIn(
        doc_id=doc_id,
        kind=kind,
        actor="a",
        source="s",
        source_ref=ref,
        occurred_us=1_763_650_000_000_000,
        payload={},
    )


def make_pool(tmp_path, store, healthy):
    return ShardPool(
        store,
        Spool(tmp_path / "s.db"),
        shards=4,
        retry_delays=NO_WAIT,
        health=lambda: asyncio.sleep(0, healthy["v"]),
        drain_interval=0.05,
    )


async def test_outage_mid_batch_spools_rest_and_drains_in_order(tmp_path):
    store, healthy = FakeStore(), {"v": True}
    pool = make_pool(tmp_path, store, healthy)
    await pool.start()
    try:
        store.fail_after_writes(3)
        healthy["v"] = False
        res = await pool.submit_batch([ev("d", ref=f"r{i}") for i in range(10)])
        assert [r.status for r in res] == ["written"] * 3 + ["spooled"] * 7
        assert all(r.seq is None and r.event_hash is None for r in res[3:])
        store.recover()
        healthy["v"] = True
        await pool.wait_drained(timeout=10)
        assert [e.source_ref for e in store.timeline_sync("d")] == [f"r{i}" for i in range(10)]
        assert [e.seq for e in store.timeline_sync("d")] == list(range(1, 11))
    finally:
        await pool.stop()


async def test_new_events_follow_spooled_ones(tmp_path):
    store, healthy = FakeStore(), {"v": False}
    pool = make_pool(tmp_path, store, healthy)
    await pool.start()
    try:
        store.fail_after_writes(3)
        await pool.submit_batch([ev("d", ref=f"r{i}") for i in range(10)])
        store.recover()  # store works again, drainer not yet released
        late = (await pool.submit_batch([ev("d", ref="late")]))[0]
        assert late.status == "spooled"  # shard still has spooled events
        healthy["v"] = True
        await pool.wait_drained(timeout=10)
        tl = store.timeline_sync("d")
        assert tl[-1].source_ref == "late" and tl[-1].seq == 11
    finally:
        await pool.stop()


async def test_redelivery_of_spooled_event_is_duplicate_without_store(tmp_path):
    store, healthy = FakeStore(), {"v": False}
    pool = make_pool(tmp_path, store, healthy)
    await pool.start()
    try:
        store.fail_after_writes(0)
        assert (await pool.submit_batch([ev("d", ref="x")]))[0].status == "spooled"
        again = (await pool.submit_batch([ev("d", ref="x")]))[0]
        assert again.status == "duplicate" and again.seq is None
    finally:
        await pool.stop()


async def test_other_shards_keep_writing_directly(tmp_path):
    store, healthy = FakeStore(), {"v": False}
    pool = make_pool(tmp_path, store, healthy)
    await pool.start()
    try:
        store.fail_after_writes(0)
        await pool.submit_batch([ev("d", ref="x")])
        store.recover()
        other = next(f"o{i}" for i in range(100) if pool.shard(f"o{i}") != pool.shard("d"))
        assert (await pool.submit_batch([ev(other, ref="y")]))[0].status == "written"
    finally:
        await pool.stop()
