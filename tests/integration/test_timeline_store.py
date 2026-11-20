import pytest
from factories import stored

from mailroom_archive.core.hashing import sha256
from mailroom_archive.core.state import Head, StateUpdate
from mailroom_archive.store.timeline import IdemRecord

pytestmark = pytest.mark.integration


async def test_write_then_head_and_timeline(store):
    ev1 = stored(doc_id="d1", seq=1, kind="arrived")
    await store.write_event(ev1, StateUpdate({"channel": "upload"}))
    head = await store.get_head("d1")
    assert head.seq == 1 and head.hash == ev1.event_hash and head.channel == "upload"
    assert [e.seq for e in await store.timeline("d1")] == [1]


async def test_missing_doc_head_is_empty(store):
    assert (await store.get_head("nope")) == Head.empty()


async def test_unicode_round_trip_verifies(store):
    ev = stored(doc_id="société-1", seq=1, kind="annotation", payload={"note": "Générale — §12 📎"})
    await store.write_event(ev, StateUpdate({}))
    back = (await store.timeline("société-1"))[0]
    assert back.payload == ev.payload and sha256(back.payload) == back.payload_sha256


async def test_occurred_us_keeps_microseconds(store):
    ev = stored(doc_id="d2", seq=1, kind="arrived", occurred_us=1763650000123457)
    await store.write_event(ev, StateUpdate({}))
    assert (await store.timeline("d2"))[0].occurred_us == 1763650000123457


async def test_state_fields_round_trip_into_head(store):
    ev = stored(doc_id="d3", seq=1, kind="correction")
    await store.write_event(ev, StateUpdate({"doc_type": "contract", "overrides": {"doc_type": 1},
                                             "extracted_data": b'{"a":1}'}))
    head = await store.get_head("d3")
    assert head.doc_type == "contract" and head.overrides == {"doc_type": 1}
    assert head.extracted_data == {"a": 1}


async def test_idempotency_round_trip(store):
    ev = stored(doc_id="d4", seq=1)
    rec = IdemRecord(doc_id="d4", seq=1, event_hash=ev.event_hash, event_id=ev.event_id,
                     payload_sha256=ev.payload_sha256, prev_hash=b"")
    assert await store.get_idem("s", "x:1") is None
    await store.put_idem("s", "x:1", rec)
    assert await store.get_idem("s", "x:1") == rec


async def test_matter_index(store):
    await store.add_to_matter("M1", 1_763_650_000_000_000, "d5", "contract", None)
    rows = await store.matter_docs("M1", 2025)
    assert [r["doc_id"] for r in rows] == ["d5"]


async def test_write_feed(store, ks_session, ks):
    ev = stored(doc_id="d6", seq=1)
    await store.write_feed(ev, shard16=3, channel="upload", doc_type=None, stage=None, sim_run_id=None)
    rows = list(ks_session.execute(f"SELECT doc_id, event_id FROM {ks}.events_by_time "
                                   "WHERE bucket = %s AND shard = 3", (store.bucket_of(ev.event_id),)))
    # the keyspace is shared across tests, so other documents may share this feed partition
    assert [r["event_id"] for r in rows if r["doc_id"] == "d6"] == [ev.event_id]
