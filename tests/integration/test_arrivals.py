import json
import secrets
import time
from datetime import UTC, datetime

import pytest

from mailroom_archive.blobs import FsBlobStore
from mailroom_archive.core.hashing import sha256
from mailroom_archive.store.arrivals import ArrivalStore

pytestmark = pytest.mark.integration


class Clock:
    """Hands out increasing arrival times in this month or an earlier one."""

    def __init__(self):
        self.base = time.time_ns() // 1000
        self.n = 0

    def set_month(self, offset: int) -> None:
        now = datetime.now(UTC)
        if offset == 0:
            self.base = time.time_ns() // 1000
        else:
            y, m = divmod(now.year * 12 + now.month - 1 + offset, 12)
            self.base = int(datetime(y, m + 1, 2, tzinfo=UTC).timestamp() * 1_000_000)

    def us(self) -> int:
        self.n += 1
        return self.base + self.n


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def arrivals(ks_session, ks, tmp_path):
    return ArrivalStore(ks_session, ks, FsBlobStore(tmp_path))


async def record(arrivals, content, arrived_us, channel="upload"):
    return await arrivals.record(
        content=content,
        content_sha256=sha256(content),
        size_bytes=len(content),
        mime="text/plain",
        channel=channel,
        original_filename="a.txt",
        intake_meta={"upload_id": "u1"},
        arrived_us=arrived_us,
    )


async def test_second_identical_arrival_is_duplicate(arrivals, clock):
    x = secrets.token_bytes(16)
    a = await record(arrivals, x, clock.us())
    b = await record(arrivals, x, clock.us(), channel="gmail")
    assert a.duplicate_of is None and b.duplicate_of == a.arrival_id
    stored = await arrivals.get(b.arrival_id)
    assert json.loads(stored["intake_meta"]) == {"upload_id": "u1", "duplicate_of": a.arrival_id}
    assert stored["doc_id"] is None and stored["channel"] == "gmail"


async def test_sightings_span_two_months(arrivals, clock):
    x = secrets.token_bytes(16)
    clock.set_month(-1)
    old = await record(arrivals, x, clock.us())
    clock.set_month(0)
    new = [await record(arrivals, x, clock.us()) for _ in range(3)]
    got = await arrivals.sightings(sha256(x), months=2, limit=2)
    assert [g["arrival_id"] for g in got] == [new[2].arrival_id, new[1].arrival_id]
    got_all = await arrivals.sightings(sha256(x), months=2, limit=10)
    assert got_all[-1]["arrival_id"] == old.arrival_id and len(got_all) == 4
    assert new[0].duplicate_of == old.arrival_id


async def test_link_sets_doc_id(arrivals, clock):
    a = await record(arrivals, secrets.token_bytes(16), clock.us())
    await arrivals.link(a.arrival_id, "doc-1", clock.us())
    assert (await arrivals.get(a.arrival_id))["doc_id"] == "doc-1"
    assert await arrivals.get("missing") is None


async def test_blob_row_and_bytes_written(arrivals, clock, ks_session, ks):
    x = secrets.token_bytes(16)
    await record(arrivals, x, clock.us())
    row = ks_session.execute(
        f"SELECT size_bytes, storage_uri FROM {ks}.blobs WHERE content_sha256 = %s", (sha256(x),)
    ).one()
    assert row["size_bytes"] == 16 and arrivals.blobs.get(sha256(x)) == x
