"""RF 3 at QUORUM: one node down changes nothing; two down spools, then drains (spec §6.5, §7.4).

Runs on the 3-node `small` profile only. It stops and starts this project's own containers
(`mailroom-archive-*`) and nothing else.
"""

import asyncio
import subprocess
import time

import httpx
import pytest

from mailroom_archive.api.app import Deps, create_app
from mailroom_archive.blobs import FsBlobStore
from mailroom_archive.settings import Settings
from mailroom_archive.spool import Spool
from mailroom_archive.store.arrivals import ArrivalStore
from mailroom_archive.store.session import quorum_probe
from mailroom_archive.writer.pool import ShardPool

pytestmark = pytest.mark.fault
NODE2, NODE3 = "mailroom-archive-cassandra-2-1", "mailroom-archive-cassandra-3-1"
H = {"authorization": "Bearer w"}


class Docker:
    def _run(self, *args: str) -> None:
        assert all(a.startswith("mailroom-archive-") for a in args[1:]), "only this project's containers"
        subprocess.run(["docker", *args], check=True, capture_output=True)

    def kill(self, name: str) -> None:
        self._run("kill", name)

    def start(self, name: str) -> None:
        self._run("start", name)


@pytest.fixture
def docker():
    return Docker()


@pytest.fixture
async def api(ks_session, ks, store, tmp_path):
    blobs = FsBlobStore(tmp_path / "blobs")
    pool = ShardPool(
        store, Spool(tmp_path / "spool.db"), shards=8, blobs=blobs, health=quorum_probe(ks_session, ks)
    )
    deps = Deps(
        store=store,
        pool=pool,
        arrivals=ArrivalStore(ks_session, ks, blobs),
        blobs=blobs,
        session=ks_session,
        ready=quorum_probe(ks_session, ks),
    )
    app = create_app(Settings(token="w", read_token="r", data_dir=tmp_path, keyspace=ks), deps=deps)
    await pool.start()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t", headers=H, timeout=120
    ) as c:
        yield c
    await pool.stop()


async def post_events(api, docs, *, per_doc, ref_prefix):
    batch = [
        {
            "doc_id": d,
            "kind": "annotation",
            "actor": "fault",
            "source": "fault",
            "source_ref": f"{ref_prefix}:{d}:{i}",
            "occurred_us": time.time_ns() // 1000,
            "payload": {"i": i},
        }
        for i in range(per_doc)
        for d in docs
    ]
    r = await api.post("/v1/events", json=batch)
    assert r.status_code == 200, r.text
    return r.json()


async def wait_until(call, cond, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond(await call()):
            return
        await asyncio.sleep(2)
    raise TimeoutError("condition not met")


async def wait_ready(api, want: bool, timeout=180):
    await wait_until(lambda: api.get("/readyz"), lambda r: (r.status_code == 200) == want, timeout)


async def test_writes_and_reads_survive_one_node_down(api, docker):
    docs = [f"nd-{i}" for i in range(30)]
    await post_events(api, docs, per_doc=5, ref_prefix="before")
    docker.kill(NODE2)
    await asyncio.sleep(15)  # let gossip mark it down
    try:
        res = await post_events(api, docs, per_doc=5, ref_prefix="during")
        assert {r["status"] for r in res} == {"written"}  # QUORUM still met, nothing spooled
        for d in docs:
            assert (await api.get(f"/v1/verify/doc/{d}")).json()["ok"]
            assert (await api.get(f"/v1/docs/{d}")).json()["head_seq"] == 10
    finally:
        docker.start(NODE2)
    await wait_ready(api, True)


async def test_two_nodes_down_spools_then_drains(api, docker):
    docker.kill(NODE2)
    docker.kill(NODE3)
    try:
        await wait_ready(api, False, timeout=60)
        res = await post_events(api, ["sp-1"], per_doc=5, ref_prefix="x")
        assert {r["status"] for r in res} == {"spooled"}
    finally:
        docker.start(NODE2)
        docker.start(NODE3)
    await wait_until(lambda: api.get("/v1/docs/sp-1"), lambda r: r.json().get("head_seq") == 5, timeout=240)
    rep = (await api.get("/v1/verify/doc/sp-1")).json()
    assert rep["ok"] and rep["events"] == 5
