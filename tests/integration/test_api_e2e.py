import hashlib
import json
import secrets

import httpx
import pytest

from mailroom_archive.api.app import Deps, create_app
from mailroom_archive.blobs import FsBlobStore
from mailroom_archive.settings import Settings
from mailroom_archive.spool import Spool
from mailroom_archive.store.arrivals import ArrivalStore
from mailroom_archive.writer.pool import ShardPool

pytestmark = pytest.mark.integration
W = {"authorization": "Bearer w"}
R = {"authorization": "Bearer r"}


@pytest.fixture
async def api(ks_session, ks, store, tmp_path):
    blobs = FsBlobStore(tmp_path / "blobs")
    pool = ShardPool(store, Spool(tmp_path / "spool.db"), shards=8, blobs=blobs)
    deps = Deps(
        store=store, pool=pool, arrivals=ArrivalStore(ks_session, ks, blobs), blobs=blobs, session=ks_session
    )
    app = create_app(Settings(token="w", read_token="r", data_dir=tmp_path, keyspace=ks), deps=deps)
    await pool.start()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c
    await pool.stop()


def evj(doc_id, i, kind="annotation", payload=None):
    return {
        "doc_id": doc_id,
        "kind": kind,
        "actor": "pipeline",
        "source": "e2e",
        "source_ref": f"e2e:{doc_id}:{i}",
        "occurred_us": 1_763_650_000_000_000 + i,
        "payload": payload or {"i": i},
    }


async def test_end_to_end(api):
    docs = [f"e2e-{n}" for n in range(20)]
    batch = [evj(d, i) for i in range(10) for d in docs]
    r = await api.post("/v1/events", json=batch, headers=W)
    assert r.status_code == 200 and all(x["status"] == "written" for x in r.json())

    data = secrets.token_bytes(64)
    sha = hashlib.sha256(data).hexdigest()
    first = await api.post(
        "/v1/arrivals",
        headers=W,
        files={"file": ("claim.pdf", data, "application/pdf")},
        data={"meta": json.dumps({"channel": "upload"})},
    )
    assert first.status_code == 200 and first.json()["duplicate_of"] is None
    second = await api.post(
        "/v1/arrivals", headers=W, json={"channel": "gmail", "content_sha256": sha, "size_bytes": 64}
    )
    assert second.json()["duplicate_of"] == first.json()["arrival_id"]

    d0 = docs[0]
    await api.post(
        f"/v1/docs/{d0}/corrections",
        headers=W,
        json={"field_path": "doc_type", "new_value": "policy", "reason": "misfiled", "reviewer": "jb"},
    )
    await api.post(
        "/v1/events",
        headers=W,
        json=[evj(d0, 99, "stage_changed", {"to_stage": "archived", "doc_type": "invoice"})],
    )
    state = (await api.get(f"/v1/docs/{d0}", headers=R)).json()
    assert state["doc_type"] == {"value": "policy", "overridden_by": 11, "pipeline_value": "invoice"}
    assert state["head_seq"] == 13 and state["stage"] == "archived"

    for d in docs:
        rep = (await api.get(f"/v1/verify/doc/{d}", headers=R)).json()
        assert rep["ok"], (d, rep)

    content = (await api.get(f"/v1/content/{sha}", headers=R)).json()
    assert len(content["sightings"]) == 2 and content["blob"]["size_bytes"] == 64
    assert (await api.get("/readyz", headers=R)).status_code == 200
    assert "archive_cassandra_hosts_up 1.0" in (await api.get("/metrics", headers=R)).text
