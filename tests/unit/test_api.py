import hashlib
import json

import pytest
from fakes import FakeStore
from fastapi.testclient import TestClient

from mailroom_archive.api.app import Deps, create_app
from mailroom_archive.blobs import FsBlobStore
from mailroom_archive.settings import Settings
from mailroom_archive.writer.pool import Backpressure, ShardPool

TOKENS = {"write": "w-token", "read": "r-token"}


def auth(kind):
    return {"authorization": f"Bearer {TOKENS[kind]}"}


def evj(doc_id, kind="annotation", payload=None, ref="r"):
    return {
        "doc_id": doc_id,
        "kind": kind,
        "actor": "a",
        "source": "s",
        "source_ref": ref,
        "occurred_us": 1_763_650_000_000_000,
        "payload": payload or {},
    }


def make_client(tmp_path, pool):
    settings = Settings(
        token=TOKENS["write"], read_token=TOKENS["read"], data_dir=tmp_path, cassandra_hosts=[]
    )
    deps = Deps(store=pool.store, pool=pool, arrivals=None, blobs=FsBlobStore(tmp_path / "blobs"))
    return TestClient(create_app(settings, deps=deps))


@pytest.fixture
def client(tmp_path):
    with make_client(tmp_path, ShardPool(FakeStore(), None, shards=4, retry_delays=(0, 0, 0))) as c:
        yield c


class _FullPool:
    store = FakeStore()

    async def start(self):
        pass

    async def stop(self):
        pass

    async def submit_batch(self, events):
        raise Backpressure(retry_after=2.5)


@pytest.fixture
def client_backpressure(tmp_path):
    with make_client(tmp_path, _FullPool()) as c:
        yield c


def test_requires_token(client):
    assert client.get("/v1/docs/d").status_code == 401
    assert client.get("/v1/docs/d", headers={"authorization": "Bearer nope"}).status_code == 401
    assert client.get("/healthz").status_code == 200


def test_read_token_cannot_write(client):
    r = client.post("/v1/events", json=[evj("d")], headers=auth("read"))
    assert r.status_code == 403


def test_post_events_then_read(client):
    r = client.post("/v1/events", json=[evj("d", ref="a"), evj("d", ref="b")], headers=auth("write"))
    assert [x["seq"] for x in r.json()] == [1, 2]
    assert client.get("/v1/docs/d", headers=auth("read")).json()["head_seq"] == 2
    tl = client.get("/v1/docs/d/timeline?after_seq=1", headers=auth("read")).json()
    assert [e["seq"] for e in tl] == [2] and tl[0]["payload"] == {}


def test_unknown_doc_is_404(client):
    assert client.get("/v1/docs/none", headers=auth("read")).status_code == 404


def test_ndjson_accepted(client):
    body = "\n".join(json.dumps(evj("n", ref=str(i))) for i in range(3))
    r = client.post(
        "/v1/events", content=body, headers={**auth("write"), "content-type": "application/x-ndjson"}
    )
    assert r.status_code == 200 and len(r.json()) == 3


def test_limits_and_forbidden_kind(client):
    many = [evj("d", ref=str(i)) for i in range(1001)]
    assert client.post("/v1/events", json=many, headers=auth("write")).status_code == 413
    assert (
        client.post("/v1/events", json=[evj("d", kind="conflict")], headers=auth("write")).status_code == 422
    )
    assert client.post("/v1/events", json=[evj("d", kind="nope")], headers=auth("write")).status_code == 422
    huge = "x" * (4 * 1024 * 1024 + 1)
    assert client.post("/v1/events", content=huge, headers=auth("write")).status_code == 413


def test_backpressure_maps_to_429(client_backpressure):
    r = client_backpressure.post("/v1/events", json=[evj("d")], headers=auth("write"))
    assert r.status_code == 429 and r.headers["retry-after"] == "3"


def test_correction_override_visible(client):
    w = auth("write")
    client.post(
        "/v1/events",
        json=[evj("d", "stage_changed", {"to_stage": "classified", "doc_type": "contract"})],
        headers=w,
    )
    r = client.post(
        "/v1/docs/d/corrections",
        json={
            "field_path": "doc_type",
            "new_value": "merger_agreement",
            "reason": "misfiled",
            "reviewer": "jb",
        },
        headers=w,
    )
    assert r.json()["seq"] == 2
    client.post(
        "/v1/events",
        json=[evj("d", "stage_changed", {"to_stage": "archived", "doc_type": "contract"}, ref="p2")],
        headers=w,
    )
    dt = client.get("/v1/docs/d", headers=auth("read")).json()["doc_type"]
    assert dt == {"value": "merger_agreement", "overridden_by": 2, "pipeline_value": "contract"}


def test_correction_of_bad_field_is_422(client):
    r = client.post(
        "/v1/docs/d/corrections",
        json={"field_path": "stage", "new_value": "x", "reason": "r", "reviewer": "jb"},
        headers=auth("write"),
    )
    assert r.status_code == 422


def test_correction_idempotency_key(client):
    body = {"field_path": "doc_type", "new_value": "x", "reason": "r", "reviewer": "jb"}
    h = {**auth("write"), "idempotency-key": "k1"}
    a = client.post("/v1/docs/d/corrections", json=body, headers=h).json()
    b = client.post("/v1/docs/d/corrections", json=body, headers=h).json()
    assert a["status"] == "written" and b["status"] == "duplicate" and a["seq"] == b["seq"]


def test_annotation(client):
    r = client.post(
        "/v1/docs/d/annotations", json={"note": "call client", "author": "jb"}, headers=auth("write")
    )
    assert r.json()["status"] == "written"


def test_verify_endpoint(client):
    client.post("/v1/events", json=[evj("v", ref=str(i)) for i in range(3)], headers=auth("write"))
    rep = client.get("/v1/verify/doc/v", headers=auth("read")).json()
    assert rep["ok"] and rep["events"] == 3


def test_blob_put_get(client):
    data = b"scanned letter"
    sha = hashlib.sha256(data).hexdigest()
    assert client.put(f"/v1/blobs/{sha}", content=data, headers=auth("write")).status_code == 200
    assert client.put(f"/v1/blobs/{sha}", content=b"other", headers=auth("write")).status_code == 422
    assert client.get(f"/v1/blobs/{sha}", headers=auth("read")).content == data
    assert client.get(f"/v1/blobs/{'0' * 64}", headers=auth("read")).status_code == 404


def test_metrics_exposed(client):
    client.post("/v1/events", json=[evj("m")], headers=auth("write"))
    text = client.get("/metrics", headers=auth("read")).text
    assert 'archive_events_ingested_total{kind="annotation",source="s",status="written"} 1.0' in text
    for name in (
        "archive_ingest_latency_seconds",
        "archive_shard_queue_depth",
        "archive_write_retries_total",
        "archive_spool_events",
        "archive_conflicts_total",
        "archive_verify_failures_total",
    ):
        assert name in text


def test_store_outage_on_read_is_503(tmp_path):
    store = FakeStore()
    store.fail_after_writes(0)
    store._down = True
    with make_client(tmp_path, ShardPool(store, None, shards=4, retry_delays=(0, 0, 0))) as c:
        r = c.get("/v1/docs/d", headers=auth("read"))
    assert r.status_code == 503 and r.headers["retry-after"] == "5"


def test_correction_with_null_value_is_kept(client):
    body = {"field_path": "doc_subclass", "new_value": None, "reason": "r", "reviewer": "jb"}
    r = client.post("/v1/docs/d/corrections", json=body, headers=auth("write"))
    assert r.status_code == 200 and r.json()["status"] == "written"
    tl = client.get("/v1/docs/d/timeline", headers=auth("read")).json()
    assert "new_value" in tl[0]["payload"] and tl[0]["payload"]["new_value"] is None


def test_bad_payloads_rejected_with_422(client):
    w = {**auth("write"), "content-type": "application/json"}
    assert (
        client.post(
            "/v1/events",
            content='[{"doc_id":"d","kind":"annotation","actor":"a","source":"s",'
            '"source_ref":"r","occurred_us":1,"payload":{"x":NaN}}]',
            headers=w,
        ).status_code
        == 422
    )
    ns = evj("d")
    ns["occurred_us"] = 1_763_650_000_000_000_000
    assert client.post("/v1/events", json=[ns], headers=w).status_code == 422
    assert (
        client.post("/v1/events", json=[evj("d", "stage_changed", {"doc_type": "x"})], headers=w).status_code
        == 422
    )
    assert (
        client.post(
            "/v1/docs/" + "x" * 200 + "/annotations", json={"note": "n", "author": "a"}, headers=w
        ).status_code
        == 422
    )


def test_idempotency_key_is_scoped_per_document(client):
    h = {**auth("write"), "idempotency-key": "same"}
    body = {"note": "n", "author": "a"}
    a = client.post("/v1/docs/a/annotations", json=body, headers=h).json()
    b = client.post("/v1/docs/b/annotations", json=body, headers=h).json()
    assert a["status"] == b["status"] == "written"


def test_reused_ref_on_another_doc_is_409(client):
    w = auth("write")
    client.post("/v1/events", json=[evj("a", ref="K")], headers=w)
    assert client.post("/v1/events", json=[evj("b", ref="K")], headers=w).status_code == 409
