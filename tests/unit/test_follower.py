"""SQLite -> real ingest/writer -> verified append-only history, without Docker."""

import json
import sqlite3

import httpx
import pytest
from fakes import FakeBlobs, FakeStore

from mailroom_archive.api.app import Deps, create_app
from mailroom_archive.core.kinds import Kind
from mailroom_archive.follower.events import timestamp_us
from mailroom_archive.follower.runner import ArchiveClient, Follower
from mailroom_archive.follower.sqlite import SQLiteSource
from mailroom_archive.settings import Settings
from mailroom_archive.verify import verify_events
from mailroom_archive.writer.pool import ShardPool

STAMP = "2026-11-20 12:00:00.123456"


class MemoryCursors:
    def __init__(self):
        self.positions = {}
        self.fail = False

    async def get(self, name):
        return self.positions.get(name)

    async def put(self, name, position):
        if self.fail:
            self.fail = False
            raise RuntimeError("checkpoint interrupted")
        self.positions[name] = position


@pytest.fixture
def sql(tmp_path):
    path = tmp_path / "pipeline.db"
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE audit_log (
                entry_id TEXT PRIMARY KEY, doc_id TEXT, matter_id TEXT, event TEXT,
                actor TEXT, timestamp TEXT, detail TEXT, seq INTEGER,
                entry_hash TEXT, prev_hash TEXT
            );
            CREATE TABLE audit_meta (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE audit_addenda (
                addendum_id TEXT PRIMARY KEY, doc_id TEXT, kind TEXT, seq INTEGER,
                actor TEXT, timestamp TEXT, detail TEXT, entry_hash TEXT, prev_hash TEXT
            );
            CREATE TABLE audit_attempts (
                attempt_id TEXT PRIMARY KEY, parent_id TEXT, doc_id TEXT, stage TEXT,
                attempt_no INTEGER, trigger TEXT, outcome TEXT, started_at TEXT,
                ended_at TEXT, detail TEXT, entry_hash TEXT
            );
            CREATE TABLE documents (
                doc_id TEXT PRIMARY KEY, matter_id TEXT, stage TEXT, doc_type TEXT,
                updated_at TEXT, scores TEXT, extracted_data TEXT, original_filename TEXT
            );
        """)
    return path


def unified(path):
    with sqlite3.connect(path) as db:
        db.execute("INSERT INTO audit_meta VALUES ('migration', '{}')")


def parent(path, identity="entry-1", doc="d", detail=None, seq=1):
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO audit_log VALUES (?, ?, 'm', 'archived', 'pipeline', ?, ?, ?, ?, '')",
            (identity, doc, STAMP, json.dumps(detail or {}), seq, "11" * 32),
        )


def addendum(path, identity, detail=None, kind="rescore", timestamp=STAMP):
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO audit_addenda VALUES (?, 'd', ?, 1, 'judge', ?, ?, ?, ?)",
            (identity, kind, timestamp, json.dumps(detail or {}), "22" * 32, "11" * 32),
        )


def attempt(path, identity, parent_id, score):
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO audit_attempts VALUES (?, ?, 'd', 'judge', 1, 'initial', 'ok', ?, ?, ?, ?)",
            (identity, parent_id, STAMP, STAMP, json.dumps({"judge_score": score}), "33" * 32),
        )


@pytest.fixture
async def target():
    store, blobs = FakeStore(), FakeBlobs()
    pool = ShardPool(store, None, shards=4, blobs=blobs, retry_delays=())
    app = create_app(Settings(token="test"), deps=Deps(store, pool, None, blobs))
    await pool.start()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app),
        base_url="http://archive",
        headers={"Authorization": "Bearer test"},
    ) as client:
        yield store, ArchiveClient(client), blobs
    await pool.stop()


def follower(path, target, cursors=None, **kwargs):
    return Follower(SQLiteSource(path), target[1], cursors or MemoryCursors(), name="pipeline-a", **kwargs)


def payloads(store, kind):
    return [json.loads(e.payload) for e in store.timeline_sync("d") if e.kind == kind]


def test_source_is_readonly_and_missing_source_is_not_created(tmp_path, sql):
    source = SQLiteSource(sql)
    with source.snapshot() as snap:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            snap.db.execute("DELETE FROM audit_log")
    missing = tmp_path / "absent.db"
    with pytest.raises(sqlite3.OperationalError):
        with SQLiteSource(missing).snapshot():
            pass
    assert not missing.exists()


def test_timestamp_has_exact_microseconds():
    assert timestamp_us("1970-01-01T00:00:00.000001Z") == 1
    assert timestamp_us("1970-01-01T01:00:00.000001+01:00") == 1
    assert timestamp_us(STAMP) == timestamp_us(STAMP.replace(" ", "T") + "Z")


async def test_legacy_mode_uses_rowid_even_when_meta_table_exists(sql, target):
    for i in range(7):
        parent(sql, identity=f"old-{i}", seq=0)
    f = follower(sql, target, page_size=2)
    assert (await f.poll_once()).rows == 7
    assert (await f.poll_once()).rows == 0
    rows = payloads(target[0], Kind.audit_row)
    assert len(rows) == 7 and all(r["legacy"] for r in rows)
    assert json.loads(f.cursors.positions["follower:pipeline-a:legacy"]) == 7
    assert verify_events("d", target[0].timeline_sync("d"), target[2]).ok


async def test_original_verdict_and_later_scores_keep_attempt_and_parent_provenance(sql, target):
    unified(sql)
    original = {
        "judge_verdict": "fail",
        "judge_score": 0.2,
        "judge_findings": ["missing §12"],
        "extraction": {"extracted_data": {"amount": 40}},
        "trace": [
            {
                "visit_id": "v1",
                "node": "judge_verify",
                "prompt": "private prompt",
                "completion": "private completion",
                "decision": {"judge_score": 0.2},
            }
        ],
    }
    parent(sql, detail=original)
    attempt(sql, "judge-1", "entry-1", 0.2)
    addendum(
        sql,
        "rescore-1",
        {"judge_verdict": "pass", "judge_score": 0.95, "scoring": {"kpi": 0.8}, "policy_version": "judge-v2"},
    )
    addendum(sql, "resume-1", {"run_attempt": 2}, kind="resumed")
    attempt(sql, "judge-2", "resume-1", 0.95)
    f = follower(sql, target, page_size=1)
    await f.poll_once()
    rows = target[0].timeline_sync("d")
    # Repeat overlap and restart a worker; original event IDs and chain stay intact.
    await follower(sql, target, f.cursors, page_size=1).poll_once()
    assert target[0].timeline_sync("d") == rows
    assert payloads(target[0], Kind.audit_row)[0]["detail"] == original
    additions = payloads(target[0], Kind.audit_addendum)
    assert additions[0]["detail"]["judge_score"] == 0.95
    assert additions[0]["prev_hash"] == "11" * 32
    attempts = payloads(target[0], Kind.audit_attempt)
    assert [(a["parent_id"], a["detail"]["judge_score"]) for a in attempts] == [
        ("entry-1", 0.2),
        ("resume-1", 0.95),
    ]
    visit = payloads(target[0], Kind.node_visit)[0]
    assert "prompt" not in visit and "completion" not in visit
    assert visit["decision"]["judge_score"] == 0.2
    assert (await target[0].get_head("d")).extracted_data == {"amount": 40}
    assert verify_events("d", rows, target[2]).ok


async def test_catalog_snapshots_preserve_prior_entries_and_same_timestamp_revisions(sql, target):
    with sqlite3.connect(sql) as db:
        db.execute(
            "INSERT INTO documents VALUES ('d', 'm', 'review', 'contract', ?, ?, ?, 'original.pdf')",
            (STAMP, json.dumps({"judge_score": 0.2}), json.dumps({"amount": 40})),
        )
    f = follower(sql, target, page_size=1)
    await f.poll_once()
    with sqlite3.connect(sql) as db:
        db.execute(
            "UPDATE documents SET stage = 'archived', scores = ? WHERE doc_id = 'd'",
            (json.dumps({"judge_score": 0.95}),),
        )
    await f.poll_once()
    await f.poll_once()
    history = payloads(target[0], Kind.stage_changed)
    assert [p["to_stage"] for p in history] == ["review", "archived"]
    assert [p["catalog_snapshot"]["scores"]["judge_score"] for p in history] == [0.2, 0.95]
    assert all(p["observed"] for p in history)
    assert (await target[0].get_head("d")).stage == "archived"


async def test_overlap_drains_all_pages_without_lowering_checkpoint(sql, target):
    unified(sql)
    for i in range(8):
        addendum(sql, f"a-{i}")
    f = follower(sql, target, page_size=2)
    await f.poll_once()
    key = "follower:pipeline-a:audit_addenda"
    original_checkpoint = f.cursors.positions[key]
    # A late transaction with an earlier timestamp remains inside the 10 minute overlap.
    addendum(sql, "late", timestamp="2026-11-20 11:55:00.123456")
    assert (await f.poll_once()).rows == 9
    assert f.cursors.positions[key] == original_checkpoint
    await f.poll_once()
    assert len(payloads(target[0], Kind.audit_addendum)) == 9


async def test_partial_delivery_does_not_checkpoint_parent(sql, target):
    unified(sql)
    parent(sql, detail={"judge_score": 0.2})
    attempt(sql, "judge-1", "entry-1", 0.2)

    class Interrupted:
        async def submit(self, events):
            await target[1].submit(events[:1])
            raise RuntimeError("connection lost after parent acknowledgement")

    cursors = MemoryCursors()
    f = Follower(SQLiteSource(sql), Interrupted(), cursors, name="pipeline-a")
    with pytest.raises(RuntimeError):
        await f.poll_once()
    assert cursors.positions == {}
    await follower(sql, target, cursors).poll_once()
    assert len(payloads(target[0], Kind.audit_row)) == 1
    assert len(payloads(target[0], Kind.audit_attempt)) == 1
    assert verify_events("d", target[0].timeline_sync("d"), target[2]).ok


async def test_checkpoint_failure_replays_identical_event(sql, target):
    parent(sql)
    cursors = MemoryCursors()
    cursors.fail = True
    with pytest.raises(RuntimeError, match="checkpoint interrupted"):
        await follower(sql, target, cursors).poll_once()
    before = target[0].timeline_sync("d")
    await follower(sql, target, cursors).poll_once()
    assert target[0].timeline_sync("d") == before


async def test_migration_replays_renumbered_legacy_rows_before_unified_chain(sql, target):
    parent(sql, identity="old-1", seq=0)
    f = follower(sql, target, page_size=1)
    await f.poll_once()
    parent(sql, identity="old-2", seq=0)  # committed between the last poll and migration
    with sqlite3.connect(sql) as db:
        db.executescript("""
            CREATE TABLE audit_log_legacy AS SELECT * FROM audit_log ORDER BY entry_id DESC;
            DELETE FROM audit_log;
            INSERT INTO audit_meta VALUES ('migration', '{}');
        """)
    parent(sql, identity="unified-1", detail={"judge_score": 0.9})
    await f.poll_once()
    await f.poll_once()
    rows = payloads(target[0], Kind.audit_row)
    assert [r["entry_id"] for r in rows] == ["old-1", "old-2", "unified-1"]
    assert rows[0]["legacy"] and rows[1]["legacy"] and "legacy" not in rows[2]
    assert verify_events("d", target[0].timeline_sync("d"), target[2]).ok


async def test_malformed_json_blocks_checkpoint_and_can_be_repaired(sql, target):
    parent(sql)
    with sqlite3.connect(sql) as db:
        db.execute("UPDATE audit_log SET detail = '{bad json'")
    f = follower(sql, target)
    with pytest.raises(json.JSONDecodeError):
        await f.poll_once()
    assert f.cursors.positions == {}
    with sqlite3.connect(sql) as db:
        db.execute("UPDATE audit_log SET detail = '{}' ")
    await f.poll_once()
    assert len(payloads(target[0], Kind.audit_row)) == 1


async def test_acknowledgement_validation():
    from mailroom_archive.core.models import EventIn

    event = EventIn(
        doc_id="d", kind="annotation", actor="a", source="s", source_ref="r", occurred_us=1, payload={}
    )
    for reply in ([], [{"doc_id": "wrong", "status": "written"}]):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request, reply=reply: httpx.Response(200, json=reply)),
            base_url="http://archive",
        ) as client:
            with pytest.raises(ValueError, match="acknowledgement"):
                await ArchiveClient(client).submit([event])


async def test_http_failure_keeps_checkpoint_for_retry(sql, target):
    parent(sql)
    cursors = MemoryCursors()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503)), base_url="http://archive"
    ) as client:
        broken = Follower(SQLiteSource(sql), ArchiveClient(client), cursors, name="pipeline-a")
        with pytest.raises(httpx.HTTPStatusError):
            await broken.poll_once()
    assert cursors.positions == {}
    await follower(sql, target, cursors).poll_once()
    assert len(payloads(target[0], Kind.audit_row)) == 1


async def test_source_read_locks_close_before_network_and_poll_has_a_fixed_end(sql, target):
    parent(sql)

    class PipelineKeepsWriting:
        async def submit(self, events):
            # Default journal mode is DELETE: this write fails immediately if the
            # follower still holds its source snapshot while awaiting ingest.
            with sqlite3.connect(sql, timeout=0) as db:
                db.execute(
                    "INSERT INTO audit_log SELECT 'next-' || count(*), 'd', 'm', 'archived', "
                    "'pipeline', ?, '{}', 0, ?, '' FROM audit_log",
                    (STAMP, "44" * 32),
                )
            await target[1].submit(events)

    f = Follower(SQLiteSource(sql), PipelineKeepsWriting(), MemoryCursors(), name="pipeline-a", page_size=1)
    result = await f.poll_once()
    assert result.rows == 1  # newly committed work waits for the next poll
    assert len(payloads(target[0], Kind.audit_row)) == 1


async def test_migration_between_pages_is_retried_without_skipping_legacy(sql, target):
    from mailroom_archive.follower.sqlite import SourceChanged

    parent(sql, identity="old-1", seq=0)
    parent(sql, identity="old-2", seq=0)

    class MigratingPipeline:
        async def submit(self, events):
            await target[1].submit(events)
            with sqlite3.connect(sql) as db:
                db.executescript("""
                    CREATE TABLE audit_log_legacy AS SELECT * FROM audit_log ORDER BY entry_id DESC;
                    DELETE FROM audit_log;
                    INSERT INTO audit_meta VALUES ('migration', '{}');
                """)

    cursors = MemoryCursors()
    f = Follower(SQLiteSource(sql), MigratingPipeline(), cursors, name="pipeline-a", page_size=1)
    with pytest.raises(SourceChanged):
        await f.poll_once()
    parent(sql, identity="unified-1")
    await follower(sql, target, cursors, page_size=1).poll_once()
    assert [r["entry_id"] for r in payloads(target[0], Kind.audit_row)] == ["old-1", "old-2", "unified-1"]


async def test_http_batch_limits_split_large_trace_without_checkpointing_early(sql):
    unified(sql)
    parent(sql, detail={"trace": [{"node": "judge_verify", "visit_id": f"v-{i}"} for i in range(1001)]})
    batches = []

    def acknowledge(request):
        body = json.loads(request.content)
        batches.append(body)
        return httpx.Response(200, json=[{"doc_id": ev["doc_id"], "status": "spooled"} for ev in body])

    cursors = MemoryCursors()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(acknowledge), base_url="http://archive"
    ) as client:
        f = Follower(SQLiteSource(sql), ArchiveClient(client), cursors, name="pipeline-a")
        await f.poll_once()
    assert list(map(len, batches)) == [1000, 2]
    assert json.loads(cursors.positions["follower:pipeline-a:audit_log"]) == 1


async def test_http_byte_limit_and_single_oversized_event():
    from mailroom_archive.core.models import EventIn

    base = EventIn(
        doc_id="d",
        kind="annotation",
        actor="a",
        source="s",
        source_ref="r",
        occurred_us=1,
        payload={"text": "é" * 1_100_000},
    )
    sizes = []

    def acknowledge(request):
        sizes.append(len(request.content))
        return httpx.Response(200, json=[{"doc_id": "d", "status": "written"}])

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(acknowledge), base_url="http://archive"
    ) as client:
        archive = ArchiveClient(client)
        await archive.submit([base, base.model_copy(update={"source_ref": "r2"})])
        assert len(sizes) == 2 and max(sizes) <= 4 * 1024 * 1024
        with pytest.raises(ValueError, match="4 MiB"):
            await archive.submit([base.model_copy(update={"payload": {"text": "é" * 3_000_000}})])


async def test_large_judge_findings_spill_and_still_verify(sql, target):
    unified(sql)
    detail = {"judge_verdict": "fail", "judge_score": 0.2, "judge_findings": ["missing §12; " * 2000]}
    parent(sql, detail=detail)
    await follower(sql, target).poll_once()
    rows = target[0].timeline_sync("d")
    assert len(rows) == 1 and rows[0].payload_ref
    body = target[2].get(rows[0].payload_sha256)
    assert json.loads(body)["detail"] == detail
    assert verify_events("d", rows, target[2]).ok


async def test_catalog_follower_respects_archive_correction(sql, target):
    with sqlite3.connect(sql) as db:
        db.execute(
            "INSERT INTO documents VALUES ('d', 'm', 'review', 'contract', ?, '{}', '{}', 'original.pdf')",
            (STAMP,),
        )
    f = follower(sql, target)
    await f.poll_once()
    from mailroom_archive.core.models import EventIn

    await target[1].submit(
        [
            EventIn(
                doc_id="d",
                kind="correction",
                actor="reviewer",
                source="api",
                source_ref="correct-type",
                occurred_us=timestamp_us(STAMP),
                payload={
                    "origin": "archive",
                    "field_path": "doc_type",
                    "new_value": "merger_agreement",
                    "reason": "reviewed",
                },
            )
        ]
    )
    with sqlite3.connect(sql) as db:
        db.execute("UPDATE documents SET stage = 'archived'")
    await f.poll_once()
    head = await target[0].get_head("d")
    assert head.stage == "archived" and head.doc_type == "merger_agreement"
    assert payloads(target[0], Kind.conflict)[0]["pipeline_value"] == "contract"
    assert verify_events("d", target[0].timeline_sync("d"), target[2]).ok
