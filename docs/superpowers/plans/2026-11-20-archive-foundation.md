# mailroom-archive Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fresh `mailroom-archive` repository with the event model, the Cassandra schema, the
shard writer (chained events, current state, corrections and conflicts), the spool, arrivals
and blobs, the ingest/read/verify API, and a test proving it survives one node down. That's
everything a producer needs to write to the archive and read back verified history.

**Architecture:** Pure core (canonical JSON, hashing, event and state rules) with no I/O;
a store layer wrapping prepared CQL; a shard pool that serializes each document's writes;
a SQLite spool for outages; a FastAPI app and a Typer CLI on top. Integration tests run
against Cassandra 5 in Docker Compose (`single` profile per push, `small` 3-node profile for
the fault test).

**Where tests run:** unit tests on the Mac (`uv run pytest tests/unit`); everything that needs
Cassandra on the Docker test host through `scripts/vm.sh` (see Global Constraints).

**Tech Stack:** Python 3.12 (managed by uv), FastAPI + uvicorn, cassandra-driver, pydantic v2,
xxhash, structlog, prometheus-client, Typer, httpx, pytest + pytest-asyncio, Cassandra 5.0
(`cassandra:5.0` image), Docker Compose.

**Spec:** `docs/superpowers/specs/2026-11-20-mailroom-archive-design.md` (approved 2026-11-20).
Section numbers below (§) refer to it.

**This is plan 1 of 5.** Later plans, each written after the previous one lands:
2. follower, inbox probe, ArchiveSink, upstream hash version 2 parity (§7.6–7.8);
3. feed exporter, Parquet warehouse, seals, `analyze` and the DuckDB views and queries (§6.4 seals, §7.5, §8);
4. simulator modes A and B with scenarios and `sim check` (§9);
5. observability (dashboards, alerts, OpenTelemetry), tiering, multi-instance leases, the Scylla profile (§7.3, §10, §11).

## Global Constraints

- New code only: nothing copied from third-party repositories. The publication scan (Task 10) reads its deny-list from the untracked file `.publish-denylist` (one regex per line), so the list itself is never committed.
- Commits are authored under the owner's GitHub noreply identity, with no trailers or footers of any kind.
- **Tests that need Docker run on a remote test host** named by the SSH alias in `ARCHIVE_VM_HOST` (required; never hard-coded). The host has 4 CPUs, 7.8 GB, Docker and uv. `scripts/vm.sh sync` copies the working tree to `~/mailroom-archive` there with rsync; `scripts/vm.sh run <command>` runs a command in that folder. Nothing on the host is a git remote.
- **Other projects on the test host stay untouched.** Every Docker command is scoped to compose project `mailroom-archive` (`docker compose -p mailroom-archive ...`) or to containers named `mailroom-archive-*`. Never run `docker system prune`, `docker volume prune`, `docker image prune`, `docker network prune`, or `docker compose down` without `-p mailroom-archive`. Cleanup is `docker compose -p mailroom-archive -f ops/compose.yml down -v`.
- Python `>=3.12`; package name `mailroom-archive`, import name `mailroom_archive`, CLI `mailroom-archive`; src layout.
- Keyspaces `archive` and `archive_sim`; RF 3 (`single` profile: RF 1); session consistency `QUORUM`.
- `doc_id` is `text`, 1 to 128 characters. `arrival_id` is a UUIDv7 string. Hashes are 32-byte `blob` in Cassandra and lowercase hex in JSON.
- `HASH_VERSION = "a1"`. Canonical JSON: `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")`.
- Event time is `occurred_us` (int, microseconds since the epoch, UTC). `event_id` is a `timeuuid` minted immediately before the write.
- Shard count 64 (`xxhash64(doc_id) % 64`); feed shard count 16 (`xxhash64(doc_id) % 16`); shard queue size 2,000.
- Payloads over 16 KiB spill to the blob store (`payload_ref`); batch limit 1,000 events or 4 MiB.
- Write retries: 3 attempts with delays 100 ms, 400 ms and 1.6 s, then spool.
- TTLs: `events_by_time` 1209600 s; `trace_io` 2592000 s; `idempotency` 7776000 s.
- Every route except `/healthz` requires a bearer token: write token `MAILROOM_ARCHIVE_TOKEN`, read token `MAILROOM_ARCHIVE_READ_TOKEN` (the write token also reads).
- Kinds (closed set): `arrived, stage_changed, node_visit, trace_io, audit_row, audit_attempt, audit_addendum, pending_row, correction, conflict, annotation, relation, retention`. Clients may not send `conflict` or `retention` (writer-only).

## Review Focus

1. **The same event delivered twice, including after a crash between the idempotency write and Q8** → one stored event with the original `seq` and `event_hash`. Pinned in Task 4 (`test_redelivery_after_crash_reissues_identical_write`).
2. **Concurrent events for one document in one batch, and across two requests at once** → `seq` 1..n with no gaps, each `prev_hash` linking to the previous one. Pinned in Task 4 (`test_concurrent_submits_same_doc_form_one_chain`).
3. **Unicode and non-ASCII in payloads and ids** (`"Société Générale — §12"`, emoji in filenames) → hashes recompute identically after a round trip through Cassandra. Pinned in Task 1 (`test_canonical_bytes_unicode_stable`) and Task 3 (`test_unicode_round_trip_verifies`).
4. **A payload just over 16 KiB, or one containing floats like `0.1` or `1e-7`** → spilled payload verifies; floats re-serialize to the same bytes. Pinned in Task 1 (`test_float_canonical_round_trip`) and Task 6 (`test_large_payload_spills_and_verifies`).
5. **Cassandra unreachable mid-batch** → every event is either written or spooled, none lost, order kept per document after the drain. Pinned in Task 5 (`test_outage_mid_batch_spools_rest_and_drains_in_order`).

---

### Task 0: Fresh repository

Done before this plan's first commit: the repository starts from an empty history with only
this plan, the spec, `README.md` and `.gitignore`, authored under the Global Constraints
identity, and is pushed to a newly created private `Exios66/mailroom-archive`.

---

### Task 1: Project scaffold and the pure core

**Files:**
- Create: `pyproject.toml`, `src/mailroom_archive/__init__.py`, `src/mailroom_archive/core/__init__.py`, `core/canonical.py`, `core/hashing.py`, `core/kinds.py`, `core/models.py`, `core/ids.py`
- Test: `tests/unit/test_canonical.py`, `tests/unit/test_hashing.py`, `tests/unit/test_models.py`

**Interfaces:**
- Produces:
  - `canonical_bytes(obj: Any) -> bytes`
  - `sha256(data: bytes) -> bytes` (32 bytes)
  - `HASH_VERSION: Final = "a1"`
  - `event_hash(*, doc_id: str, seq: int, kind: str, occurred_us: int, actor: str, source: str, source_ref: str, payload_sha256: bytes, prev_hash: bytes) -> bytes`: hashes `canonical_bytes({"hash_version", "doc_id", "seq", "kind", "occurred_us", "actor", "source", "source_ref", "payload_sha256": hex, "prev_hash": hex or ""})`
  - `class Kind(StrEnum)` with the 13 kinds; `CLIENT_KINDS: frozenset[Kind]` (all but `conflict`, `retention`)
  - `new_event_id() -> uuid.UUID` (time-based UUID v1, usable as a CQL `timeuuid`); `new_arrival_id() -> str` (UUIDv7 string); `event_id_us(event_id: uuid.UUID) -> int`
  - pydantic models in `core/models.py`:
    - `EventIn(doc_id: str, kind: Kind, actor: str, source: str, source_ref: str, occurred_us: int, payload: dict[str, Any], matter_id: str | None = None, sim_run_id: str | None = None)` with `doc_id` 1–128 chars and `occurred_us` ≥ 0
    - `StoredEvent(doc_id, seq: int, event_id: UUID, kind, actor, source, source_ref, occurred_us, payload: bytes, payload_ref: str | None, payload_sha256: bytes, prev_hash: bytes, event_hash: bytes, corrects_seq: int | None)`
    - `WriteResult(doc_id: str, status: Literal["written", "duplicate", "spooled"], seq: int | None, event_hash: str | None)`

- [ ] **Step 1: Write `pyproject.toml`** (hatchling build; dependencies: `fastapi`, `uvicorn[standard]`, `cassandra-driver`, `pydantic>=2`, `xxhash`, `structlog`, `prometheus-client`, `typer`, `httpx`, `uuid6`; dev group: `pytest`, `pytest-asyncio`, `ruff`; script `mailroom-archive = "mailroom_archive.cli:app"`; pytest markers `integration` and `fault`; `asyncio_mode = "auto"`). Pin to the newest stable versions at build time. Run `uv sync` and check that it succeeds.

- [ ] **Step 2: Write the failing tests**

```python
# tests/unit/test_canonical.py
def test_canonical_bytes_sorted_compact():
    assert canonical_bytes({"b": 1, "a": [2, {"d": 3, "c": 4}]}) == b'{"a":[2,{"c":4,"d":3}],"b":1}'

def test_canonical_bytes_unicode_stable():
    s = {"name": "Société Générale — §12", "f": "📎 claim.pdf"}
    out = canonical_bytes(s)
    assert out == '{"f":"📎 claim.pdf","name":"Société Générale — §12"}'.encode("utf-8")
    assert canonical_bytes(json.loads(out)) == out

def test_float_canonical_round_trip():
    for v in (0.1, 1e-7, 123456.789, -0.0):
        out = canonical_bytes({"v": v})
        assert canonical_bytes(json.loads(out)) == out

def test_canonical_rejects_nan():
    with pytest.raises(ValueError):
        canonical_bytes({"v": float("nan")})

# tests/unit/test_hashing.py
def test_event_hash_matches_documented_form():
    ph = bytes.fromhex("11" * 32)
    got = event_hash(doc_id="doc-1", seq=1, kind="arrived", occurred_us=1763650000000000,
                     actor="probe", source="inbox", source_ref="inbox:a", payload_sha256=ph, prev_hash=b"")
    expected = ('{"actor":"probe","doc_id":"doc-1","hash_version":"a1","kind":"arrived",'
                '"occurred_us":1763650000000000,"payload_sha256":"' + "11" * 32 + '",'
                '"prev_hash":"","seq":1,"source":"inbox","source_ref":"inbox:a"}')
    assert got == hashlib.sha256(expected.encode()).digest()

def test_event_hash_changes_with_any_field():
    base = dict(doc_id="d", seq=2, kind="correction", occurred_us=1, actor="a", source="s",
                source_ref="r", payload_sha256=b"\x00" * 32, prev_hash=b"\x01" * 32)
    h = event_hash(**base)
    for k, v in [("seq", 3), ("kind", "annotation"), ("occurred_us", 2), ("prev_hash", b"\x02" * 32)]:
        assert event_hash(**{**base, k: v}) != h

# tests/unit/test_models.py
def test_doc_id_length_bounds():
    with pytest.raises(ValidationError): EventIn(doc_id="", kind="arrived", actor="a", source="s", source_ref="r", occurred_us=0, payload={})
    with pytest.raises(ValidationError): EventIn(doc_id="x" * 129, kind="arrived", actor="a", source="s", source_ref="r", occurred_us=0, payload={})
    assert EventIn(doc_id="fixture-doc-a", kind="arrived", actor="a", source="s", source_ref="r", occurred_us=0, payload={}).doc_id == "fixture-doc-a"

def test_unknown_kind_rejected():
    with pytest.raises(ValidationError):
        EventIn(doc_id="d", kind="teleported", actor="a", source="s", source_ref="r", occurred_us=0, payload={})

def test_event_id_is_time_ordered():
    a, b = new_event_id(), new_event_id()
    assert a.version == 1 and event_id_us(a) <= event_id_us(b)
```

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/unit -v`
Expected: FAIL (ImportError: module `mailroom_archive.core...` not found).

- [ ] **Step 4: Implement the functions and models listed in Interfaces**

`canonical_bytes` is the exact `json.dumps` call in Global Constraints. In `event_hash`, empty `prev_hash` bytes encode as `""`.

- [ ] **Step 5: Run the tests and lint**

Run: `uv run pytest tests/unit -v && uv run ruff check .`
Expected: all PASS; ruff clean.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "Core: canonical JSON, event hashing, kinds, models"
```

---

### Task 2: Pure state rules: current state, corrections, conflicts

The rules in §6.5a, kept free of I/O so they are fully unit-tested.

**Files:**
- Create: `src/mailroom_archive/core/state.py`
- Test: `tests/unit/test_state.py`

**Interfaces:**
- Consumes: `EventIn`, `Kind` (Task 1)
- Produces:
  - `@dataclass(frozen=True) Head(seq: int, hash: bytes, matter_id: str | None, doc_type: str | None, doc_subclass: str | None, stage: str | None, stage_us: int | None, content_sha256: bytes | None, channel: str | None, original_filename: str | None, extracted_data: dict | None, overrides: dict[str, int], audit_entry_hash: bytes | None)`; `Head.empty()` returns seq 0, hash `b""`, everything else None or empty.
  - `@dataclass(frozen=True) StateUpdate(fields: dict[str, Any])`: only the static columns to set, with CQL names (`stage`, `stage_at`, `doc_type`, `doc_subclass`, `matter_id`, `content_sha256`, `channel`, `original_filename`, `extracted_data` as canonical bytes, `overrides` as `dict[str, int]`, `audit_entry_hash`, `created_at`, `sim_run_id`)
  - `plan_state(head: Head, ev: EventIn, seq: int) -> tuple[StateUpdate, list[EventIn]]`: returns the static update for this event and any follow-up writer events (`conflict`).
  - `CORRECTABLE_TOP: frozenset = {"doc_type", "doc_subclass", "matter_id"}`; other correctable paths start with `extracted_data.` (dot paths into the dict).

Rules (§6.5a):
- `stage_changed` sets `stage` (= `payload["to_stage"]`), `stage_at`, plus `doc_type`/`doc_subclass` when present, **except** fields under an override.
- `arrived` sets `content_sha256` (from hex), `channel`, `original_filename` and `created_at` (when `head.seq == 0`).
- `audit_row` sets `audit_entry_hash` from `payload["entry_hash"]` and `extracted_data` from `payload["detail"]["extraction"]["extracted_data"]` when present, except overridden paths.
- `correction` with `origin == "archive"` sets the field and `overrides[field_path] = seq`; with `release: true` it removes the override and leaves the value as is. With `origin == "pipeline"`, it is treated like any pipeline update.
- A pipeline value for an overridden field that differs from current state returns one `conflict` EventIn with `field_path, override_seq, archive_value, pipeline_value, pipeline_source_ref`, `actor="archive"`, `source="writer"`, and `source_ref=f"conflict:{ev.source_ref}:{field_path}"`.

- [ ] **Step 1: Write the failing tests**

```python
def test_stage_changed_sets_stage_and_type():
    upd, extra = plan_state(Head.empty(), ev("stage_changed", {"from_stage": "inbox", "to_stage": "classified", "doc_type": "contract"}), 1)
    assert upd.fields["stage"] == "classified" and upd.fields["doc_type"] == "contract" and extra == []

def test_archive_correction_overrides_and_records_seq():
    upd, _ = plan_state(head(doc_type="contract"), ev("correction", {"origin": "archive", "field_path": "doc_type", "new_value": "merger_agreement", "reason": "r", "reviewer": "jb", "corrects_seq": 3}), 7)
    assert upd.fields["doc_type"] == "merger_agreement" and upd.fields["overrides"] == {"doc_type": 7}

def test_pipeline_disagreeing_with_override_keeps_archive_value_and_emits_conflict():
    h = head(doc_type="merger_agreement", overrides={"doc_type": 7})
    upd, extra = plan_state(h, ev("stage_changed", {"to_stage": "archived", "doc_type": "contract"}, source_ref="documents:d:9"), 8)
    assert "doc_type" not in upd.fields and upd.fields["stage"] == "archived"
    assert [e.kind for e in extra] == ["conflict"]
    assert extra[0].payload == {"field_path": "doc_type", "override_seq": 7, "archive_value": "merger_agreement",
                                "pipeline_value": "contract", "pipeline_source_ref": "documents:d:9"}

def test_pipeline_agreeing_with_override_is_not_a_conflict():
    h = head(doc_type="contract", overrides={"doc_type": 7})
    _, extra = plan_state(h, ev("stage_changed", {"to_stage": "archived", "doc_type": "contract"}), 8)
    assert extra == []

def test_release_removes_override():
    upd, _ = plan_state(head(doc_type="x", overrides={"doc_type": 7}), ev("correction", {"origin": "archive", "field_path": "doc_type", "release": True, "reason": "r", "reviewer": "jb"}), 9)
    assert upd.fields["overrides"] == {}

def test_extracted_field_override_survives_audit_row():
    h = head(extracted_data={"policy_no": "A1"}, overrides={"extracted_data.policy_no": 4})
    row = {"entry_hash": "ab" * 32, "detail": {"extraction": {"extracted_data": {"policy_no": "B2", "amount": 10}}}}
    upd, extra = plan_state(h, ev("audit_row", row), 5)
    assert json.loads(upd.fields["extracted_data"]) == {"policy_no": "A1", "amount": 10}
    assert extra[0].payload["field_path"] == "extracted_data.policy_no"

def test_correction_of_unknown_field_rejected():
    with pytest.raises(ValueError):
        plan_state(Head.empty(), ev("correction", {"origin": "archive", "field_path": "stage", "new_value": "x", "reason": "r", "reviewer": "jb"}), 1)
```

(`ev(kind, payload, source_ref="r")` and `head(**fields)` are small test helpers in the test file.)

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_state.py -v`. Expected: FAIL (import error).

- [ ] **Step 3: Implement `Head`, `StateUpdate`, `plan_state` in `core/state.py`**

- [ ] **Step 4: Run them to verify they pass**

Run: `uv run pytest tests/unit -v`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "Core: current-state rules, archive overrides and conflicts"
```

---

### Task 3: Schema, compose profiles, store layer

**Files:**
- Create: `schema/00_keyspace.cql`, `schema/10_doc_timeline.cql`, `schema/20_arrivals.cql`, `schema/21_sightings_by_sha.cql`, `schema/22_blobs.cql`, `schema/30_events_by_time.cql`, `schema/40_trace_io.cql`, `schema/41_pending_rows.cql`, `schema/50_docs_by_matter.cql`, `schema/60_idempotency.cql`, `schema/70_seals.cql`, `schema/80_cursors.cql`, `schema/81_sim_runs.cql`
- Create: `src/mailroom_archive/store/__init__.py`, `store/session.py`, `store/schema.py`, `store/timeline.py`
- Create: `ops/compose.yml`, `ops/cassandra/small.env`, `ops/cassandra/full.env`, `scripts/vm.sh`
- Test: `tests/integration/conftest.py`, `tests/integration/test_schema.py`, `tests/integration/test_timeline_store.py`

**Interfaces:**
- Consumes: `StoredEvent`, `Head`, `StateUpdate` (Tasks 1–2)
- Produces:
  - `connect(hosts: list[str], *, username: str | None = None, password: str | None = None) -> cassandra.cluster.Session` with default consistency `QUORUM`, `dict_factory` rows, and protocol v5
  - `class StoreUnavailable(Exception)`: raised by every store method in place of the driver's `NoHostAvailable`, `Unavailable`, `ReadTimeout`, `WriteTimeout` and `OperationTimedOut`
  - `async def run(session, stmt, params: tuple | dict = ()) -> list[dict]`: wraps `execute_async` in an asyncio future
  - `apply_schema(session, keyspace: str, profile: Literal["single", "small", "full"]) -> list[str]`: renders every `schema/*.cql` (placeholders `{ks}`, `{rf}`, `{timeline_compaction}`) and returns the table names
  - `class TimelineStore(session, keyspace)`:
    - `async get_head(doc_id: str) -> Head` (Q1; `Head.empty()` when no partition)
    - `async write_event(ev: StoredEvent, upd: StateUpdate) -> None` (Q8: single-partition `BatchStatement` with the INSERT and one UPDATE of `upd.fields` plus `head_seq`/`head_hash`)
    - `async timeline(doc_id: str, after_seq: int = 0, limit: int | None = None) -> list[StoredEvent]` (Q2/Q3)
    - `async get_idem(source: str, source_ref: str) -> IdemRecord | None`; `async put_idem(source: str, source_ref: str, rec: IdemRecord) -> None`, where `IdemRecord(doc_id, seq, event_hash, event_id, payload_sha256, prev_hash)` holds everything needed to re-issue Q8
    - `async write_feed(ev: StoredEvent, *, shard16: int, channel, doc_type, stage, sim_run_id) -> None` (bucket = `YYYYMMDDHH` from `event_id` time, UTC)
    - `async add_to_matter(matter_id: str, created_us: int, doc_id: str, doc_type: str | None, content_sha256: bytes | None) -> None`; `async matter_docs(matter_id: str, year: int, limit: int = 500) -> list[dict]` (Q5)

Table definitions are exactly §6.4 (with `doc_id text`, `occurred_us bigint`, the `extracted_data`
and `overrides` statics, `seal_no` and `leaf_ids` in `seals`). `{timeline_compaction}` is
`{'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L4'}` for every Cassandra profile.
`{rf}` is 1 for `single` and 3 otherwise.

Compose (`ops/compose.yml`, project name `mailroom-archive`): services `cassandra-1..3`
(image `cassandra:5.0`, `CASSANDRA_SEEDS=cassandra-1`, `CASSANDRA_CLUSTER_NAME=mailroom-archive`,
`healthcheck: nodetool status | grep -c '^UN'` must reach the profile's node count,
data on named volumes). The `single` profile runs only `cassandra-1` with port 9042 published. The
`small` profile runs all three with `MAX_HEAP_SIZE=512M`, `HEAP_NEWSIZE=128M`, `mem_limit: 1200m`
and `CONCURRENT_COMPACTORS=1`; node 1 publishes 9042. `full`: `MAX_HEAP_SIZE=1G`, `mem_limit: 2g`.

- [ ] **Step 1: Write the CQL files, the compose file and `scripts/vm.sh`**

`scripts/vm.sh sync` runs `rsync -az --delete --exclude .git --exclude .venv --exclude data/ ./ "$ARCHIVE_VM_HOST":mailroom-archive/`. `scripts/vm.sh run <cmd...>` runs `ssh "$ARCHIVE_VM_HOST" "cd ~/mailroom-archive && ~/.local/bin/uv sync -q && <cmd>"`. `scripts/vm.sh up <profile>` runs `docker compose -p mailroom-archive -f ops/compose.yml --profile <profile> up -d --wait`, and `scripts/vm.sh down` runs `docker compose -p mailroom-archive -f ops/compose.yml down -v`. Around both, it records the names of running containers that are not `mailroom-archive-*`, and afterwards checks that every one of them is still running; if any is not, it stops and reports it.

- [ ] **Step 2: Write the failing integration tests**

```python
# conftest: skips unless CASSANDRA_HOSTS is set; creates keyspace "it_<random>" via apply_schema(profile from ARCHIVE_PROFILE, default "single"); drops it after.

def test_apply_schema_creates_all_tables(ks_session, ks):
    names = apply_schema(ks_session, ks, "single")
    assert set(names) == {"doc_timeline", "arrivals", "sightings_by_sha", "blobs", "events_by_time", "trace_io",
                          "pending_rows", "docs_by_matter", "idempotency", "seals", "cursors", "sim_runs"}

def test_apply_schema_is_idempotent(ks_session, ks):
    apply_schema(ks_session, ks, "single"); apply_schema(ks_session, ks, "single")

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
```

(`stored(...)` builds a `StoredEvent` with real hashes, using `event_hash` and `canonical_bytes`.)

- [ ] **Step 3: Start Cassandra and run the tests to verify they fail**

Run: `scripts/vm.sh sync && scripts/vm.sh up single && scripts/vm.sh run CASSANDRA_HOSTS=127.0.0.1 ~/.local/bin/uv run pytest tests/integration -m integration -v`
Expected: FAIL (import errors).

- [ ] **Step 4: Implement `store/session.py`, `store/schema.py`, `store/timeline.py`**

Prepare every statement once per keyspace in `TimelineStore.__init__`. Because the statics in a
Q8 UPDATE vary, prepare per distinct field set and cache by the sorted field names.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `scripts/vm.sh sync && scripts/vm.sh run CASSANDRA_HOSTS=127.0.0.1 ~/.local/bin/uv run pytest tests/integration -m integration -v`. Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "Schema, compose profiles and the timeline store"
```

---

### Task 4: Shard pool and writer

**Files:**
- Create: `src/mailroom_archive/writer/__init__.py`, `writer/shards.py`, `writer/pool.py`, `writer/spill.py`
- Test: `tests/unit/test_shards.py`, `tests/unit/test_pool.py` (against an in-memory fake store), `tests/integration/test_pool_cassandra.py`

**Interfaces:**
- Consumes: `TimelineStore` methods (Task 3); `plan_state` (Task 2); `event_hash`, `canonical_bytes`, `sha256`, `new_event_id` (Task 1)
- Produces:
  - `shard_of(doc_id: str, n: int = 64) -> int`; `feed_shard(doc_id: str) -> int` (n = 16); both `xxhash.xxh64_intdigest(doc_id.encode("utf-8")) % n`
  - `class Backpressure(Exception)` with attribute `retry_after: float`
  - `class ShardPool(store: TimelineStore, spool: Spool | None, *, shards: int = 64, queue_size: int = 2000, blobs: BlobStore | None = None)` with a public `store` attribute, `async start()`, `async stop()`, and `async submit_batch(events: list[EventIn]) -> list[WriteResult]`, which keeps input order in the results and raises `Backpressure` before enqueueing anything if any target queue lacks room
  - `spill_payload(payload_bytes: bytes, blobs: BlobStore | None) -> tuple[bytes, str | None]`: returns `(b"", "payload/<sha256hex>")` above 16 KiB (stored in blobs), else `(payload_bytes, None)`

Per event, inside the shard task (§7.2 and §6.4 idempotency order):
1. `get_idem(source, source_ref)`: a hit re-issues `write_event` with the recorded values and returns `duplicate`.
2. Head comes from the shard's LRU cache (10,000 entries), else `get_head`.
3. `seq = head.seq + 1`; payload bytes via `canonical_bytes(ev.payload)`; compute `payload_sha256` (over the full bytes, before spilling); `prev_hash = head.hash`; `event_hash`; `event_id = new_event_id()`.
4. `put_idem`, then `write_event(stored, plan_state(...)[0])`, then `write_feed`, then (the first time an event gives the document a `matter_id`) `add_to_matter`, then update the cache.
5. Any `conflict` events from `plan_state` are processed next, in the same shard, before the next queued event.
6. On a Cassandra error: retry the remaining steps 3 times (100 ms, 400 ms, 1.6 s), then hand the event to the spool (Task 5). Without a spool, raise.

- [ ] **Step 1: Write the failing tests**

```python
def test_shard_of_stable_and_in_range():
    assert shard_of("fixture-doc-a") == shard_of("fixture-doc-a")
    assert all(0 <= shard_of(f"d{i}") < 64 for i in range(1000))
    assert len({shard_of(f"d{i}") for i in range(10000)}) == 64

async def test_batch_for_one_doc_chains_in_order(pool, fake_store):
    res = await pool.submit_batch([ev("d", "arrived", ref=f"r{i}") for i in range(5)])
    assert [r.seq for r in res] == [1, 2, 3, 4, 5]
    tl = fake_store.timeline_sync("d")
    assert all(tl[i].prev_hash == tl[i - 1].event_hash for i in range(1, 5)) and tl[0].prev_hash == b""

async def test_concurrent_submits_same_doc_form_one_chain(pool, fake_store):
    await asyncio.gather(*[pool.submit_batch([ev("d", "annotation", ref=f"r{i}")]) for i in range(50)])
    assert [e.seq for e in fake_store.timeline_sync("d")] == list(range(1, 51))

async def test_duplicate_returns_original(pool):
    a = (await pool.submit_batch([ev("d", "arrived", ref="same")]))[0]
    b = (await pool.submit_batch([ev("d", "arrived", ref="same")]))[0]
    assert b.status == "duplicate" and (b.seq, b.event_hash) == (a.seq, a.event_hash)

async def test_redelivery_after_crash_reissues_identical_write(pool, fake_store):
    fake_store.fail_next("write_event")            # idem row written, Q8 lost
    with pytest.raises(StoreUnavailable):
        await pool_without_spool.submit_batch([ev("d", "arrived", ref="x")])
    r = (await pool_without_spool.submit_batch([ev("d", "arrived", ref="x")]))[0]
    assert r.status == "duplicate" and r.seq == 1 and len(fake_store.timeline_sync("d")) == 1

async def test_conflict_written_after_triggering_event(pool, fake_store):
    await pool.submit_batch([ev("d", "correction", {"origin": "archive", "field_path": "doc_type", "new_value": "x", "reason": "r", "reviewer": "jb"}, ref="c1")])
    await pool.submit_batch([ev("d", "stage_changed", {"to_stage": "archived", "doc_type": "y"}, ref="s1")])
    assert [e.kind for e in fake_store.timeline_sync("d")] == ["correction", "stage_changed", "conflict"]

async def test_backpressure_when_queue_full():
    pool = ShardPool(SlowFakeStore(), None, shards=1, queue_size=2)
    await pool.start()
    with pytest.raises(Backpressure):
        await asyncio.gather(*[pool.submit_batch([ev(f"d{i}", "arrived", ref=f"r{i}")]) for i in range(10)])

def test_spill_threshold():
    small, big = b"x" * 16384, b"x" * 16385
    assert spill_payload(small, FakeBlobs()) == (small, None)
    body, ref = spill_payload(big, FakeBlobs())
    assert body == b"" and ref == "payload/" + hashlib.sha256(big).hexdigest()
```

The integration test (`test_pool_cassandra.py`) runs `test_batch_for_one_doc_chains_in_order` and the concurrency test against the real `TimelineStore`.

- [ ] **Step 2: Run them to verify they fail.** Run: `uv run pytest tests/unit/test_shards.py tests/unit/test_pool.py -v`. Expected: FAIL (import).

- [ ] **Step 3: Implement `writer/shards.py`, `writer/spill.py`, `writer/pool.py`** (the fake store goes in `tests/fakes.py`, implementing the same async methods as `TimelineStore` plus `fail_next` and `timeline_sync`)

- [ ] **Step 4: Run them to verify they pass.** Run: `uv run pytest tests/unit -v && scripts/vm.sh sync && scripts/vm.sh run CASSANDRA_HOSTS=127.0.0.1 ~/.local/bin/uv run pytest tests/integration -m integration -v`. Expected: PASS.

- [ ] **Step 5: Commit** `git add -A && git commit -m "Shard pool: chained writes, idempotency-first, conflicts, back-pressure"`

---

### Task 5: Spool

**Files:**
- Create: `src/mailroom_archive/spool.py`
- Modify: `src/mailroom_archive/writer/pool.py` (hand off to the spool; route a shard's new events to the spool while it has pending events; drainer task)
- Test: `tests/unit/test_spool.py`, `tests/unit/test_pool_outage.py`

**Interfaces:**
- Consumes: `EventIn` (Task 1); `ShardPool` (Task 4)
- Produces:
  - `class Spool(path: Path)`: SQLite in WAL mode with `PRAGMA synchronous=FULL`; table `spooled(id INTEGER PRIMARY KEY, shard INT, source TEXT, source_ref TEXT, event_json TEXT, spooled_us INT, UNIQUE(source, source_ref))`
    - `append(shard: int, ev: EventIn) -> bool`: False if `(source, source_ref)` is already spooled
    - `pending_shards() -> set[int]`; `has_pending(shard: int) -> bool`
    - `peek(shard: int, limit: int = 100) -> list[tuple[int, EventIn]]` (oldest first); `ack(ids: list[int]) -> None`
    - `stats() -> SpoolStats(events: int, oldest_us: int | None)`
  - Spooled results: `WriteResult(status="spooled", seq=None, event_hash=None)`; a repeat of a spooled event returns `duplicate` with `seq=None`.
  - `ShardPool(..., health: Callable[[], Awaitable[bool]])`: the drainer polls `health()` every second and, when it's true, replays each pending shard oldest first through the normal per-event path before that shard accepts direct writes again.
  - `async ShardPool.wait_drained(timeout: float) -> None`: returns when the spool is empty; raises `TimeoutError` otherwise.
  - A startup warning `archive_spool_ephemeral` when the spool path is under a temp directory (`tempfile.gettempdir()`) or not under `MAILROOM_ARCHIVE_DATA_DIR`.

- [ ] **Step 1: Write the failing tests**

```python
def test_append_dedups_and_orders(tmp_path):
    sp = Spool(tmp_path / "s.db")
    assert sp.append(3, ev("d", ref="a")) and not sp.append(3, ev("d", ref="a"))
    sp.append(3, ev("d", ref="b"))
    assert [e.source_ref for _, e in sp.peek(3)] == ["a", "b"]

def test_survives_reopen(tmp_path):
    Spool(tmp_path / "s.db").append(1, ev("d", ref="a"))
    assert Spool(tmp_path / "s.db").has_pending(1)

async def test_outage_mid_batch_spools_rest_and_drains_in_order(tmp_path):
    store = FakeStore(); healthy = {"v": True}
    pool = ShardPool(store, Spool(tmp_path / "s.db"), shards=4, health=lambda: asyncio.sleep(0, healthy["v"]))
    await pool.start()
    store.fail_after_writes(3); healthy["v"] = False
    res = await pool.submit_batch([ev("d", "annotation", ref=f"r{i}") for i in range(10)])
    assert [r.status for r in res] == ["written"] * 3 + ["spooled"] * 7
    store.recover(); healthy["v"] = True
    await pool.wait_drained(timeout=10)
    assert [e.source_ref for e in store.timeline_sync("d")] == [f"r{i}" for i in range(10)]
    assert [e.seq for e in store.timeline_sync("d")] == list(range(1, 11))

async def test_new_events_follow_spooled_ones(tmp_path):
    store = FakeStore(); healthy = {"v": False}
    pool = ShardPool(store, Spool(tmp_path / "s.db"), shards=4, health=lambda: asyncio.sleep(0, healthy["v"]))
    await pool.start()
    store.fail_after_writes(3)
    await pool.submit_batch([ev("d", "annotation", ref=f"r{i}") for i in range(10)])
    store.recover()                                   # store works again, drainer not yet released
    late = (await pool.submit_batch([ev("d", "annotation", ref="late")]))[0]
    assert late.status == "spooled"                   # shard still has spooled events
    healthy["v"] = True
    await pool.wait_drained(timeout=10)
    assert store.timeline_sync("d")[-1].source_ref == "late" and store.timeline_sync("d")[-1].seq == 11
```

- [ ] **Step 2: Verify they fail.** `uv run pytest tests/unit/test_spool.py tests/unit/test_pool_outage.py -v` → FAIL.
- [ ] **Step 3: Implement `Spool` and the pool changes.**
- [ ] **Step 4: Verify they pass.** `uv run pytest tests/unit -v` → PASS.
- [ ] **Step 5: Commit** `git add -A && git commit -m "Spool: durable unsequenced fallback with ordered drain"`

---

### Task 6: Blobs, arrivals and sightings

**Files:**
- Create: `src/mailroom_archive/blobs.py`, `src/mailroom_archive/store/arrivals.py`
- Test: `tests/unit/test_blobs.py`, `tests/integration/test_arrivals.py`, `tests/integration/test_spill_cassandra.py`

**Interfaces:**
- Consumes: `run`, the session (Task 3); `new_arrival_id`, `sha256` (Task 1); `ShardPool` (Task 4)
- Produces:
  - `class BlobStore(Protocol)`: `put(data: bytes, expected_sha256: bytes | None = None) -> BlobInfo`, `get(sha256: bytes) -> bytes`, `exists(sha256: bytes) -> bool`
  - `class FsBlobStore(root: Path)`: path `root/ab/cd/<hex>`; writes to a temp file then `os.replace`; `put` raises `HashMismatch` when `expected_sha256` differs; idempotent
  - `BlobInfo(sha256: bytes, size_bytes: int, storage_uri: str)`
  - `class ArrivalStore(session, keyspace, blobs: BlobStore)`:
    - `async record(*, content: bytes | None, content_sha256: bytes, size_bytes: int, mime: str | None, channel: str, original_filename: str | None, intake_meta: dict, arrived_us: int, sim_run_id: str | None = None) -> Arrival` stores the blob (if content is given), `blobs` row (`INSERT ... IF NOT EXISTS`), arrival, and a sighting
    - `async sightings(content_sha256: bytes, months: int = 2, limit: int = 50) -> list[dict]`: one query per month, merged newest first (§6.6 Q4)
    - `async link(arrival_id: str, doc_id: str, linked_us: int) -> None`; `async get(arrival_id: str) -> dict | None`
  - `Arrival(arrival_id: str, duplicate_of: str | None)`: `duplicate_of` is the newest earlier sighting's `arrival_id`, which is also written into the stored `intake_meta`

- [ ] **Step 1: Write the failing tests**

```python
def test_put_get_and_mismatch(tmp_path):
    bs = FsBlobStore(tmp_path); data = b"claim form"
    info = bs.put(data, hashlib.sha256(data).digest())
    assert bs.get(info.sha256) == data and info.storage_uri.endswith(hashlib.sha256(data).hexdigest())
    with pytest.raises(HashMismatch): bs.put(b"other", info.sha256)

async def test_second_identical_arrival_is_duplicate(arrivals):
    a = await arrivals.record(content=b"x", content_sha256=sha256(b"x"), size_bytes=1, mime="text/plain", channel="upload", original_filename="a.txt", intake_meta={}, arrived_us=1)
    b = await arrivals.record(content=b"x", content_sha256=sha256(b"x"), size_bytes=1, mime="text/plain", channel="gmail", original_filename="a.txt", intake_meta={}, arrived_us=2)
    assert a.duplicate_of is None and b.duplicate_of == a.arrival_id

async def test_sightings_span_two_months(arrivals, clock):
    clock.set_month(-1); old = await record_x(arrivals)
    clock.set_month(0);  new = [await record_x(arrivals) for _ in range(3)]
    got = await arrivals.sightings(sha256(b"x"), months=2, limit=2)
    assert [g["arrival_id"] for g in got] == [new[2].arrival_id, new[1].arrival_id]
    got_all = await arrivals.sightings(sha256(b"x"), months=2, limit=10)
    assert got_all[-1]["arrival_id"] == old.arrival_id and len(got_all) == 4

async def test_large_payload_spills_and_verifies(pool_cassandra, blobs):
    payload = {"text": "é" * 20000}
    r = (await pool_cassandra.submit_batch([ev("big", "annotation", payload, ref="b")]))[0]
    stored = (await pool_cassandra.store.timeline("big"))[0]
    assert stored.payload == b"" and stored.payload_ref.startswith("payload/")
    assert sha256(blobs.get(bytes.fromhex(stored.payload_ref.split("/")[1]))) == stored.payload_sha256
```

(Write `test_sightings_span_two_months` in full; `clock` is a fixture that sets `arrived_us`, and the sighting `seen_at` timeuuids come from it.)

- [ ] **Step 2: Verify they fail.**
- [ ] **Step 3: Implement `blobs.py` and `store/arrivals.py`.**
- [ ] **Step 4: Verify they pass** (unit + integration).
- [ ] **Step 5: Commit** `git add -A && git commit -m "Content-addressed blobs, arrivals and duplicate sightings"`

---

### Task 7: Verification

**Files:**
- Create: `src/mailroom_archive/verify/__init__.py`, `verify/chain.py`
- Test: `tests/unit/test_verify_chain.py`

**Interfaces:**
- Consumes: `StoredEvent`, `event_hash`, `sha256`, `canonical_bytes` (Task 1); `BlobStore` (Task 6)
- Produces:
  - `Check(name: str, ok: bool, detail: str | None = None, seq: int | None = None)`; `VerifyReport(doc_id: str, ok: bool, events: int, checks: list[Check])` (pydantic, JSON-serializable)
  - `verify_events(doc_id: str, events: list[StoredEvent], blobs: BlobStore | None) -> VerifyReport`, with checks named `seq_contiguous`, `prev_link`, `payload_hash`, `event_hash`, `payload_available`; one failing `Check` per broken event, and one passing summary check per name when all pass

- [ ] **Step 1: Write the failing tests**

```python
def test_clean_chain_passes():
    assert verify_events("d", chain(5), None).ok

@pytest.mark.parametrize("tamper,check", [
    (lambda evs: evs.pop(2), "seq_contiguous"),
    (lambda evs: setattr_copy(evs, 3, payload=b'{"x":2}'), "payload_hash"),
    (lambda evs: setattr_copy(evs, 3, kind="annotation"), "event_hash"),
    (lambda evs: setattr_copy(evs, 4, prev_hash=b"\x00" * 32), "prev_link"),
])
def test_tamper_detected(tamper, check):
    evs = chain(5); tamper(evs)
    rep = verify_events("d", evs, None)
    assert not rep.ok and any(c.name == check and not c.ok for c in rep.checks)

def test_spilled_payload_checked_against_blob(tmp_path):
    blobs = FsBlobStore(tmp_path); evs = chain(3, spill_seq=2, blobs=blobs)
    assert verify_events("d", evs, blobs).ok
    blob_path(tmp_path, evs[1].payload_ref).unlink()
    rep = verify_events("d", evs, blobs)
    assert not rep.ok and any(c.name == "payload_available" and c.seq == 2 and not c.ok for c in rep.checks)
```

(`chain(n, spill_seq=None, blobs=None)` builds valid `StoredEvent`s, spilling that seq's payload into `blobs`; `setattr_copy` replaces one event with a modified copy; `blob_path` maps a `payload_ref` to its file.)

- [ ] **Step 2: Verify they fail.** **Step 3: Implement.** **Step 4: Verify they pass.**
- [ ] **Step 5: Commit** `git add -A && git commit -m "Chain, payload and spill verification"`

---

### Task 8: API, settings and CLI

**Files:**
- Create: `src/mailroom_archive/settings.py`, `api/__init__.py`, `api/app.py`, `api/auth.py`, `api/routes_events.py`, `api/routes_read.py`, `api/metrics.py`, `src/mailroom_archive/cli.py`
- Test: `tests/unit/test_api.py` (FastAPI `TestClient` with the fake store), `tests/integration/test_api_e2e.py`

**Interfaces:**
- Consumes: `ShardPool`, `Backpressure` (Tasks 4–5), `ArrivalStore`, `FsBlobStore` (Task 6), `verify_events` (Task 7), `TimelineStore` (Task 3)
- Produces:
  - `Settings` (pydantic-settings): `cassandra_hosts: list[str]` (`MAILROOM_ARCHIVE_CASSANDRA_HOSTS`, comma-separated), `keyspace` (`MAILROOM_ARCHIVE_KEYSPACE`, default `archive`), `token`, `read_token`, `data_dir` (`MAILROOM_ARCHIVE_DATA_DIR`, default `./data`; spool at `data_dir/spool.db`, blobs at `data_dir/blobs`), `shards=64`, `queue_size=2000`
  - `create_app(settings: Settings, *, deps: Deps | None = None) -> FastAPI`, where `Deps` bundles store, pool, arrivals, blobs (tests inject fakes)
  - Routes as §7.1:
    - `POST /v1/events`: body is a JSON array or NDJSON; 413 over 1,000 events or 4 MiB; 422 for a client-forbidden kind; 429 with `Retry-After` on `Backpressure`; 200 with `list[WriteResult]`
    - `POST /v1/arrivals`: multipart (`file` plus a `meta` JSON field) or JSON with `content_sha256` only; returns `{arrival_id, duplicate_of}`
    - `PUT /v1/blobs/{sha256}` (hash checked) and `GET /v1/blobs/{sha256}`
    - `GET /v1/docs/{doc_id}`: current state, with overridden fields shown as `{value, overridden_by, pipeline_value}`, where `pipeline_value` comes from the latest `conflict` event for that field
    - `GET /v1/docs/{doc_id}/timeline?after_seq=&limit=` (limit ≤ 1000)
    - `POST /v1/docs/{doc_id}/corrections`: body `{field_path, new_value | release, reason, reviewer, corrects_seq?}`; becomes a `correction` with `origin: "archive"`, `source: "api"`, `source_ref: "api:<uuid4>"` unless the `Idempotency-Key` header is given
    - `POST /v1/docs/{doc_id}/annotations`
    - `GET /v1/content/{sha256}`: blob row plus sightings, each with the linked `doc_id`
    - `GET /v1/matters/{matter_id}/docs?year=`: Q5, each row joined with its current state (Q1)
    - `GET /v1/verify/doc/{doc_id}`: a `VerifyReport`
    - `GET /healthz`: always 200; `GET /readyz`: 200 when a `SELECT now() FROM system.local` at QUORUM on the keyspace succeeds, else 503
    - `GET /metrics`: Prometheus, with the §11 names this slice produces: `archive_events_ingested_total{kind,source,status}`, `archive_ingest_latency_seconds`, `archive_shard_queue_depth{shard}`, `archive_write_retries_total`, `archive_spool_events`, `archive_spool_oldest_seconds`, `archive_cassandra_hosts_up`, `archive_blob_bytes_total`, `archive_duplicates_total{type}`, `archive_conflicts_total`, `archive_verify_failures_total`
  - CLI (`mailroom-archive`): `schema apply --profile single|small|full [--keyspace]`, `serve [--host 127.0.0.1 --port 8544]`, `verify doc <doc_id> [--json]` (exits 1 on failure)
  - Logs: structlog JSON with event names `archive_batch_written`, `archive_spooled`, `archive_conflict`, `archive_verify_failed`, each carrying `doc_id`, `shard`, `source_ref` where they apply

- [ ] **Step 1: Write the failing tests**

```python
def test_requires_token(client):
    assert client.get("/v1/docs/d").status_code == 401
    assert client.get("/healthz").status_code == 200

def test_read_token_cannot_write(client):
    r = client.post("/v1/events", json=[evj("d")], headers=auth("read"))
    assert r.status_code == 403

def test_post_events_then_read(client):
    r = client.post("/v1/events", json=[evj("d", ref="a"), evj("d", ref="b")], headers=auth("write"))
    assert [x["seq"] for x in r.json()] == [1, 2]
    assert client.get("/v1/docs/d", headers=auth("read")).json()["head_seq"] == 2

def test_ndjson_accepted(client):
    body = "\n".join(json.dumps(evj("n", ref=str(i))) for i in range(3))
    r = client.post("/v1/events", content=body, headers={**auth("write"), "content-type": "application/x-ndjson"})
    assert r.status_code == 200 and len(r.json()) == 3

def test_limits_and_forbidden_kind(client):
    assert client.post("/v1/events", json=[evj("d", ref=str(i)) for i in range(1001)], headers=auth("write")).status_code == 413
    assert client.post("/v1/events", json=[evj("d", kind="conflict")], headers=auth("write")).status_code == 422

def test_backpressure_maps_to_429(client_backpressure):
    r = client_backpressure.post("/v1/events", json=[evj("d")], headers=auth("write"))
    assert r.status_code == 429 and "retry-after" in r.headers

def test_correction_override_visible(client):
    client.post("/v1/events", json=[evj("d", "stage_changed", {"to_stage": "classified", "doc_type": "contract"})], headers=auth("write"))
    client.post("/v1/docs/d/corrections", json={"field_path": "doc_type", "new_value": "merger_agreement", "reason": "misfiled", "reviewer": "jb"}, headers=auth("write"))
    client.post("/v1/events", json=[evj("d", "stage_changed", {"to_stage": "archived", "doc_type": "contract"}, ref="p2")], headers=auth("write"))
    dt = client.get("/v1/docs/d", headers=auth("read")).json()["doc_type"]
    assert dt == {"value": "merger_agreement", "overridden_by": 2, "pipeline_value": "contract"}

def test_verify_endpoint(client):
    client.post("/v1/events", json=[evj("v", ref=str(i)) for i in range(3)], headers=auth("write"))
    rep = client.get("/v1/verify/doc/v", headers=auth("read")).json()
    assert rep["ok"] and rep["events"] == 3
```

`test_api_e2e.py` (integration) starts the app against Cassandra with `httpx.AsyncClient` and
covers: post 200 events across 20 documents, upload an arrival with a file, post a duplicate
arrival (`duplicate_of` set), run a correction and a conflicting pipeline update, then check that
`verify` passes for all 20 documents and `GET /v1/content/{sha}` shows both sightings.

- [ ] **Step 2: Verify they fail.** **Step 3: Implement.** **Step 4: Verify they pass** (unit + integration). Also run `uv run mailroom-archive --help` and check that the three commands are listed.
- [ ] **Step 5: Commit** `git add -A && git commit -m "Ingest, read, correction and verify API; CLI; metrics"`

---

### Task 9: One node down, and CI

**Files:**
- Create: `tests/fault/test_node_down.py`, `.github/workflows/ci.yml`, `docs/running.md`
- Modify: `README.md` (quick start: `docker compose -f ops/compose.yml --profile single up -d --wait`, `uv run mailroom-archive schema apply --profile single`, `uv run mailroom-archive serve`)

**Interfaces:**
- Consumes: the whole stack (Tasks 3–8)

- [ ] **Step 1: Write the fault test** (marker `fault`; requires `ARCHIVE_PROFILE=small` and Docker)

```python
async def test_writes_and_reads_survive_one_node_down(api, docker):
    docs = [f"nd-{i}" for i in range(30)]
    await post_events(api, docs, per_doc=5, ref_prefix="before")
    docker.kill("mailroom-archive-cassandra-2-1"); await asyncio.sleep(5)
    try:
        res = await post_events(api, docs, per_doc=5, ref_prefix="during")
        assert {r["status"] for r in res} == {"written"}          # QUORUM still met, nothing spooled
        for d in docs:
            assert (await api.get(f"/v1/verify/doc/{d}")).json()["ok"]
            assert (await api.get(f"/v1/docs/{d}")).json()["head_seq"] == 10
    finally:
        docker.start("mailroom-archive-cassandra-2-1")

async def test_two_nodes_down_spools_then_drains(api, docker):
    docker.kill("mailroom-archive-cassandra-2-1"); docker.kill("mailroom-archive-cassandra-3-1")
    try:
        res = await post_events(api, ["sp-1"], per_doc=5, ref_prefix="x")
        assert {r["status"] for r in res} == {"spooled"}
    finally:
        docker.start("mailroom-archive-cassandra-2-1"); docker.start("mailroom-archive-cassandra-3-1")
    await wait_until(lambda: api.get("/v1/docs/sp-1"), lambda r: r.json().get("head_seq") == 5, timeout=180)
    assert (await api.get("/v1/verify/doc/sp-1")).json()["ok"]
```

- [ ] **Step 2: Run it on the 3-node profile**

Run: `scripts/vm.sh sync && scripts/vm.sh down && scripts/vm.sh up small && scripts/vm.sh run "~/.local/bin/uv run mailroom-archive schema apply --profile small && ARCHIVE_PROFILE=small CASSANDRA_HOSTS=127.0.0.1 ~/.local/bin/uv run pytest tests/fault -m fault -v"`
Expected: both PASS, and `vm.sh` reports every other project's containers still running. Afterwards: `scripts/vm.sh down`.

- [ ] **Step 3: Write CI** (`.github/workflows/ci.yml`): on push and PR, run `uv sync`, `ruff check`, then `pytest tests/unit`. A second job runs the `single` profile with the Cassandra service container `cassandra:5.0` and `pytest -m integration`. The fault test is manual (`workflow_dispatch`), because hosted runners are too small for three nodes.

- [ ] **Step 4: Write `docs/running.md`** (profiles, env vars from `Settings`, tokens, the trust-zone requirements from spec §3: encrypted volumes, TLS outside `single`, Cassandra auth).

- [ ] **Step 5: Run everything once more**

Run: `uv run ruff check . && uv run pytest tests/unit -v && scripts/vm.sh sync && scripts/vm.sh up single && scripts/vm.sh run CASSANDRA_HOSTS=127.0.0.1 ~/.local/bin/uv run pytest -m integration -v && scripts/vm.sh down`
Expected: all PASS; other projects' containers still running.

- [ ] **Step 6: Commit and push**

```bash
git add -A && git commit -m "Node-failure tests, CI and running guide"
git push origin main
```

---

### Task 10: Pre-publication check and going public

- [ ] **Step 1: Scan the tree and the full history**

```bash
git log --all -p | grep -i -n -E -f .publish-denylist | head
git log --all --format='%an <%ae>' | sort -u
grep -rIl -E "(AKIA|ghp_|gho_|-----BEGIN [A-Z ]*PRIVATE KEY)" --exclude-dir=.git . ; echo "secrets_exit=$?"
```

Expected: no matches; the only author is the owner's noreply identity; `secrets_exit=1`.

- [ ] **Step 2: Ask the owner to confirm going public** (show the scan output and the author identity). Do not continue without a yes.

- [ ] **Step 3: Make it public**

Switch the repository to public with the GitHub CLI and read the visibility back.

Expected: `PUBLIC`.
