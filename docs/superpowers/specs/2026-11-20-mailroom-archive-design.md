# mailroom-archive: content-hash archive, ingest and load simulator

- **Date:** 2026-11-20
- **Status:** draft, reviewed by a builder check, awaiting owner approval
- **Repo:** `Exios66/mailroom-archive` (private until the fresh history below lands, then public)
- **Sits beside:** `llm-mailroom` (core pipeline, 0.9.3 vendored in `local-mailroom-sandbox`;
  upstream `main` has the unified audit row and trace sinks of the 2026-11-03 spec)

## 1. Intent

The mailroom pipeline classifies, extracts and archives legal, insurance and corporate mail.
What it remembers today lives in one SQLite (or Postgres) file: the `documents` catalog, a
hash-chained `audit_log`, trace I/O, and a daily Parquet warehouse. That is fine for one box
and a few thousand documents. It is not built for:

- very fast writes of traces and logs from many documents at once;
- accurate corrections and status changes after the fact, without losing what was there
  before;
- surviving a storage node going down;
- handing the full history to analysis tools without a slow, bespoke export;
- proving all of that under the volume a real mailroom sees.

mailroom-archive is a separate service that keeps **every document's arrival, content hash,
status history, traces and audit records** in a replicated store built for heavy write
traffic, keeps them verifiable, and streams them into columnar files for analysis. A
simulator drives it (and, at low rate, the real pipeline) with realistic mail and filing
traffic.

The storage design uses a well-known Cassandra pattern: one partition
per entity, the entity's current facts in static columns, its history as clustered rows
under it, three replicas, and quorum reads and writes so one node can fail. The code here is
new and written for documents.

## 2. Goals, non-goals, anti-goals

### Goals

1. **Record everything the mailroom records**, from outside the pipeline:
   arrivals (with SHA-256 of the bytes), the catalog's stage changes, unified audit rows,
   their attempts and addenda, node visits, trace I/O (when full trace is on), and pending
   rows that failed to commit (§5.6 of the 2026-11-03 spec).
2. **Append-only with accurate edits.** Corrections, status changes and annotations are new
   events. Current state is updated alongside, atomically, in the same partition. Nothing a
   reader saw is ever overwritten in place.
3. **Tamper evidence.** Every event is hash-chained within its document; every hour of
   ingest is sealed with a Merkle root; upstream audit hashes are stored verbatim and
   re-verified.
4. **Fault tolerance.** Three replicas, quorum consistency, a local spool when the cluster
   is unreachable, at-least-once intake with idempotent writes.
5. **Observability and auditability.** Metrics, structured logs, optional OpenTelemetry
   spans, a verification CLI and API, and dashboards.
6. **OLAP in one step.** Closed time buckets land in Parquet within five minutes;
   `mailroom-archive analyze` opens DuckDB with ready-made views.
7. **Bounded resources.** Content-addressed blob dedup, compression, TTLs on bulky
   short-lived data, optional tiering of old timelines to Parquet, a small-footprint profile.
8. **A simulator in two modes:** synthetic event load at high rate, and real files through
   the real pipeline at low rate, both with ground truth the archive is checked against.

### Non-goals

- Re-implementing classification, extraction, judging or review. The 13-node graph stays in
  llm-mailroom. The archive records what it did.
- Replacing the pipeline's own SQLite/Postgres storage. The pipeline keeps working with the
  archive switched off.
- A review UI. The pipeline (and The-Mailroom) own that; the archive serves reads.
- Multi-region deployment. The keyspace strategy allows it later (§10).

### Anti-goals (failure even if it "works")

- The pipeline slows down, fails a node, or loses a document because the archive is slow or
  down.
- An acknowledged event is lost after one node fails.
- Document text or personal data reaches anyone outside the pipeline's trust zone, or is
  stored unencrypted or readable without authentication (§3, privacy).
- A correction silently replaces history.
- Any file or commit in the public repo that reproduces third-party code.

## 3. Constraints

- **Pipeline untouched.** Integration only through llm-mailroom's published seams: the
  `mailroom.trace_sinks` entry point and read-only access to its database and inbox.
- **Trace-sink contract** (upstream `src/audit/sinks/__init__.py`): `name`, `record_io`,
  `capture_pending`, `fetch_pending`, `fetch_io`; optional `recoverable`, `off_box`,
  `max_payload_bytes`. A sink that sends data off the process **must** set `off_box = True`,
  so it receives I/O and pending rows **only when `audit.full_trace` is on**. A failing sink
  is skipped and never fails a node. This is why the sink cannot be the only way in.
- **Upstream hash format.** Hash version 2: SHA-256 of canonical JSON (`sort_keys`) over
  `hash_version, prev_hash, doc_id, entry_id, matter_id, actor, timestamp, event, detail`,
  byte-identical with the upstream pipeline's audit-entry formatter.
- **Document classes:** `contract`, `merger_agreement`, `corporate_record`, `correspondence`,
  `insurance_claim` (taxonomy in `config/mailroom.taxonomy.base.yaml`). Pipeline stages:
  `inbox, processing, classified, review, failed, archived`. Pipeline channels today:
  `upload`, `gmail`.
- **Hardware:** must run on an 8 GB VM or laptop with 3 storage nodes, and scale out by
  adding nodes, not by rewriting.
- **Stack continuity with the mailroom:** Python 3.12, uv, FastAPI, structlog, Typer,
  pyarrow, DuckDB.
- **Privacy: the archive is inside the pipeline's trust zone** (owner's decision). It holds
  full copies (document bytes, senders, subjects, extracted fields), so:
  - it runs only on machines the owner controls; blob stores and warehouse roots are on
    encrypted volumes or encrypted buckets (server-side encryption required for S3);
  - Cassandra data directories are on encrypted volumes (open-source Cassandra has no
    built-in encryption at rest); client-to-node and node-to-node TLS are on outside the
    single-machine development profile;
  - every API route, reads included, needs a token; Cassandra authentication is on;
  - the ArchiveSink still declares `off_box = True`, as upstream's sink contract requires of
    any sink that sends data out of the process. It therefore receives trace I/O only when
    `audit.full_trace` is on; the follower and probe are the archive's own trust-zone copies.
- **Public repo hygiene:** see §12.

## 4. Approaches considered

| | A. Cassandra hot store + Parquet/DuckDB (**picked**) | B. Postgres, partitioned tables | C. ClickHouse for everything |
|---|---|---|---|
| Write path | Very fast, spread across nodes, no single leader | Fast on one node; one primary takes every write | Very fast for batched inserts |
| One node down | Keeps reading and writing at QUORUM with RF=3 | Needs a replica and failover tooling (Patroni etc.) | Needs Keeper plus replicated tables |
| Edits / status changes | Clustered rows + static current state in one partition, atomic per document | Natural (UPDATE + history table) | Awkward: mutations are heavy; needs ReplacingMergeTree tricks |
| OLAP | Export to Parquet, DuckDB on top | Decent with indexes; heavy scans compete with writes | Excellent natively |
| Resource use | Heaviest (JVM per node); tunable | Lightest | Moderate |
| Fit with the proven pattern | Exact | Partial | Poor |

**Why A.** It matches the requirements that matter most: write volume, surviving a node,
and per-document current state plus full history in one place. Its weakness, analysis, is
covered by a columnar export that DuckDB reads directly. B would be simpler but makes
failover an operations project and puts analysis load on the write primary. C is the best
analytics engine but fights the correction/status model and its replication is heavier to
run on small boxes.

**Engine variant.** ScyllaDB speaks the same protocol and needs much less memory. The
design avoids features Scylla lacks except compaction, which `schema apply` picks per
profile (Scylla has no `UnifiedCompactionStrategy`; it gets `IncrementalCompactionStrategy`
or leveled), so a Scylla profile needs no application code changes (§10).

## 5. Architecture

```
                       ┌──────────────────────────────────────────────────────────┐
                       │                    llm-mailroom (unchanged)               │
   mail / filings ───► │ inbox/ ─► watcher ─► 13-node graph ─► archive bins         │
   (or simulator B)    │                         │                                  │
                       │           mailroom.db: documents, audit_log, attempts,     │
                       │           addenda, audit_trace_io      trace sinks ──┐     │
                       └──────────┬──────────────────────┬────────────────────┼─────┘
                                  │ read-only            │ inbox watch        │ entry point
                                  ▼                      ▼                    ▼ "archive"
                          ┌──────────────┐        ┌─────────────┐      ┌──────────────┐
                          │   follower   │        │ inbox probe │      │ ArchiveSink  │
                          └──────┬───────┘        └──────┬──────┘      └──────┬───────┘
                                 │ event batches         │                    │
  simulator A (synthetic) ───────┴──────────────┬────────┴────────────────────┘
                                                ▼
                                   ┌─────────────────────────┐   spool (SQLite WAL)
                                   │   ingest API (FastAPI)  │◄─────────────────┐
                                   │ shard router (doc_id)   │──── on outage ───┘
                                   │ single writer per shard │
                                   └───────────┬─────────────┘
                                               │ CQL, QUORUM
                                  ┌────────────┴────────────┐       blob store (content
                                  │ Cassandra 5 ×3, RF=3    │       addressed: fs or S3)
                                  └────────────┬────────────┘
                              ┌────────────────┼──────────────────┐
                              ▼                ▼                  ▼
                        ┌──────────┐    ┌────────────┐     ┌────────────┐
                        │ exporter │    │   sealer   │     │ read API / │
                        │→ Parquet │    │ Merkle/hr  │     │ verify CLI │
                        └────┬─────┘    └────────────┘     └────────────┘
                             ▼
                  warehouse/ (Parquet, zstd) ──► DuckDB `mailroom-archive analyze`
```

### 5.1 Components

| Component | What it does |
|---|---|
| **ingest API** | Accepts event batches and blob uploads, assigns per-document sequence numbers, computes event hashes, writes to Cassandra, falls back to the spool. Also serves reads and verification. |
| **shard router** | Hashes `doc_id` into N shards (default 64). Each shard has one writer task, so a document's chain head is never contended and needs no lightweight transactions. With several API replicas, shards are leased (§7.3). |
| **follower** | Tails the mailroom database read-only and turns new rows into events. The primary integration. |
| **inbox probe** | Watches the mailroom inbox and records an `arrived` event with the file's SHA-256 the moment a file lands, before the pipeline moves it. |
| **ArchiveSink** | A `mailroom.trace_sinks` plugin. Receives trace I/O and pending rows when full trace is on; reads them back for `audit-reconcile`. |
| **blob store** | Content-addressed storage of document bytes and oversize payloads: `blobs/ab/cd/<sha256>`. Local filesystem by default, any S3-compatible store optionally. |
| **exporter** | Moves closed time buckets from Cassandra to Parquet every five minutes; daily snapshot of current document state. |
| **sealer** | After each hour closes, computes a Merkle root over that hour's event hashes per shard and chains the seals. |
| **verify** | Recomputes per-document chains, upstream audit hashes, payload hashes and seals. CLI and API. |
| **simulator** | Two modes (§9). |

### 5.2 Data flow for one document

1. File lands in `inbox/`. Inbox probe hashes it and records an **arrival** (sha256, size,
   channel, sidecar metadata) with its own `arrival_id`, since the pipeline has not yet given
   it a `doc_id`. The blob is stored once by hash; the sighting is recorded. When the
   pipeline's `documents` row appears, the follower links the arrival to it and writes an
   `arrived` event into that document's timeline.
2. Pipeline creates the catalog row and runs the graph. The follower sees `documents` change
   and posts `stage_changed` events; sees the unified `audit_log` row, its `audit_attempts`
   and later `audit_addenda`, and posts `audit_row`, `audit_attempt`, `audit_addendum`
   carrying the upstream record verbatim.
3. With full trace on, every node's prompt and completion also arrive through the sink as
   `trace_io`.
4. A reviewer correction (in the pipeline, or through the archive's correction endpoint)
   arrives as `correction`, pointing at the event it corrects. The document's static state
   updates in the same write.
5. The exporter writes the hour's events to Parquet; the sealer seals the hour.

## 6. Data model

### 6.1 Identity and hashing

- `doc_id`: the pipeline's document id, stored as CQL `text` exactly as upstream has it
  (upstream allows up to 128 characters: UUIDs, and ids like `fixture-doc-a` or `syn-…`).
  Simulator documents use UUIDv7 strings so they sort by creation time.
- `arrival_id`: UUIDv7 text assigned by whoever records a file arriving (inbox probe,
  simulator). An arrival exists before the pipeline gives the file a `doc_id`, and some
  arrivals never get one (re-sent duplicates the pipeline skips). See §6.4 `arrivals`.
- `event_id`: `timeuuid`, assigned when the event is written to Cassandra (ingest time).
- `seq`: per-document integer, 1, 2, 3, ..., assigned by the shard writer.
- `occurred_us`: when the event happened, as `bigint` microseconds since the epoch. CQL
  `timestamp` only keeps milliseconds, so the hashed value is stored as is.
- Hashes are stored as 32-byte `blob`, never hex text (half the bytes, faster compares).
  APIs show lowercase hex.
- **Event hash (`hash_version = "a1"`)**:
  `event_hash = SHA-256(canonical_json({hash_version, doc_id, seq, kind, occurred_us,
  actor, source, source_ref, payload_sha256, prev_hash}))`, where canonical JSON is
  `sort_keys=True, separators=(",", ":"), ensure_ascii=False`, `occurred_us` as an integer,
  and hashes as lowercase hex. `prev_hash` is the previous event's hash for the same
  document, or `""` for `seq = 1`.
- The payload is hashed separately (`payload_sha256` over its canonical JSON bytes). This
  lets a large payload move to the blob store without breaking the chain.
- Upstream audit records keep their own `entry_hash` and `prev_hash`; the archive stores
  them inside the payload and verifies them with the hash version 2 function (ported, with a
  parity test against the upstream pipeline's audit-entry formatter).

### 6.2 Event kinds

| kind | Source | Payload (canonical JSON) |
|---|---|---|
| `arrived` | follower when it links an arrival (§6.4), simulator | `content_sha256, size_bytes, mime, channel, original_filename, sender, subject, received_at, intake_meta, blob_ref` |
| `stage_changed` | follower (`documents`), simulator | `from_stage, to_stage, doc_type, doc_subclass, classification_confidence, extraction_confidence, escalation_reason, review_decision` |
| `node_visit` | follower (row `detail.trace[]`), simulator | one visit record as in spec §4.4, without prompt/completion |
| `trace_io` | ArchiveSink | `visit_id, io_sha256, io_ref` (bytes live in `trace_io`) |
| `audit_row` | follower | the upstream unified row, verbatim |
| `audit_attempt` | follower | one upstream attempt sub-row, verbatim |
| `audit_addendum` | follower | one upstream addendum, verbatim |
| `pending_row` | ArchiveSink | the spec §5.6 pending record |
| `correction` | API, follower | `corrects_seq, field_path, old_value, new_value, reason, reviewer, origin: archive or pipeline, release: bool` |
| `conflict` | shard writer | `field_path, override_seq, archive_value, pipeline_value, pipeline_source_ref` |
| `annotation` | API | `note, tags, author` |
| `relation` | follower, API | `related_doc_id, relation, confidence` |
| `retention` | tiering job | `tiered_to, parquet_files, events_tiered, last_seq` |

The kind set is closed and versioned; unknown kinds are rejected with 422.

### 6.3 Keyspaces

- `archive` for real traffic, `archive_sim` for simulator runs, same tables. Separate keyspaces
  keep test load from polluting real history and let a simulation be dropped in one statement.
- Development: `SimpleStrategy`, `replication_factor: 3`. Production:
  `NetworkTopologyStrategy {'dc1': 3}`.

### 6.4 Tables (query first)

Each table exists to answer named queries (§6.6). Sizes assume compressed storage.

#### `doc_timeline`: one partition per document

The core table. Static columns are the document's current state; clustered rows are its
complete, ordered history.

```sql
CREATE TABLE archive.doc_timeline (
    doc_id            text,
    seq               int,
    -- current state (static: one copy per partition)
    matter_id         text      STATIC,
    content_sha256    blob      STATIC,
    channel           text      STATIC,
    original_filename text      STATIC,
    doc_type          text      STATIC,
    doc_subclass      text      STATIC,
    stage             text      STATIC,
    stage_at          timestamp STATIC,
    head_seq          int       STATIC,
    head_hash         blob      STATIC,
    audit_entry_hash  blob      STATIC,   -- upstream unified row's entry_hash, once known
    extracted_data    blob      STATIC,   -- current extracted fields, canonical JSON
    overrides         map<text, int> STATIC,  -- field_path -> seq of the archive correction in force
    created_at        timestamp STATIC,
    sim_run_id        text      STATIC,
    -- history
    event_id          timeuuid,
    kind              text,
    actor             text,
    source            text,
    source_ref        text,
    occurred_us       bigint,    -- microseconds since epoch; the hashed value
    payload           blob,      -- canonical JSON bytes, or empty when spilled
    payload_ref       text,      -- blob store key when payload > 16 KiB
    payload_sha256    blob,
    prev_hash         blob,
    event_hash        blob,
    corrects_seq      int,
    PRIMARY KEY ((doc_id), seq)
) WITH CLUSTERING ORDER BY (seq ASC)
  AND compaction  = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L4'}
  AND compression = {'class': 'ZstdCompressor', 'compression_level': 3};
```

- One write per event: a single-partition `BATCH` that inserts the row and sets the static
  columns. Single-partition batches are atomic and isolated, so readers never see a new event
  without the updated state, or the reverse.
- Expected size: 20 to 200 events per document, about 300 bytes each compressed, so well
  under 100 KiB per partition. Payloads over 16 KiB spill to the blob store.
- Leveled-style compaction (UCS `L4`) because documents are read by key and updated over
  days.

#### `arrivals`: one row per file that arrived, before and regardless of a `doc_id`

```sql
CREATE TABLE archive.arrivals (
    arrival_id        text PRIMARY KEY,   -- UUIDv7
    content_sha256    blob,
    size_bytes        bigint,
    mime              text,
    channel           text,
    original_filename text,
    intake_meta       blob,               -- canonical JSON of the sidecar (upload_id, message_id, ...)
    arrived_us        bigint,
    doc_id            text,               -- set when the follower links it; null for skipped duplicates
    linked_us         bigint,
    sim_run_id        text
);
```

- The inbox probe (or simulator) writes the arrival at once, with no `doc_id`.
- The follower links it when the pipeline's `documents` row appears. It matches on
  `file_sha256`, then `upload_id` or `message_id` from the sidecar, then `original_filename`,
  taking the earliest unlinked arrival. When it links, it sets `doc_id` here and writes an
  `arrived` event (payload: the arrival, plus `arrival_id`) to the document's timeline. That
  event may not be `seq = 1` if the follower saw other events first; ordering in the timeline
  is by `seq`, and `arrived_us` keeps the true time.
- Two identical files arriving together are two arrivals. Each links to its own document, or
  stays unlinked if the pipeline skipped it as already processed; an unlinked arrival older
  than `MAILROOM_ARCHIVE_LINK_TIMEOUT` (default 1 hour) is reported, not deleted.

#### `sightings_by_sha`: every time the same bytes arrived

```sql
CREATE TABLE archive.sightings_by_sha (
    content_sha256  blob,
    month           int,        -- yyyymm, bounds partitions of very common files
    seen_at         timeuuid,
    arrival_id      text,
    channel         text,
    original_filename text,
    PRIMARY KEY ((content_sha256, month), seen_at)
) WITH CLUSTERING ORDER BY (seen_at DESC)
  AND compression = {'class': 'LZ4Compressor'};
```

Duplicate detection on arrival: two reads, this month and last (merged in the app, so one
month cannot crowd out the other). Exact duplicates are flagged in the arrival's
`intake_meta` as `duplicate_of` (the earlier `arrival_id`). The linked `doc_id` is read from
`arrivals`.

#### `blobs`: one row per distinct content

```sql
CREATE TABLE archive.blobs (
    content_sha256 blob PRIMARY KEY,
    size_bytes     bigint,
    mime           text,
    storage_uri    text,
    first_seen     timestamp,
    verified_at    timestamp
);
```

Bytes are never stored in Cassandra. Writes use `INSERT ... IF NOT EXISTS` only on this
table (low rate: once per distinct file); a lost race is harmless because the content is
identical.

#### `events_by_time`: the ingest feed

```sql
CREATE TABLE archive.events_by_time (
    bucket      text,       -- 'YYYYMMDDHH' UTC, from event_id (ingest time)
    shard       tinyint,    -- 0..15, hash(doc_id) % 16
    event_id    timeuuid,
    doc_id      text,
    seq         int,
    occurred_us bigint,
    kind        text,
    channel     text,
    doc_type    text,
    stage       text,
    event_hash  blob,
    sim_run_id  text,
    PRIMARY KEY ((bucket, shard), event_id)
) WITH CLUSTERING ORDER BY (event_id ASC)
  AND default_time_to_live = 1209600            -- 14 days; Parquet keeps it after
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
  AND compression = {'class': 'LZ4Compressor'};
```

It does not share a partition with `doc_timeline`, so it is a second write issued right
after Q8 (a multi-partition logged batch would cost more than it saves). If it fails,
the shard writer retries it; the exporter also cross-checks counts against
`doc_timeline.head_seq` (§7.5). This table drives exports, seals and live dashboards, and
expires on its own, so it never grows unbounded.

#### `trace_io`: prompt and completion text

```sql
CREATE TABLE archive.trace_io (
    visit_id    text PRIMARY KEY,
    doc_id      text,
    prompt      text,
    completion  text,
    io_sha256   blob,
    spill_ref   text,       -- blob key when prompt+completion > 64 KiB
    created_at  timestamp
) WITH default_time_to_live = 2592000           -- 30 days hot; Parquet keeps it after
  AND compaction  = {'class': 'TimeWindowCompactionStrategy',
                     'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
  AND compression = {'class': 'ZstdCompressor', 'compression_level': 6};
```

Answers `fetch_io(ref)` for 30 days. The highest-volume, most compressible data, so the
strongest compression and a TTL. Trace I/O is always exported to Parquet, and `fetch_io`
falls back to the warehouse after expiry, so `recoverable = True` holds for good.

#### `pending_rows`: rows the pipeline could not commit

```sql
CREATE TABLE archive.pending_rows (
    shard          tinyint,     -- 0..15
    captured_at    timeuuid,
    doc_id         text,
    detail_sha256  blob,
    complete       boolean,
    record         blob,        -- canonical JSON of the pending record
    PRIMARY KEY ((shard), captured_at)
);
```

`fetch_pending()` reads the 16 partitions. Rows are removed when the follower later sees the
reconciled row land in `audit_log` (it matches on `doc_id` and `detail_sha256`). Volume is
tiny, so the tombstones this leaves are not a concern.

#### `docs_by_matter`

```sql
CREATE TABLE archive.docs_by_matter (
    matter_id   text,
    year        smallint,
    created_at  timestamp,
    doc_id      text,
    doc_type    text,
    content_sha256 blob,
    PRIMARY KEY ((matter_id, year), created_at, doc_id)
) WITH CLUSTERING ORDER BY (created_at DESC, doc_id ASC);
```

Current stage is read from `doc_timeline` statics (one key lookup per document), so this
index never has to be rewritten on stage change.

#### `idempotency`

```sql
CREATE TABLE archive.idempotency (
    source      text,
    source_ref  text,
    doc_id      text,
    seq         int,
    event_hash  blob,
    PRIMARY KEY ((source, source_ref))
) WITH default_time_to_live = 7776000;          -- 90 days
```

`source_ref` is the upstream identity (`audit_log:<entry_id>`, `documents:<doc_id>:<updated_at>`,
`sink:<visit_id>`, `sim:<run_id>:<n>`). Only the shard writer that owns the document reads
and writes it, so a read before write is race free without lightweight transactions.

**Order matters.** The writer writes the idempotency row (planned `seq`, `event_hash`)
**before** Q8. On redelivery it finds the row and re-issues Q8 with the identical values,
which is a harmless upsert if Q8 had landed and completes it if the writer crashed in
between. Within 90 days, redelivery always returns the original `seq` and `event_hash`.

#### `seals`

```sql
CREATE TABLE archive.seals (
    day          int,        -- yyyymmdd
    bucket       text,       -- 'YYYYMMDDHH'
    shard        tinyint,
    seal_no      int,        -- 0 = the hour seal; 1, 2, ... = late seals
    event_count  int,
    first_event  timeuuid,
    last_event   timeuuid,
    leaf_ids     list<timeuuid>,   -- only on late seals: exactly which events they cover
    merkle_root  blob,
    prev_seal    blob,       -- previous seal's hash for this shard
    seal_hash    blob,
    sealed_at    timestamp,
    PRIMARY KEY ((day), bucket, shard, seal_no)
);
```

- `event_id` is minted immediately before the write, so a bucket is the ingest hour, and
  spool replays and backfills land in the hour they are written, not the hour they happened.
- The sealer seals an hour 10 minutes after it closes. Merkle leaves are `event_hash` values
  in `event_id` order, minus any ids already covered by earlier seals of that hour.
- Anything that becomes visible in a sealed hour later (a write that was in flight at the
  boundary) goes into the next late seal (`seal_no` 1, 2, ...), which lists its `leaf_ids`.
  Verification of the hour seal recomputes over the hour's events minus every late seal's
  `leaf_ids`. No seal is ever rewritten. Seals chain per shard.

#### `cursors` and `sim_runs`

```sql
CREATE TABLE archive.cursors (
    name        text PRIMARY KEY,   -- 'follower.audit_log', 'exporter.events', ...
    position    text,
    updated_at  timestamp
);

CREATE TABLE archive.sim_runs (
    run_id      text PRIMARY KEY,
    scenario    text,
    seed        bigint,
    config      text,              -- the scenario YAML, as run
    started_at  timestamp,
    ended_at    timestamp,
    stats       map<text, double>
);
```

### 6.5 Consistency

- Session default `QUORUM` for reads and writes (`LOCAL_QUORUM` under
  `NetworkTopologyStrategy`). With RF=3, two replicas must answer, so read + write > RF: a
  read sees every write Cassandra acknowledged, and any one node can be down. An event
  acknowledged as `spooled` (§7.4) is durable but not readable until the spool drains.
- No lightweight transactions on hot tables in the default single-instance mode.
  Correctness of per-document `seq` and `prev_hash` comes from the single-writer-per-shard
  rule. Multi-instance mode (§7.3) switches the static update in Q8 to a conditional one.
- Prepared statements for every query, prepared once per keyspace at startup.

### 6.5a Corrections and who wins

The archive can override the pipeline (owner's decision).

- A `correction` from the archive API (`origin: archive`) sets the field in current state
  (`doc_type`, `doc_subclass`, `matter_id`, or a path inside `extracted_data`) and records
  `overrides[field_path] = seq`, in the same Q8 batch.
- A correction or update arriving from the pipeline (`origin: pipeline`, from the follower)
  for a field with no override updates current state normally.
- When the pipeline reports a value for an **overridden** field that differs from the
  override, current state keeps the archive's value and the writer appends a `conflict` event
  holding both values. The read API shows the field as `{value, overridden_by, pipeline_value}`,
  and `archive_conflicts_total` counts it.
- A correction with `release: true` removes the override; the latest pipeline value becomes
  current again. Every step is an event, so the full history of who said what stays in the
  chain.
- Corrections never flow back into the pipeline. Pushing them upstream is a follow-up that
  needs a pipeline API (§15).

### 6.6 Hot-path queries (CQL)

```sql
-- Q1 current state of a document (statics only; LIMIT 1 avoids reading history)
SELECT doc_id, matter_id, content_sha256, channel, doc_type, doc_subclass,
       stage, stage_at, head_seq, head_hash, audit_entry_hash, extracted_data, overrides
FROM archive.doc_timeline WHERE doc_id = ? LIMIT 1;

-- Q2 full timeline, oldest first (verification, UI)
SELECT seq, event_id, kind, actor, source, occurred_us, payload, payload_ref,
       payload_sha256, prev_hash, event_hash, corrects_seq
FROM archive.doc_timeline WHERE doc_id = ?;

-- Q3 timeline page after a known seq
SELECT seq, kind, occurred_us, payload, event_hash
FROM archive.doc_timeline WHERE doc_id = ? AND seq > ? LIMIT 200;

-- Q4 has this exact content arrived before? Run once for this month and once for last,
-- merge newest first in the app (an IN across partitions returns one partition first).
SELECT seen_at, arrival_id, channel, original_filename
FROM archive.sightings_by_sha WHERE content_sha256 = ? AND month = ? LIMIT 50;

-- Q5 documents in a matter this year, newest first
SELECT created_at, doc_id, doc_type FROM archive.docs_by_matter
WHERE matter_id = ? AND year = ? LIMIT 500;

-- Q6 one ingest-feed partition (exporter, sealer, live dashboard), from a cursor
SELECT event_id, doc_id, seq, kind, channel, doc_type, stage, event_hash
FROM archive.events_by_time WHERE bucket = ? AND shard = ? AND event_id > ?;

-- Q7 fetch_io
SELECT prompt, completion, io_sha256, spill_ref FROM archive.trace_io WHERE visit_id = ?;

-- Q8 write one event (single-partition batch: history row + current state, atomic)
BEGIN BATCH
  INSERT INTO archive.doc_timeline (doc_id, seq, event_id, kind, actor, source, source_ref,
         occurred_us, payload, payload_ref, payload_sha256, prev_hash, event_hash, corrects_seq)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
  UPDATE archive.doc_timeline SET stage = ?, stage_at = ?, head_seq = ?, head_hash = ?
  WHERE doc_id = ?;
APPLY BATCH;
```

Q8's `UPDATE` sets only the static columns the event changes (for example `stage` only on
`stage_changed`). No query uses `ALLOW FILTERING` or a secondary index.

## 7. Write path and fault tolerance

### 7.1 Ingest API

| Method and path | Purpose |
|---|---|
| `POST /v1/events` | NDJSON or JSON array, up to 1,000 events or 4 MiB. Returns per event `{doc_id, status: written, duplicate or spooled}`, plus `seq` and `event_hash` unless spooled. |
| `POST /v1/arrivals` | Record a file arrival (§6.4 `arrivals`); returns `arrival_id` and any `duplicate_of`. |
| `PUT /v1/blobs/{sha256}` | Upload bytes; the server checks the hash. Idempotent. |
| `GET /v1/blobs/{sha256}` | Download bytes. |
| `GET /v1/docs/{doc_id}` | Current state (Q1). |
| `GET /v1/docs/{doc_id}/timeline?after_seq=&limit=` | History (Q2/Q3). |
| `POST /v1/docs/{doc_id}/corrections` | Add a `correction` (requires `reason`, `reviewer`). |
| `POST /v1/docs/{doc_id}/annotations` | Add an `annotation`. |
| `GET /v1/content/{sha256}` | Blob metadata plus sightings (Q4). |
| `GET /v1/matters/{matter_id}/docs?year=` | Q5 joined with Q1. |
| `GET /v1/verify/doc/{doc_id}` | Recompute the chain, payload hashes and upstream hashes. |
| `GET /v1/verify/seal/{bucket}` | Recompute the hour's Merkle roots. |
| `GET /healthz`, `GET /readyz` | Liveness; readiness includes "QUORUM reachable". |
| `GET /metrics` | Prometheus. |

Every route except `/healthz` needs a token: a write token (`MAILROOM_ARCHIVE_TOKEN`) for
writes and corrections, a read token for everything else (§3, privacy).

### 7.2 Shard writer

- `shard = xxhash64(doc_id) % N` (N = 64). Each shard is an asyncio task with a bounded queue
  (default 2,000 events).
- Per event: check `idempotency` (a hit re-issues the recorded write, §6.4), read the chain
  head (in-memory LRU cache, else Q1), assign `seq = head_seq + 1`, compute hashes, write the
  `idempotency` row, run Q8, write `events_by_time` and any side tables (`docs_by_matter`,
  `trace_io`), then update the cache.
- Batches from one request are split by shard and processed concurrently across shards,
  sequentially within a shard, preserving per-document order.
- **Back-pressure:** when a shard queue is full, the API returns 429 with `Retry-After`;
  clients (follower, simulator, sink) retry with jittered exponential backoff.

### 7.3 More than one API instance

One instance owning all 64 shards is the default and needs no coordination. When more are
needed:

- Shards are leased through a `shard_leases` table with `INSERT ... IF NOT EXISTS USING TTL 30`,
  renewed every 10 seconds. An instance forwards events for shards it does not own.
- A lease alone cannot stop a paused former owner from writing the same `(doc_id, seq)`, and
  Q8 is an upsert, so history could be silently overwritten. Multi-instance mode therefore
  makes Q8's static update conditional: `UPDATE ... SET head_seq = ?, ... IF head_seq = ?`
  (with the row insert in the same conditional batch). Every write to `doc_timeline` then goes
  through Paxos, so lightweight and plain writes are never mixed on a partition. A failed
  condition makes the writer reload the head and retry, or give up the shard if its lease is
  gone. This costs throughput, which is why it is off in single-instance mode.

### 7.4 Spool

- When Cassandra writes fail after 3 attempts (100 ms, 400 ms, 1.6 s), the shard writer
  appends the event **unsequenced** to a local SQLite spool in WAL mode with
  `synchronous=FULL` and returns `spooled` (no `seq` or `event_hash`: the chain head may not
  be readable during the outage). Acknowledgement means: in Cassandra at QUORUM, or fsynced
  to the spool.
- The spool keeps a local index on `(source, source_ref)`, so a redelivery during the outage
  is answered `duplicate` without Cassandra.
- While a shard has spooled events, new events for that shard go to the spool too, so order
  is kept. Once `/readyz` passes, a drainer replays oldest first through the normal write path
  (idempotency check, sequencing, hashing), then resumes direct writes.
- The spool path must be on persistent storage; startup warns `archive_spool_ephemeral`
  otherwise (same rule the pipeline uses).

### 7.5 Integrity checks

- The exporter reads each feed partition only up to `now - 2 minutes` and re-scans the
  previous 5 minutes on every pass (ids written slightly out of order become visible late);
  the Parquet merge drops repeats by `(doc_id, seq)`.
- For every document touched, it checks that exported `seq` values are contiguous from the
  last exported one up to `doc_timeline.head_seq`. Any gap, at the end or in the middle (a
  failed `events_by_time` write), is backfilled from `doc_timeline` and counted in
  `archive_feed_backfill_total`.
- `verify` walks a document's chain from `seq = 1`, recomputing each `event_hash` and
  `payload_sha256`, and for upstream kinds the hash version 2 `entry_hash`.

### 7.6 Follower

- Read-only connection to the mailroom DB (`DATABASE_URL` of the pipeline, or its SQLite
  path opened `mode=ro`).
- Sources and cursors:

| Upstream table | Cursor | Emits |
|---|---|---|
| `audit_log` (unified) | global `seq` | `audit_row`; a `node_visit` for each `detail.trace[]` entry; `stage_changed` for each stage transition the trace shows |
| `audit_attempts` | none: read by `parent_id` when the parent row or `resumed` addendum is seen (they commit in the same transaction) | `audit_attempt` |
| `audit_addenda` | `(timestamp, addendum_id)`, re-scanning the last 10 minutes each pass | `audit_addendum`, `correction` for review decisions with field diffs, `relation` for `relations_linked`; a `resumed` addendum's sub-rows are read by `parent_id` |
| `documents` | `(updated_at, doc_id)`, re-scanning the last 10 minutes | `stage_changed` with `observed: true` when `stage` differs from the archive's static `stage`; links pending arrivals (§6.4) |
| `audit_log` before the unified migration, or `audit_log_legacy` after it | SQLite `rowid` (Postgres: `(timestamp, entry_id)` re-scanning the last 10 minutes) | `audit_row` with `legacy: true` |

- **Which tables exist decides the mode.** The follower targets upstream `main` (unified audit
  row) and also supports 0.9.3 and other pre-migration databases. It reads `audit_meta`: when
  absent, the per-event `audit_log` is the legacy source; when present, the legacy source is
  `audit_log_legacy` and `audit_log` holds unified rows. A migration seen mid-run (marker
  appears) makes the follower finish the legacy cursor, then switch.
- **Stage history is approximate from polling.** Polling `documents` only sees the current
  stage, so fast moves (inbox → processing → classified within one poll) collapse. The exact
  sequence comes from the unified row's trace when it lands; until then, polled changes are
  marked `observed: true`. Analysis (A2, A3) prefers trace-derived transitions.
- Poll every 1 second, 500 rows per query; cursor saved after the batch is acknowledged.
  At-least-once; overlap re-scans and the idempotency table make repeats harmless.
- Lag metric: `archive_follower_lag_rows{source}` and `..._seconds`.

### 7.7 Inbox probe

Watches the pipeline's `data/pipeline/inbox/` with watchdog, hashes each new file, reads its
`.meta` sidecar if present, uploads the blob, and posts an arrival (`POST /v1/arrivals`). It
has no `doc_id` yet; the follower links it (§6.4). If the pipeline moves the file before the
probe reads it, the probe looks in `processing/<id>/`. If that also fails, the follower
creates the arrival later from `documents.file_sha256` with `late_arrival: true`.

### 7.8 ArchiveSink (trace-sink plugin)

```toml
[project.entry-points."mailroom.trace_sinks"]
archive = "mailroom_archive.sink:ArchiveSink"
```

- `name = "archive"`, `off_box = True`, `recoverable = True`, `max_payload_bytes = 4 MiB`.
- `record_io`: posts a `trace_io` event; returns `"archive:<visit_id>"`.
- `capture_pending`: posts a `pending_row` event; returns `"archive:pending:<shard>:<timeuuid>"`.
- `fetch_pending`: reads `pending_rows`; `fetch_io`: Q7.
- Timeouts 2 seconds; any error returns `None` (the pipeline's contract: a sink never fails a
  node). Calls go through a small in-process queue so a slow archive never blocks a node.

## 8. Analysis (OLAP)

### 8.1 Warehouse layout

```
warehouse/
  events/date=2026-11-20/hour=14/part-<uuid>.parquet          # every event
  documents_current/date=2026-11-20/part-0.parquet            # daily snapshot of statics
  trace_io/date=2026-11-20/part-<uuid>.parquet                # always; fetch_io falls back here
  arrivals/date=2026-11-20/part-<uuid>.parquet
  seals/date=2026-11-20/part-0.parquet
  sim_runs/part-0.parquet
  _manifest.json                                              # files, row counts, sha256 per file
```

- Parquet, zstd level 3, row groups of about 128 MiB, sorted by `(doc_id, seq)` inside each
  file so per-document scans prune well.
- `events` columns: everything in `doc_timeline` plus the static columns current at write
  time (`matter_id`, `doc_type`, `stage`, `channel`) **denormalized**, `sim_run_id`,
  `occurred_at` (timestamp with microseconds, from `occurred_us`), `ingested_at` (from
  `event_id`), and `payload` as a JSON string column.
- Each keyspace exports to its own root (`warehouse/archive/`, `warehouse/archive_sim/`), so
  simulations never mix with real traffic in analysis. Hashes are lowercase hex strings in Parquet (empty string for the
  first event's `prev_hash`) so analysts can read and join them. Each `kind` also gets a
  typed view (§8.2).
- Freshness: the exporter runs every 5 minutes and writes the current hour as small parts; a
  compaction pass merges an hour's parts once it closes.
- The manifest records each file's SHA-256, so the warehouse itself is verifiable.

### 8.2 Opening it

`mailroom-archive analyze` starts DuckDB with these views. They are also written to
`warehouse/views.sql` so any DuckDB, Jupyter or BI tool can load them. The root is a DuckDB
variable, so the same views work on a local folder or an S3 prefix
(`SET VARIABLE archive_root = 's3://bucket/archive'`, with the `httpfs` extension).

```sql
SET VARIABLE archive_root = 'warehouse';

CREATE OR REPLACE VIEW events AS
SELECT *
FROM read_parquet(getvariable('archive_root') || '/events/**/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW documents AS
SELECT *
FROM read_parquet(getvariable('archive_root') || '/documents_current/**/*.parquet', hive_partitioning = true)
QUALIFY row_number() OVER (PARTITION BY doc_id ORDER BY date DESC) = 1;

-- typed view of stage changes; date and hour are kept so callers can prune partitions
CREATE OR REPLACE VIEW stage_changes AS
SELECT date, hour, doc_id, seq, occurred_at, doc_type, channel,
       payload ->> '$.from_stage'                         AS from_stage,
       payload ->> '$.to_stage'                           AS to_stage,
       coalesce(CAST(payload ->> '$.observed' AS BOOLEAN), false) AS observed
FROM events
WHERE kind = 'stage_changed';

-- typed view of node visits
CREATE OR REPLACE VIEW node_visits AS
SELECT date, hour, doc_id, occurred_at, doc_type,
       payload ->> '$.node'                              AS node,
       payload ->> '$.status'                            AS status,
       payload ->> '$.outcome'                           AS outcome,
       payload ->> '$.model'                             AS model,
       CAST(payload ->> '$.latency_ms'        AS DOUBLE) AS latency_ms,
       CAST(payload ->> '$.prompt_tokens'     AS BIGINT) AS prompt_tokens,
       CAST(payload ->> '$.completion_tokens' AS BIGINT) AS completion_tokens,
       CAST(payload ->> '$.cost_usd'          AS DOUBLE) AS cost_usd
FROM events
WHERE kind = 'node_visit';
```

### 8.3 Standard analysis queries (DuckDB)

```sql
-- A1 ingest rate per minute by channel, last 24 hours
-- Partition filter on date first, so only today's and yesterday's files are read.
SELECT date_trunc('minute', occurred_at) AS minute,
       channel,
       count(*)                          AS arrivals
FROM events
WHERE kind = 'arrived'
  AND date >= (now() AT TIME ZONE 'UTC')::DATE - 1   -- partitions are UTC dates
  AND occurred_at >= now() - INTERVAL 24 HOUR
GROUP BY ALL
ORDER BY minute, channel;
```

```sql
-- A2 time from arrival to terminal stage, percentiles by document type
WITH arrivals AS (
    SELECT doc_id, min(occurred_at) AS arrived_at
    FROM events
    WHERE kind = 'arrived'
    GROUP BY doc_id
),
terminal AS (
    -- first time each document reached a terminal stage
    SELECT doc_id, doc_type, min(occurred_at) AS done_at, arg_min(to_stage, occurred_at) AS outcome
    FROM stage_changes
    WHERE to_stage IN ('archived', 'failed')
    GROUP BY doc_id, doc_type
)
SELECT t.doc_type,
       t.outcome,
       count(*)                                                              AS docs,
       quantile_cont(epoch(t.done_at - a.arrived_at), [0.5, 0.95, 0.99])     AS seconds_p50_p95_p99
FROM terminal AS t
JOIN arrivals AS a USING (doc_id)
GROUP BY ALL
ORDER BY t.doc_type, t.outcome;
```

```sql
-- A3 documents stuck: latest stage is not terminal and older than 1 hour.
-- Built from events (5-minute freshness), not the daily documents snapshot.
WITH latest AS (
    SELECT doc_id,
           arg_max(to_stage, seq)   AS stage,
           max(occurred_at)         AS stage_at,
           arg_max(doc_type, seq)   AS doc_type
    FROM stage_changes
    WHERE date >= (now() AT TIME ZONE 'UTC')::DATE - 30   -- partition filter, UTC
    GROUP BY doc_id
)
SELECT doc_id, doc_type, stage, stage_at
FROM latest
WHERE stage IN ('inbox', 'processing', 'classified', 'review')
  AND stage_at < now() - INTERVAL 1 HOUR
ORDER BY stage_at
LIMIT 500;
```

```sql
-- A4 duplicate content: same bytes arriving more than once, with the channels involved
SELECT payload ->> '$.content_sha256'                 AS content_sha256,
       count(*)                                       AS arrivals,
       count(DISTINCT doc_id)                         AS docs,
       list(DISTINCT channel ORDER BY channel)        AS channels,
       min(occurred_at)                               AS first_seen,
       max(occurred_at)                               AS last_seen
FROM events
WHERE kind = 'arrived'
GROUP BY 1
HAVING count(*) > 1
ORDER BY arrivals DESC
LIMIT 100;
```

```sql
-- A5 node cost and latency by model, last 7 days
SELECT node, model,
       count(*)                                   AS visits,
       sum(cost_usd)                              AS cost_usd,
       quantile_cont(latency_ms, 0.95)            AS p95_latency_ms,
       avg(prompt_tokens + completion_tokens)     AS avg_tokens,
       avg(CASE WHEN outcome = 'retry' THEN 1 ELSE 0 END) AS retry_rate
FROM node_visits
WHERE date >= (now() AT TIME ZONE 'UTC')::DATE - 7     -- prune files first
  AND occurred_at >= now() - INTERVAL 7 DAY
GROUP BY ALL
ORDER BY cost_usd DESC;
```

```sql
-- A6 corrections: which fields reviewers change most, by document type
SELECT doc_type,
       payload ->> '$.field_path' AS field_path,
       count(*)                   AS corrections,
       count(DISTINCT doc_id)     AS docs
FROM events
WHERE kind = 'correction'
GROUP BY ALL
ORDER BY corrections DESC
LIMIT 50;
```

```sql
-- A7 offline chain check over the warehouse: every document's seq runs 1..n with no gaps
-- and each prev_hash equals the previous event's hash. Returns only broken links.
WITH ordered AS (
    SELECT doc_id, seq, prev_hash, event_hash,
           lag(event_hash) OVER (PARTITION BY doc_id ORDER BY seq) AS expected_prev,
           lag(seq)        OVER (PARTITION BY doc_id ORDER BY seq) AS prev_seq
    FROM events
)
SELECT doc_id, seq, prev_seq
FROM ordered
WHERE (seq = 1 AND prev_hash <> '')
   OR (seq > 1 AND (prev_seq IS DISTINCT FROM seq - 1 OR prev_hash IS DISTINCT FROM expected_prev));
```

A7 checks linkage only; recomputing hashes needs the Python verifier (`verify --warehouse`),
because DuckDB's `sha256` works on strings and the canonical form must match byte for byte.

```sql
-- A8 simulator run summary: throughput and end-to-end latency per scenario
SELECT e.sim_run_id,
       count(*) FILTER (WHERE kind = 'arrived')                         AS docs,
       count(*)                                                         AS events,
       -- ingest time, not occurred_at: simulated time runs at clock_speed
       count(*) / epoch(max(ingested_at) - min(ingested_at))            AS events_per_sec
FROM events AS e
WHERE e.sim_run_id IS NOT NULL
GROUP BY e.sim_run_id
ORDER BY e.sim_run_id;
```

**Performance notes.** All queries filter on hive partitions (`date`, `hour`) when given a
time range, so DuckDB skips other files. Sorting files by `(doc_id, seq)` makes A7 and
per-document scans cheap. Paths in the views are relative; point them at an S3 prefix by
changing one variable (`SET archive_root = 's3://...'`).

### 8.4 Mailroom warehouse compatibility

`mailroom-archive import-mailroom-warehouse <dir>` loads the pipeline's own Parquet
(`documents_<date>`, `audit_rows_<date>`, `audit_attempts_<date>`, `audit_addenda_<date>`)
as views beside the archive's, so the two can be joined on `doc_id` and `entry_hash`.

## 9. Simulator

### 9.1 Mode A: synthetic events (load)

`mailroom-archive sim run scenarios/steady.yaml`

- Generates complete document lifecycles and sends them to the ingest API (default) or, with
  `--direct`, straight to Cassandra through the shard writer library to benchmark the store
  without HTTP.
- **Arrivals:** non-homogeneous Poisson process with a diurnal curve and scheduled bursts
  (end of quarter filings, court deadlines, claims after a storm). Per-scenario rate.
- **Mix:** the five classes with configurable weights. Channels: `upload` and `gmail` (the
  pipeline's real channels), plus `postal_scan`, `efiling` and `fax`, which the pipeline does
  not have and are tagged `simulated_channel: true`. In Mode B, simulated channels arrive as
  `upload` with the simulated channel kept in the sidecar. Matters with bundles of related documents, reusing the
  corpus's bundle ideas.
- **Content:** generated text documents from per-class templates (seeded Faker), so every
  document has real bytes and a real SHA-256. Optional `--corpus` uses rows from
  a public mailroom corpus dataset as templates. `--no-blobs` sends hashes only for pure
  metadata load.
- **Duplicates:** injected at configured rates using the corpus vocabulary:
  `exact_duplicate`, `normalized_duplicate`, `near_duplicate`, `template_variant`,
  `legitimate_repeat`.
- **Lifecycle:** follows the pipeline's state machine (inbox → processing → classified →
  archived, or review, or failed), with per-node visits and latency, token and cost
  distributions, classification retries, arbiter rescue, judge failures, human corrections
  hours later, rescore addenda days later (time-compressed by a `clock_speed` factor).
- **Faults:** scheduled `docker kill` / `docker start` of a storage node, added network
  delay (`tc netem`, when available), ingest API restarts, follower restarts.
- **Ground truth:** every generated event is written to `sim_out/<run_id>/truth.parquet`.
  `sim check <run_id>` compares it with the archive: every event present exactly once,
  correct order, chains verify, seals verify.

### 9.2 Mode B: real files through the real pipeline (end to end)

`mailroom-archive sim e2e scenarios/e2e-smoke.yaml --inbox <path to mailroom inbox>`

- Writes real files with `.meta` sidecars into the pipeline's inbox at a low rate (default one
  every 10 seconds), using the same generators plus corpus documents.
- Runs against `local-mailroom-sandbox` with its mock provider by default, so there is no
  model cost; a real provider is opt-in.
- The sandbox vendors llm-mailroom 0.9.3, which has neither the unified audit row nor trace
  sinks. Until the sandbox refreshes its vendored copy (upstream rollout step 3), Mode B checks
  the probe and the follower against the legacy per-event `audit_log`, and the sink is tested
  only by contract tests. After the refresh, Mode B also covers unified rows and the sink.
- The inbox probe, follower and (if full trace is on) sink capture everything; `sim check`
  then verifies each document arrived with the right hash, reached a terminal stage, has an
  upstream audit row whose hash verifies, and that the archive's chain verifies.

### 9.3 Scenarios shipped

| Scenario | Rate | Duration | Purpose |
|---|---|---|---|
| `smoke` | 10 docs/s | 2 min | CI and quick checks |
| `steady` | 100 docs/s (about 2,500 events/s) | 30 min | Sustained capacity |
| `burst` | 500 docs/s | 60 s, then steady | Back-pressure, spool, recovery |
| `node-down` | 100 docs/s | 15 min, one node killed at minute 5, restarted at 10 | Fault tolerance |
| `soak` | 50 docs/s | 24 h | Leaks, compaction, TTL expiry |
| `dup-storm` | 50 docs/s, 60% duplicates | 10 min | Dedup and hot hash partitions |
| `e2e-smoke` | 1 doc / 10 s | 10 min | Mode B |

Each run writes `sim_out/<run_id>/report.md` and a row in `sim_runs`: achieved rates, p50/p95/
p99 ingest latency, 429s, spooled events, peak spool size, follower lag, export lag, verify
result.

### 9.4 Targets

On the 8 GB, 3-node profile: `steady` sustained with p99 ingest latency under 250 ms and zero
verify failures; `node-down` with zero lost acknowledged events and zero verify failures;
`burst` drains its spool within 5 minutes of the burst ending. These are targets to measure
and record in the report, not promises; the first real measurements go in the README.

## 10. Resources and scaling

- **Content dedup:** document bytes are stored once per SHA-256; duplicate storms cost only
  sighting rows.
- **Compression:** Zstd on `doc_timeline` (level 3) and `trace_io` (level 6); LZ4 on the feed
  tables; zstd Parquet.
- **Expiry:** `events_by_time` 14 days, `trace_io` 30 days, `idempotency` 90 days. Parquet is
  the long-term record.
- **Tiering (optional, off by default):** documents in a terminal stage for more than
  `MAILROOM_ARCHIVE_TIER_DAYS` (default 180) have their history verified against Parquet, then
  their clustered rows deleted with one range delete per partition. Statics remain, with a
  `retention` event recording which files hold the history. Reads of tiered history are served
  from Parquet.
- **Partition bounds:** every partition key includes something that bounds it: document,
  month (sightings), year (matters), hour and shard (feed).
- **Memory profiles** (compose profiles):
  - `full`: 3 × Cassandra 5, `MAX_HEAP_SIZE=1G`, container limit 2 GB each.
  - `small` (default on 8 GB machines): 3 × Cassandra 5, `MAX_HEAP_SIZE=512M`,
    `HEAP_NEWSIZE=128M`, limit 1.2 GB each, concurrent compactors 1.
  - `single`: 1 node, RF=1, for development only (fault tests skipped).
  - `scylla` (later): 3 × ScyllaDB `--smp 1 --memory 750M`.
- **Estimated footprint:** about 25 events per document at about 300 bytes compressed is about
  7.5 KB per document, about 22 KB with RF=3. One million documents is about 22 GB across the
  cluster before trace I/O and blobs. Measured numbers replace these after the first soak run.
- **Scaling out:** add nodes (data rebalances); add API instances (shard leases); raise shard
  count only between deployments (it changes ownership, not data).

## 11. Observability

- **Metrics** (`/metrics`, Prometheus): `archive_events_ingested_total{kind,source,status}`,
  `archive_ingest_latency_seconds` (histogram), `archive_shard_queue_depth{shard}`,
  `archive_write_retries_total`, `archive_spool_events`, `archive_spool_oldest_seconds`,
  `archive_follower_lag_rows{source}`, `archive_follower_lag_seconds{source}`,
  `archive_export_lag_seconds`, `archive_feed_backfill_total`, `archive_seal_last_bucket`,
  `archive_verify_failures_total`, `archive_cassandra_hosts_up`, `archive_blob_bytes_total`,
  `archive_duplicates_total{type}`.
- **Logs:** structlog JSON, one line per batch and per error, with `doc_id`, `shard`,
  `event_id`, `source_ref`; the same event names style as the pipeline (`archive_spooled`,
  `archive_gap_backfilled`, `archive_verify_failed`).
- **Traces:** OpenTelemetry spans on ingest batches and shard writes when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set, so they show next to the pipeline's Phoenix or OTLP
  traces.
- **Dashboards:** Grafana JSON in `ops/grafana/` (ingest, latency, shards, spool, follower
  lag, export lag, cluster health), Prometheus and Grafana in the `obs` compose profile.
- **Alerts** (rules in `ops/prometheus/alerts.yml`): spool non-empty for 5 minutes; follower
  lag over 60 seconds; export lag over 15 minutes; any verify failure; fewer than 2 hosts up.
- **Audit:** `mailroom-archive verify doc <id> | seal <bucket> | range --from --to |
  --warehouse` and the matching API; output is JSON with each check and its result.

## 12. Repository and publication

- **Fresh history.** The repository starts from an empty history whose first commit holds only
  this design, the plan and new code.
- **Nothing copied.** No third-party code is carried over; everything is written for this design.
- **Pre-publication check** before switching the repo to public: a scripted search of the tree
  and of `git log -p` against a local, untracked deny-list; no secrets; license file present
  (the owner chooses).
- **Publication** happens after the first working vertical slice is on `main` and the owner
  confirms.

### Layout

```
src/mailroom_archive/
  api/            FastAPI app, routes, auth
  core/           event model, canonical JSON, hashing, kinds
  store/          Cassandra session, schema, prepared statements, shard writer, spool
  blobs/          filesystem and S3 blob stores
  follower/       mailroom DB tailer, inbox probe
  sink.py         ArchiveSink (mailroom.trace_sinks entry point)
  export/         Parquet exporter, compaction, manifest
  seal/           Merkle sealer
  verify/         chain, payload, upstream and seal verification
  analyze/        DuckDB views and CLI
  sim/            generators, scenarios, runner, faults, checker
  cli.py          Typer entry point `mailroom-archive`
schema/           CQL files (one per table), applied by `mailroom-archive schema apply`
scenarios/        simulator YAML
ops/              compose files and profiles, Grafana, Prometheus
tests/            unit, contract (upstream hash parity, sink protocol), integration (compose)
docs/
```

## 13. Testing

- **Unit:** canonical JSON and hashing (golden vectors), shard routing, idempotency, spool
  ordering, Merkle roots, generators (seeded determinism), scenario parsing.
- **Contract:** hash version 2 parity with the upstream pipeline's audit-entry formatter on recorded
  rows; `ArchiveSink` against the upstream `TraceSink` protocol (`isinstance` check, `None`
  on errors, `off_box` honoured); follower against a fixture mailroom DB built by upstream's
  `build_legacy_audit_fixture.py` and a unified-row fixture.
- **Integration (compose, `single` and `small` profiles):** write, read, correct, verify;
  `node-down` scenario passes with zero lost acknowledged events; spool fills and drains;
  exporter output verified by DuckDB queries A1 to A8; `sim check` passes for `smoke`.
- **End to end:** `e2e-smoke` against `local-mailroom-sandbox` in mock mode.
- CI on GitHub Actions: unit and contract on every push; integration on the `single` profile
  on every push and on `small` nightly.

## 14. Decided and left to the builder

**Decided with the owner:** separate service beside the pipeline; append-only corrections
with current state alongside; the archive can override the pipeline, with conflicts recorded
(§6.5a); the archive is inside the pipeline's trust zone (§3); Cassandra hot store following
the static-column pattern with RF=3 and quorum; both simulator modes; publication cleared, in
a fresh history with no third-party code; name
`mailroom-archive`.

**The designer's calls (open to the owner's correction):**
- Follower as primary integration, sink as secondary (forced by the sink contract).
- Per-document chains plus hourly Merkle seals instead of one global chain, so writes scale
  across shards.
- 64 shards, 16 feed shards, the TTLs and size thresholds above.
- Channels beyond `upload`/`gmail` exist only in the simulator, tagged.
- DuckDB over Parquet as the analysis surface.
- Arrivals are their own entity, linked to a document by the follower (§6.4), because the
  pipeline assigns `doc_id` only after it claims a file.
- `doc_id` stored as text; event time stored as integer microseconds so hashes recompute.
- Idempotency row written before the event; spool holds unsequenced events.
- Trace I/O always exported to Parquet so `fetch_io` keeps working after the 30-day TTL.
- The follower targets upstream `main` and also supports 0.9.3 / pre-migration databases.
- Single-instance writes need no lightweight transactions; multi-instance mode uses them on
  every `doc_timeline` write for safety.

**Left to the builder:** Python package versions (pin the newest stable at build time), exact
Faker templates, Grafana panel layout, the S3 client, and the order of work in the plan.

## 15. Revisit as it grows

- ScyllaDB profile once memory is the bottleneck.
- Kafka or Redpanda between intake and the shard writers if multiple producers outgrow HTTP
  batching.
- Iceberg tables over the Parquet files when several engines need to read them concurrently.
- Multi-datacenter replication with `LOCAL_QUORUM`.
- Moving the follower to change data capture if the pipeline moves to Postgres.
- Sending archive corrections back into the pipeline once it has an API for them.
