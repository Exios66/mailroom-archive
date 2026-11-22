# Running mailroom-archive

## Cassandra profiles

`ops/compose.yml` runs Cassandra 5 under the compose project name `mailroom-archive`.

| Profile | Nodes | Heap / memory limit per node | Replication | Use |
|---|---|---|---|---|
| `single` | 1 | 512 MB / 1200 MB | RF 1 | development, CI |
| `small` | 3 | 512 MB / 1200 MB | RF 3 | an 8 GB machine; fault tests |
| `full` | 3 | 1 GB / 2 GB | RF 3 | a larger host |

```bash
ARCHIVE_ENV=small ARCHIVE_MEM=1200m docker compose -p mailroom-archive -f ops/compose.yml --profile small up -d --wait
uv run mailroom-archive schema apply --profile small
```

Nodes join one at a time (Cassandra refuses concurrent bootstraps), so `small` and `full`
take a couple of minutes to become healthy. Stop and remove everything, data included, with
`docker compose -p mailroom-archive -f ops/compose.yml --profile full down -v`.

All reads and writes use `QUORUM`. With RF 3, any one node can be down with no effect. With
two down, writes go to the local spool and are replayed in order once a quorum is back.

## Settings

All settings come from environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `MAILROOM_ARCHIVE_CASSANDRA_HOSTS` | `127.0.0.1` | comma-separated contact points |
| `MAILROOM_ARCHIVE_CASSANDRA_USERNAME`, `..._PASSWORD` | unset | Cassandra authentication |
| `MAILROOM_ARCHIVE_KEYSPACE` | `archive` | `archive_sim` for simulator runs |
| `MAILROOM_ARCHIVE_TOKEN` | empty | write token (also reads) |
| `MAILROOM_ARCHIVE_READ_TOKEN` | empty | read-only token |
| `MAILROOM_ARCHIVE_DATA_DIR` | `./data` | spool (`spool.db`) and blobs (`blobs/`) |
| `MAILROOM_ARCHIVE_SHARDS` | `64` | writer shards |
| `MAILROOM_ARCHIVE_QUEUE_SIZE` | `2000` | events per shard queue before 429 |

Every route except `/healthz` needs `Authorization: Bearer <token>`. With an empty token, no
request can authenticate, so set both before serving.

```bash
uv run mailroom-archive serve --host 127.0.0.1 --port 8544
uv run mailroom-archive verify doc <doc_id> --json
```

The spool must be on persistent storage under `MAILROOM_ARCHIVE_DATA_DIR`. If it isn't,
startup logs `archive_spool_ephemeral`.

## Read-only SQLite follower

Run the ingest API first, then run one follower for each pipeline database:

```bash
uv run mailroom-archive follow --sqlite /path/to/mailroom.db --name pipeline-main --once
uv run mailroom-archive follow --sqlite /path/to/mailroom.db --name pipeline-main
```

The follower uses `MAILROOM_ARCHIVE_TOKEN` and `--archive-url` (default
`http://127.0.0.1:8544`) to send events through the running API. It also needs Cassandra
access for the existing `cursors` table. Its Cassandra settings/keyspace must match the API.
It does not start another shard writer or modify the pipeline database.

Choose a stable `--name`, unique for that source database, and keep it on restarts. It
namespaces both event identities and checkpoints. Run only one follower with that name;
checkpoint ownership/leases are not implemented. Replacing the source with a different
database requires a different name. Do not routinely reset cursors or change names:
idempotency retention is 90 days, as in the foundation design.

Each pass reads pages of at most 500 rows and closes each source snapshot before network
work. Checkpoints advance after every event from a source row is durably acknowledged,
including an API spool acknowledgement. HTTP/storage errors leave unacknowledged work for
replay. Continuous mode retries HTTP, SQL/storage and migration interruptions on its polling
loop; `--once` exits unsuccessfully on an error. Malformed JSON, inconsistent parent/document
identities and unsupported records stop the follower without skipping the source row.
An individual event larger than the API's 4 MiB limit must be addressed before progress can
continue; ordinary payload spilling over 16 KiB still happens in the archive writer.

Initial audit rows and judge attempts are retained alongside later addenda, including
rescores and resumed attempts. Each observed catalog revision includes the original fields
under `payload.catalog_snapshot`; updates sharing a timestamp are distinguished by content.
An unchanged stage observation is not a new transition, and fast intermediate states between
polls can be missed. The source's timestamp representation must be consistent within each
cursor column, as it is in the pipeline's SQLite writer. The ten minute overlap captures
late updates within that window; older backdated changes require an explicit reconciliation.

The CLI currently supports SQLite only. Arrival linking, trace sink integration, exact
trace-derived transitions, upstream audit-hash verification and training-label exports are
tracked in the [follow-on plan](superpowers/plans/2026-11-20-archive-following.md).
`verify doc` checks the archive chain and payloads; it does not yet verify upstream audit hashes.

## Trust zone

The archive holds full copies of what the pipeline sees: document bytes, senders, subjects
and extracted fields. Run it only on machines you control, and:

- keep the blob store, the spool and the Cassandra data directories on encrypted volumes
  (open-source Cassandra has no encryption at rest of its own);
- turn on client-to-node and node-to-node TLS for anything beyond the `single` profile;
- turn on Cassandra authentication (`PasswordAuthenticator`) and give the archive its own role;
- keep both API tokens secret, since reads expose document contents too.

## Tests

```bash
uv run pytest tests/unit                                   # no Docker needed
CASSANDRA_HOSTS=127.0.0.1 uv run pytest -m integration     # single profile running
ARCHIVE_PROFILE=small CASSANDRA_HOSTS=127.0.0.1 uv run pytest tests/fault -m fault
```

The fault tests stop and start the `mailroom-archive-cassandra-2-1` and `-3-1` containers.
To run Docker-backed tests on another machine, set `ARCHIVE_VM_HOST` to its SSH alias and use
`scripts/vm.sh sync | up <profile> | run <command> | down`. The script only touches this
project's containers, and stops if any other container on that host stops running.
