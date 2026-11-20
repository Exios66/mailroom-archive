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
