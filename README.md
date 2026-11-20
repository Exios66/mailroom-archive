# mailroom-archive

A content-hash archive and ingest service for the mailroom document pipeline. It records every
document's arrival, status history, traces and audit records in a replicated Cassandra store,
keeps them hash-chained and verifiable, and exports them to Parquet for analysis. A built-in
simulator drives it with high volumes of legal, insurance and corporate mail and filings.

Design: [docs/superpowers/specs/2026-11-20-mailroom-archive-design.md](docs/superpowers/specs/2026-11-20-mailroom-archive-design.md)

## Quick start

```bash
docker compose -p mailroom-archive -f ops/compose.yml --profile single up -d --wait
uv run mailroom-archive schema apply --profile single
MAILROOM_ARCHIVE_TOKEN=change-me MAILROOM_ARCHIVE_READ_TOKEN=change-me-too uv run mailroom-archive serve
```

Then post events:

```bash
curl -s localhost:8544/v1/events -H 'authorization: Bearer change-me' -H 'content-type: application/json' \
  -d '[{"doc_id":"doc-1","kind":"stage_changed","actor":"pipeline","source":"demo","source_ref":"demo:1",
        "occurred_us":1763650000000000,"payload":{"to_stage":"classified","doc_type":"contract"}}]'
curl -s localhost:8544/v1/verify/doc/doc-1 -H 'authorization: Bearer change-me-too'
```

Profiles, settings, the trust-zone requirements and the test suites are in
[docs/running.md](docs/running.md).
