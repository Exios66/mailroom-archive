# mailroom-archive

A content-hash archive and ingest service for the mailroom document pipeline. It records every
document's arrival, status history, traces and audit records in a replicated Cassandra store,
keeps them hash-chained and verifiable, and exports them to Parquet for analysis. A built-in
simulator drives it with high volumes of legal, insurance and corporate mail and filings.

Design: [docs/superpowers/specs/2026-11-20-mailroom-archive-design.md](docs/superpowers/specs/2026-11-20-mailroom-archive-design.md)
