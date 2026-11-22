# Mailroom archive: following the pipeline and retaining outcomes

This is plan 2 of the five-plan sequence established by
[the foundation plan](2026-11-20-archive-foundation.md). The foundation landed on `main`
through `b926d3d`; its latest behavioral change is write-path hardening in `2b5c5e6`.
The repository had no merged or open pull requests when checked on 2026-11-20.
Work here extends that foundation rather than changing its storage architecture.

## Design alignment

- The pipeline owns its relational catalog, judging and review. The archive follows SQL
  read-only and stores immutable events in Cassandra. It does not write back to mailroom.db.
- History, corrections, conflict rules, event hashing, spilling, quorum writes and the
  outage spool remain those of design §§6–7. Producers use the existing ingest API; a
  follower process never starts another shard writer against the same keyspace.
- `audit_row`, `audit_attempt` and `audit_addendum` already represent initial decisions,
  attempts and later outcomes. No new event kind or parallel verdict table is needed.
- The future relational analysis surface remains Parquet/DuckDB (plan 3, design §8).
  Cassandra is not replaced by a second authoritative SQL archive.

Upstream compatibility was checked against the local pipeline's unified/legacy SQL models
and the recent merged work in
[PR 312](https://github.com/Exios66/llm-mailroom/pull/312) and
[PR 314](https://github.com/Exios66/llm-mailroom/pull/314): provider-neutral score fields,
decision slots, calibration and stricter archive gates. The follower retains arbitrary
`detail`, `scores`, model and prompt provenance rather than assuming a particular tracing
provider, graph size or scoring taxonomy. Scoring definitions remain owned by Dojo.

## First slice: read-only SQLite follower

- [x] Open existing SQLite files with `mode=ro` and `query_only`; never initialize SQL schema.
- [x] Detect unified mode from the `audit_meta` **migration key**, not table existence.
- [x] Read legacy rows by SQLite rowid, unified rows by global seq, and addenda/catalog rows
  by timestamp plus primary key. Page at most 500 source rows at a time.
- [x] Re-scan the last ten minutes for addenda and documents, draining all overlap pages
  without moving a saved checkpoint backwards. Bound each poll to its starting high watermarks.
- [x] Materialize each page and its attempts in one SQL snapshot, then close it before
  HTTP or Cassandra work. A long network wait cannot hold a pipeline database read lock.
- [x] Preserve raw initial rows, later rescores, verdicts, findings, scores and parent/attempt
  identifiers. Emit metadata-only node visits alongside their verbatim parent record.
- [x] Retain each observed catalog revision, including revisions with the same timestamp,
  with `observed: true` and the full `catalog_snapshot` in `stage_changed`.
- [x] Reuse the running ingest API, including request count/byte limits and acknowledgement
  validation. Advance Cassandra checkpoints only after every derived event is acknowledged.
- [x] Handle legacy-to-unified migration between pages: retry using a separate migrated
  legacy cursor and stable legacy event identities, even when copied rowids change.
- [x] Add `mailroom-archive follow --sqlite ... --name ... [--once]`.
- [x] Cover replay, partial delivery, checkpoint failure, late additions, tied timestamps,
  migration, spilled findings, archive overrides, API failures, request limits and source
  lock release with offline tests through the real API and shard pool.
- [ ] Run the added Cassandra checkpoint integration test on the configured remote test host.

Catalog observation clarification to §7.6: an updated catalog row is retained even when
its stage is unchanged. This preserves changes in scores or provenance that would otherwise
be lost. `observed: true` distinguishes it from evidence of an actual stage transition;
transition analytics must not count every observation as a transition.

## Remaining plan 2 work

- [ ] Verify upstream v1/v2 row hashes, attempt hashes, addendum linkage and reference
  envelopes; add recorded golden vectors and upstream parity checks. Current `verify`
  verifies the archive chain and payloads, not the authenticity of the source audit chain.
- [ ] Derive exact stage transitions, review field-diff corrections and document relations
  from upstream evidence. Raw source records are retained now; interpretation is separate.
- [ ] Add read-only Postgres support with bounded repeatable-read transactions.
- [ ] Add inbox probe and arrival matching/linking, including skipped duplicates and late arrivals.
- [ ] Add ArchiveSink and hot/warehouse-backed trace and pending retrieval, respecting
  `off_box` and full-trace gating. Do not claim recoverability before fallback is implemented.
- [ ] Add follower lag metrics and fixture-backed sandbox end-to-end tests.

## Plan 3 requirements for KPI and training projections

The follower provides evidence for this work; it does not assign training labels.

- [ ] Export versioned Parquet datasets, manifests and hourly seals as designed; retain
  every original and subsequent outcome event, including spilled payloads.
- [ ] Add DuckDB joins on document, attempt, parent audit identity and archive sequence/hash,
  exposing original output, raw judge verdict/score/findings, model/prompt identity,
  reviewer decisions and subsequent rescoring without overwriting any earlier result.
- [ ] Keep judge verdict, measured KPI, operational stage and human disposition as separate
  columns. `archived` and `review` are workflow states, not positive/negative ground truth.
- [ ] Define a versioned labeling policy with metric name, score scale, upstream scoring
  version, thresholds and provenance. Retain unresolved, missing, unverified, disputed and
  abstained outcomes explicitly; none should default to a negative label or numeric zero.
- [ ] Produce reproducible positive/negative/abstain examples with an as-of cutoff and
  lineage back to immutable source events. Group duplicate content and related documents
  when splitting training/evaluation data to prevent leakage.
- [ ] Measure KPI trends by judge, model, prompt and policy version, keeping original and
  post-review outcomes distinguishable. Feed exports into refinement runs only after
  deterministic validation and source-verification checks pass.

Plans 4 and 5 remain the simulator/fault scenarios and operational maturity work listed in
the foundation plan; this slice does not claim those milestones are complete.
