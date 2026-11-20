-- One partition per document: statics are current state, clustered rows the full history.
CREATE TABLE IF NOT EXISTS {ks}.doc_timeline (
    doc_id            text,
    seq               int,
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
    audit_entry_hash  blob      STATIC,
    extracted_data    blob      STATIC,           -- canonical JSON
    overrides         map<text, int> STATIC,      -- field_path -> seq of the archive correction
    created_at        timestamp STATIC,
    sim_run_id        text      STATIC,
    event_id          timeuuid,
    kind              text,
    actor             text,
    source            text,
    source_ref        text,
    occurred_us       bigint,                     -- microseconds; the hashed value
    payload           blob,                       -- canonical JSON, empty when spilled
    payload_ref       text,
    payload_sha256    blob,
    prev_hash         blob,
    event_hash        blob,
    corrects_seq      int,
    PRIMARY KEY ((doc_id), seq)
) WITH CLUSTERING ORDER BY (seq ASC)
  AND compaction  = {timeline_compaction}
  AND compression = {'class': 'ZstdCompressor', 'compression_level': 3};
