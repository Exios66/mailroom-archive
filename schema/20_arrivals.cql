CREATE TABLE IF NOT EXISTS {ks}.arrivals (
    arrival_id        text PRIMARY KEY,           -- UUIDv7
    content_sha256    blob,
    size_bytes        bigint,
    mime              text,
    channel           text,
    original_filename text,
    intake_meta       blob,                       -- canonical JSON of the sidecar
    arrived_us        bigint,
    doc_id            text,                       -- set when linked
    linked_us         bigint,
    sim_run_id        text
);
