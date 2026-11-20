-- Written before Q8; a redelivery re-issues Q8 from these values (spec 6.4).
CREATE TABLE IF NOT EXISTS {ks}.idempotency (
    source         text,
    source_ref     text,
    doc_id         text,
    seq            int,
    event_hash     blob,
    event_id       timeuuid,
    payload_sha256 blob,
    prev_hash      blob,
    PRIMARY KEY ((source, source_ref))
) WITH default_time_to_live = 7776000;
