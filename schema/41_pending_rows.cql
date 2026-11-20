CREATE TABLE IF NOT EXISTS {ks}.pending_rows (
    shard          tinyint,
    captured_at    timeuuid,
    doc_id         text,
    detail_sha256  blob,
    complete       boolean,
    record         blob,
    PRIMARY KEY ((shard), captured_at)
);
