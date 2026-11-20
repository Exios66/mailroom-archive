CREATE TABLE IF NOT EXISTS {ks}.blobs (
    content_sha256 blob PRIMARY KEY,
    size_bytes     bigint,
    mime           text,
    storage_uri    text,
    first_seen     timestamp,
    verified_at    timestamp
);
