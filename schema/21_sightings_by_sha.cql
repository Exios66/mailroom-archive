CREATE TABLE IF NOT EXISTS {ks}.sightings_by_sha (
    content_sha256    blob,
    month             int,                        -- yyyymm
    seen_at           timeuuid,
    arrival_id        text,
    channel           text,
    original_filename text,
    PRIMARY KEY ((content_sha256, month), seen_at)
) WITH CLUSTERING ORDER BY (seen_at DESC)
  AND compression = {'class': 'LZ4Compressor'};
