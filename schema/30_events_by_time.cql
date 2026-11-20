-- Ingest feed: drives export, seals and live dashboards; expires after 14 days.
CREATE TABLE IF NOT EXISTS {ks}.events_by_time (
    bucket      text,                             -- YYYYMMDDHH UTC from event_id
    shard       tinyint,                          -- xxhash64(doc_id) % 16
    event_id    timeuuid,
    doc_id      text,
    seq         int,
    occurred_us bigint,
    kind        text,
    channel     text,
    doc_type    text,
    stage       text,
    event_hash  blob,
    sim_run_id  text,
    PRIMARY KEY ((bucket, shard), event_id)
) WITH CLUSTERING ORDER BY (event_id ASC)
  AND default_time_to_live = 1209600
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
  AND compression = {'class': 'LZ4Compressor'};
