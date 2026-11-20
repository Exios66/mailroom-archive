CREATE TABLE IF NOT EXISTS {ks}.seals (
    day          int,                             -- yyyymmdd
    bucket       text,                            -- YYYYMMDDHH
    shard        tinyint,
    seal_no      int,                             -- 0 = hour seal; 1, 2, ... = late seals
    event_count  int,
    first_event  timeuuid,
    last_event   timeuuid,
    leaf_ids     list<timeuuid>,
    merkle_root  blob,
    prev_seal    blob,
    seal_hash    blob,
    sealed_at    timestamp,
    PRIMARY KEY ((day), bucket, shard, seal_no)
);
