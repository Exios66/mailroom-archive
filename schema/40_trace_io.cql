CREATE TABLE IF NOT EXISTS {ks}.trace_io (
    visit_id    text PRIMARY KEY,
    doc_id      text,
    prompt      text,
    completion  text,
    io_sha256   blob,
    spill_ref   text,
    created_at  timestamp
) WITH default_time_to_live = 2592000
  AND compaction  = {'class': 'TimeWindowCompactionStrategy',
                     'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
  AND compression = {'class': 'ZstdCompressor', 'compression_level': 6};
