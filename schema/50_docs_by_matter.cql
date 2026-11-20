CREATE TABLE IF NOT EXISTS {ks}.docs_by_matter (
    matter_id      text,
    year           smallint,
    created_at     timestamp,
    doc_id         text,
    doc_type       text,
    content_sha256 blob,
    PRIMARY KEY ((matter_id, year), created_at, doc_id)
) WITH CLUSTERING ORDER BY (created_at DESC, doc_id ASC);
