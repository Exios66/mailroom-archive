CREATE TABLE IF NOT EXISTS {ks}.cursors (
    name        text PRIMARY KEY,
    position    text,
    updated_at  timestamp
);
