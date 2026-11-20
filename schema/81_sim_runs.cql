CREATE TABLE IF NOT EXISTS {ks}.sim_runs (
    run_id      text PRIMARY KEY,
    scenario    text,
    seed        bigint,
    config      text,
    started_at  timestamp,
    ended_at    timestamp,
    stats       map<text, double>
);
