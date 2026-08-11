-- 0001_init: schedules table for recurring, cron-driven job templates.

CREATE TABLE IF NOT EXISTS schedules (
    id          TEXT PRIMARY KEY,
    queue       TEXT NOT NULL,
    payload     BLOB,
    priority    INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    cron_expr   TEXT NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    next_run_at INTEGER NOT NULL,
    last_run_at INTEGER,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- Covers the Due query: enabled schedules whose next_run_at has passed,
-- ordered soonest first.
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules (enabled, next_run_at);
