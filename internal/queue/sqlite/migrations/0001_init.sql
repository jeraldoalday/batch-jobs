-- 0001_init: core jobs table and the index used to find ready-to-run jobs.

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    queue       TEXT NOT NULL,
    payload     BLOB,
    status      TEXT NOT NULL,
    priority    INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    last_error  TEXT NOT NULL DEFAULT '',
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    run_at      INTEGER NOT NULL
);

-- Covers the dequeue claim query: jobs eligible to run, ordered by
-- priority (desc) then age (asc), filtered by status and run_at.
CREATE INDEX IF NOT EXISTS idx_jobs_dequeue
    ON jobs (status, run_at, priority DESC, created_at);

-- Covers Queue.Len and status-scoped listing/stats.
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
