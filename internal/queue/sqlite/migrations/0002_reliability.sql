-- 0002_reliability: per-job execution timeout and idempotency-key dedupe.

ALTER TABLE jobs ADD COLUMN timeout_ns INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT '';

-- Enforces at most one job per (queue, idempotency_key) so a producer can
-- safely retry Enqueue after e.g. a network error without risking a
-- duplicate. Jobs that opt out of dedupe (the common case) have an empty
-- idempotency_key, which the WHERE clause excludes from the constraint so
-- they never collide with each other.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key != '';
