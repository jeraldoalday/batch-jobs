-- 0003_leases: lease bookkeeping for jobs a worker has claimed.
--
-- A claimed job is marked 'running' and stays that way until the worker
-- that took it reports an outcome. If that worker's process dies first --
-- OOM kill, SIGKILL, a machine going away -- nothing ever reports, and the
-- row sits in 'running' forever: invisible to Dequeue, uncounted by Len,
-- and never retried. These two columns are what lets another process
-- notice and take it back.

-- owner identifies the process holding the claim, for operator diagnosis
-- ("which host was running this when it stalled?"). It carries no
-- correctness weight: recovery is decided by the lease expiry alone, so a
-- restarted process that reuses its owner string is not a special case.
ALTER TABLE jobs ADD COLUMN owner TEXT NOT NULL DEFAULT '';

-- lease_expires_at is when the claim lapses, in Unix nanoseconds. A worker
-- extends it by heartbeating while it works, so a long job holds its claim
-- as long as it is alive. Zero means "no lease", which is how rows written
-- before this migration -- and jobs claimed with leases disabled -- are
-- left alone by recovery rather than being reclaimed en masse.
ALTER TABLE jobs ADD COLUMN lease_expires_at INTEGER NOT NULL DEFAULT 0;

-- Covers the reaper's scan for expired claims: running jobs whose lease
-- has lapsed. The status prefix keeps the scan proportional to the number
-- of in-flight jobs, not the whole history.
CREATE INDEX IF NOT EXISTS idx_jobs_lease
    ON jobs (status, lease_expires_at)
    WHERE lease_expires_at != 0;
