-- 0005_retention: an index for finding, and deleting, old finished jobs.
--
-- Pruning asks a question nothing has asked before: "which jobs in this
-- terminal status were last updated before some instant?". idx_jobs_status
-- (0001) can narrow that to one status, but nothing orders the result, so
-- SQLite reads every row of that status and tests updated_at on each. On
-- the table pruning exists to shrink -- millions of successes -- that is a
-- full scan of the very rows a prune pass is trying to avoid touching one
-- at a time, and it happens once per batch.
--
-- Leading with status and following with updated_at turns each batch into
-- a range scan that starts at the oldest row and stops the moment it
-- reaches the cutoff, so the work is proportional to what the batch
-- actually deletes rather than to the history it is deleting from.
CREATE INDEX IF NOT EXISTS idx_jobs_retention ON jobs (status, updated_at);

-- idx_jobs_status is now redundant: (status) is a prefix of
-- (status, updated_at), so every lookup it served -- Len's pending/retrying
-- count, Stats' GROUP BY -- is served by the new index just as well.
--
-- It is dropped rather than left in place because an index is not free
-- when it is unused: every insert, every claim, and every status
-- transition maintains it, which on this table means on the hottest write
-- path in the system.
--
-- List gains as well, and it is the reason to lead with status rather than
-- follow: "WHERE status = ? ORDER BY updated_at DESC LIMIT ?" previously
-- sorted every job in the status to return a page of fifty. It now walks
-- this index backwards and stops at the limit.
DROP INDEX IF EXISTS idx_jobs_status;
