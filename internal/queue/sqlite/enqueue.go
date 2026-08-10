package sqlite

import (
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// upsertQuery inserts a new job row, or, if a row with the same ID already
// exists (e.g. a job being rescheduled after a failed attempt), overwrites
// its mutable fields in place. CreatedAt is intentionally never updated by
// the DO UPDATE clause so a job's original enqueue time is preserved.
const upsertQuery = `
INSERT INTO jobs (` + jobColumns + `)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    queue       = excluded.queue,
    payload     = excluded.payload,
    status      = excluded.status,
    priority    = excluded.priority,
    max_retries = excluded.max_retries,
    retry_count = excluded.retry_count,
    last_error  = excluded.last_error,
    updated_at  = excluded.updated_at,
    run_at      = excluded.run_at;
`

// Enqueue implements queue.Queue.
func (q *Queue) Enqueue(j *job.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return queue.ErrClosed
	}
	q.mu.Unlock()

	if _, err := q.db.Exec(upsertQuery, insertArgs(j)...); err != nil {
		return fmt.Errorf("sqlite: enqueue %s: %w", j.ID, err)
	}

	q.mu.Lock()
	q.notifyLocked()
	q.mu.Unlock()
	return nil
}
