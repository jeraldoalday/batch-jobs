package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// upsertQuery inserts a new job row, or, if a row with the same ID already
// exists (e.g. a job being rescheduled after a failed attempt), overwrites
// its mutable fields in place. CreatedAt is intentionally never updated by
// the DO UPDATE clause so a job's original enqueue time is preserved.
const upsertQuery = `
INSERT INTO jobs (` + jobColumns + `)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    queue           = excluded.queue,
    payload         = excluded.payload,
    status          = excluded.status,
    priority        = excluded.priority,
    max_retries     = excluded.max_retries,
    retry_count     = excluded.retry_count,
    last_error      = excluded.last_error,
    updated_at      = excluded.updated_at,
    run_at          = excluded.run_at,
    timeout_ns      = excluded.timeout_ns,
    idempotency_key = excluded.idempotency_key;
`

// idempotentMatchQuery finds an existing job sharing j's (queue,
// idempotency_key) pair, so a repeated Enqueue with the same key can adopt
// the original job's state instead of creating a duplicate.
const idempotentMatchQuery = `
SELECT ` + jobColumns + ` FROM jobs WHERE queue = ? AND idempotency_key = ?;
`

// Enqueue implements queue.Queue. If j has a non-empty IdempotencyKey and a
// job with the same (Queue, IdempotencyKey) pair already exists, Enqueue
// does not insert a new row: it copies the existing job's fields into *j
// and returns nil, so the caller observes the original job rather than a
// duplicate.
func (q *Queue) Enqueue(j *job.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return queue.ErrClosed
	}
	q.mu.Unlock()

	if j.IdempotencyKey != "" {
		if existing, err := q.findByIdempotencyKey(j.Queue, j.IdempotencyKey); err == nil {
			*j = *existing
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sqlite: enqueue %s: %w", j.ID, err)
		}
	}

	if _, err := q.db.Exec(upsertQuery, insertArgs(j)...); err != nil {
		if j.IdempotencyKey != "" && isUniqueConstraintErr(err) {
			// Lost a race with a concurrent Enqueue for the same
			// idempotency key: adopt whichever job won.
			if existing, ferr := q.findByIdempotencyKey(j.Queue, j.IdempotencyKey); ferr == nil {
				*j = *existing
				return nil
			}
		}
		return fmt.Errorf("sqlite: enqueue %s: %w", j.ID, err)
	}

	q.mu.Lock()
	q.notifyLocked()
	q.mu.Unlock()
	return nil
}

// findByIdempotencyKey looks up the job sharing (queue, key), returning
// sql.ErrNoRows if none exists.
func (q *Queue) findByIdempotencyKey(queueName, key string) (*job.Job, error) {
	return scanJob(q.db.QueryRow(idempotentMatchQuery, queueName, key))
}

// isUniqueConstraintErr reports whether err was caused by a UNIQUE
// constraint violation, matched on the driver's error message to avoid this
// package taking a direct dependency on modernc.org/sqlite's error type.
func isUniqueConstraintErr(err error) bool {
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}
