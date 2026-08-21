package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// nextRunAtQuery finds the soonest run_at among not-yet-ready jobs, so
// Dequeue can size its wait timer precisely instead of always polling at
// pollInterval. The ?1 = ” escape makes the queue-name filter optional in
// a single prepared statement, rather than needing two near-identical
// queries.
const nextRunAtQuery = `
SELECT MIN(run_at) FROM jobs
WHERE status IN ('pending', 'retrying') AND (?1 = '' OR queue = ?1);
`

// claimQuery atomically picks the highest-priority, oldest, ready job and
// marks it running in a single statement, so two connections racing to
// dequeue can never claim the same row. "Ready" means the job is pending
// or awaiting retry and its scheduled run_at has passed. An empty ?2
// claims from any queue; otherwise only jobs on that named queue are
// eligible.
//
// The same statement stamps the claim with its holder (?3) and its expiry
// (?4), so a job is never momentarily running without a lease: a crash
// between claiming and leasing would leave exactly the orphan the lease
// exists to catch.
const claimQuery = `
UPDATE jobs SET status = 'running', updated_at = ?1, owner = ?3, lease_expires_at = ?4
WHERE id = (
    SELECT id FROM jobs
    WHERE status IN ('pending', 'retrying') AND run_at <= ?1
      AND (?2 = '' OR queue = ?2)
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
)
RETURNING ` + jobColumns + `;
`

// TryDequeue implements queue.Queue.
func (q *Queue) TryDequeue() (*job.Job, error) {
	return q.tryClaim("")
}

// tryClaim claims the next ready job, restricted to the named queue when
// name is non-empty. It backs both the whole-database TryDequeue and the
// per-name views returned by Bind.
func (q *Queue) tryClaim(name string) (*job.Job, error) {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return nil, queue.ErrClosed
	}

	now := time.Now().UTC()
	row := q.db.QueryRow(claimQuery, now.UnixNano(), name, q.owner, q.leaseExpiry(now))

	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, queue.ErrEmpty
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: try dequeue: %w", err)
	}
	return j, nil
}

// Dequeue implements queue.Queue.
func (q *Queue) Dequeue(ctx context.Context) (*job.Job, error) {
	return q.blockingDequeue(ctx, "")
}

// blockingDequeue retries tryClaim until a job turns up, ctx is cancelled,
// or the queue is closed, waiting on either an Enqueue notification or the
// poll interval in between.
func (q *Queue) blockingDequeue(ctx context.Context, name string) (*job.Job, error) {
	for {
		j, err := q.tryClaim(name)
		if err == nil {
			return j, nil
		}
		if !errors.Is(err, queue.ErrEmpty) {
			return nil, err
		}

		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, queue.ErrClosed
		}
		waitCh := q.changed
		q.mu.Unlock()

		timer := time.NewTimer(q.waitDuration(name))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-waitCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// waitDuration bounds how long Dequeue sleeps before re-checking the
// database: no longer than pollInterval, and no longer than the time until
// the soonest delayed job becomes ready, whichever is shorter. A non-empty
// name restricts that lookahead to one queue, so a bound view is not woken
// early by another queue's delayed job.
func (q *Queue) waitDuration(name string) time.Duration {
	wait := q.pollInterval

	var nextRunAt sql.NullInt64
	if err := q.db.QueryRow(nextRunAtQuery, name).Scan(&nextRunAt); err != nil || !nextRunAt.Valid {
		return wait
	}

	if d := time.Until(time.Unix(0, nextRunAt.Int64).UTC()); d < wait {
		if d < 0 {
			d = 0
		}
		wait = d
	}
	return wait
}
