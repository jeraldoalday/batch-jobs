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
// pollInterval.
const nextRunAtQuery = `
SELECT MIN(run_at) FROM jobs WHERE status IN ('pending', 'retrying');
`

// claimQuery atomically picks the highest-priority, oldest, ready job and
// marks it running in a single statement, so two connections racing to
// dequeue can never claim the same row. "Ready" means the job is pending
// or awaiting retry and its scheduled run_at has passed.
const claimQuery = `
UPDATE jobs SET status = 'running', updated_at = ?1
WHERE id = (
    SELECT id FROM jobs
    WHERE status IN ('pending', 'retrying') AND run_at <= ?1
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
)
RETURNING ` + jobColumns + `;
`

// TryDequeue implements queue.Queue.
func (q *Queue) TryDequeue() (*job.Job, error) {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return nil, queue.ErrClosed
	}

	now := time.Now().UTC()
	row := q.db.QueryRow(claimQuery, now.UnixNano())

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
	for {
		j, err := q.TryDequeue()
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

		timer := time.NewTimer(q.waitDuration())
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
// the soonest delayed job becomes ready, whichever is shorter.
func (q *Queue) waitDuration() time.Duration {
	wait := q.pollInterval

	var nextRunAt sql.NullInt64
	if err := q.db.QueryRow(nextRunAtQuery).Scan(&nextRunAt); err != nil || !nextRunAt.Valid {
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
