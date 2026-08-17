package sqlite

import (
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// retryQuery resets a job back to pending, but only if it is currently dead
// or failed. Retry count and last error are cleared and run_at is reset to
// now, so the job is immediately eligible for another dequeue attempt.
const retryQuery = `
UPDATE jobs
SET status = 'pending', retry_count = 0, last_error = '', run_at = ?, updated_at = ?
WHERE id = ? AND status IN ('dead', 'failed');
`

// Retry resets a dead or failed job back to pending, clearing its retry
// count and last error so it gets a fresh set of attempts. It returns
// queue.ErrNotFound if no job with that ID exists, and
// queue.ErrNotRetryable if the job exists but isn't dead or failed.
func (q *Queue) Retry(id string) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return queue.ErrClosed
	}

	now := time.Now().UTC().UnixNano()
	res, err := q.db.Exec(retryQuery, now, now, id)
	if err != nil {
		return fmt.Errorf("sqlite: retry %s: %w", id, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: retry %s: %w", id, err)
	}
	if n > 0 {
		q.mu.Lock()
		q.notifyLocked()
		q.mu.Unlock()
		return nil
	}

	// Nothing was updated: either the job doesn't exist, or it exists but
	// wasn't dead/failed. Get distinguishes the two.
	if _, err := q.Get(id); err != nil {
		return err
	}
	return queue.ErrNotRetryable
}
