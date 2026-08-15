package sqlite

import (
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// cancelQuery moves a job to cancelled, but only if it is still pending or
// awaiting retry. A running job (already claimed by a worker) or a job
// already in a terminal state is left untouched; RowsAffected distinguishes
// that from a successful cancellation.
const cancelQuery = `
UPDATE jobs SET status = 'cancelled', updated_at = ?
WHERE id = ? AND status IN ('pending', 'retrying');
`

// Cancel marks the job with the given ID as cancelled, preventing it from
// ever being dequeued. It returns queue.ErrNotFound if no job with that ID
// exists, and queue.ErrNotCancelable if the job exists but is running or
// already in a terminal state.
func (q *Queue) Cancel(id string) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return queue.ErrClosed
	}

	res, err := q.db.Exec(cancelQuery, time.Now().UTC().UnixNano(), id)
	if err != nil {
		return fmt.Errorf("sqlite: cancel %s: %w", id, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: cancel %s: %w", id, err)
	}
	if n > 0 {
		return nil
	}

	// Nothing was updated: either the job doesn't exist, or it exists but
	// wasn't pending/retrying. Get distinguishes the two.
	if _, err := q.Get(id); err != nil {
		return err
	}
	return queue.ErrNotCancelable
}
