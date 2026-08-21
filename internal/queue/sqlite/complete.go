package sqlite

import (
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

var _ queue.Completer = (*Queue)(nil)

// completeQuery records how a claimed job ended and releases its lease.
//
// The status guard is what keeps recovery and completion from fighting
// over the same row: if the job is no longer 'running', this worker's
// claim was already taken away and another worker may be part-way through
// the job. Overwriting the row with this attempt's outcome would then
// erase a rescheduled retry, or mark a job succeeded that is currently
// being run somewhere else.
const completeQuery = `
UPDATE jobs
SET status           = ?1,
    last_error       = ?2,
    retry_count      = ?3,
    updated_at       = ?4,
    owner            = '',
    lease_expires_at = 0
WHERE id = ?5 AND status = 'running';
`

// Complete records a claimed job's terminal state, so the stored row stops
// saying "running" once the work is over.
//
// It is the counterpart to the claim in Dequeue: a job that is retried
// comes back through Enqueue, but one that succeeded, failed permanently,
// or exhausted its retries has nothing left to enqueue and would otherwise
// stay claimed forever -- counted as in-flight by every stats query, and,
// once leases are in play, eventually re-run by recovery as an orphan.
//
// Complete returns queue.ErrNotFound if the job is unknown, and a
// *LeaseLostError if the job exists but is no longer running under this
// claim.
func (q *Queue) Complete(j *job.Job) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return queue.ErrClosed
	}

	if !j.Status.Terminal() {
		return fmt.Errorf("sqlite: complete %s: status %q is not terminal", j.ID, j.Status)
	}

	res, err := q.db.Exec(completeQuery,
		string(j.Status), j.LastError, j.RetryCount, time.Now().UTC().UnixNano(), j.ID)
	if err != nil {
		return fmt.Errorf("sqlite: complete %s: %w", j.ID, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: complete %s: %w", j.ID, err)
	}
	if n > 0 {
		return nil
	}
	return q.diagnoseLostLease(j.ID)
}

// Complete implements queue.Completer for a bound view, delegating to the
// underlying Queue. As with Heartbeat, jobs are addressed by ID, which is
// unique across every logical queue sharing the database.
func (b *Bound) Complete(j *job.Job) error { return b.q.Complete(j) }
