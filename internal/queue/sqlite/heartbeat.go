package sqlite

import (
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// heartbeatQuery pushes a running job's lease out by another TTL. The
// status guard is what makes a heartbeat meaningful: once recovery has
// taken the job back it is no longer 'running', so the update matches
// nothing and the caller learns it lost the claim instead of silently
// re-leasing a job someone else is now executing.
const heartbeatQuery = `
UPDATE jobs SET lease_expires_at = ?, updated_at = ?
WHERE id = ? AND status = 'running' AND lease_expires_at != 0;
`

// Heartbeat extends the lease on a running job by the Queue's lease TTL,
// so a job that takes longer than one TTL to run keeps its claim for as
// long as the worker executing it is alive.
//
// It returns queue.ErrNotFound if no such job exists, and a *LeaseLostError
// if the job exists but is no longer running under a live lease -- meaning
// recovery reclaimed it, or it was cancelled, and the caller should abandon
// the work it is doing. When leasing is disabled Heartbeat is a successful
// no-op, so callers need not check whether the Queue leases before calling.
func (q *Queue) Heartbeat(id string) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return queue.ErrClosed
	}
	if q.leaseTTL <= 0 {
		return nil
	}

	now := time.Now().UTC()
	res, err := q.db.Exec(heartbeatQuery, q.leaseExpiry(now), now.UnixNano(), id)
	if err != nil {
		return fmt.Errorf("sqlite: heartbeat %s: %w", id, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: heartbeat %s: %w", id, err)
	}
	if n > 0 {
		return nil
	}
	return q.diagnoseLostLease(id)
}

// diagnoseLostLease explains why a heartbeat matched no rows: either the
// job is gone entirely, or it is no longer the caller's to hold.
func (q *Queue) diagnoseLostLease(id string) error {
	j, err := q.Get(id)
	if err != nil {
		if errors.Is(err, queue.ErrNotFound) {
			return queue.ErrNotFound
		}
		return fmt.Errorf("sqlite: heartbeat %s: %w", id, err)
	}
	return &LeaseLostError{JobID: id, Status: string(j.Status)}
}
