package sqlite

import "github.com/jeraldoalday/batch-jobs/internal/queue"

var _ queue.Queue = (*Queue)(nil)

// Len implements queue.Queue. It counts jobs that are pending or awaiting
// retry, regardless of whether their run_at has passed yet, matching
// queue.Memory's definition of "waiting" jobs.
func (q *Queue) Len() int {
	var n int
	// Errors here (e.g. the db already closed concurrently) are treated as
	// "nothing waiting" rather than surfaced, since Len has no error return
	// in the queue.Queue interface.
	_ = q.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE status IN ('pending', 'retrying')`).Scan(&n)
	return n
}

// Close implements queue.Queue.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}
	q.closed = true
	q.notifyLocked()
	return q.db.Close()
}
