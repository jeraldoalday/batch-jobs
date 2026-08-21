package sqlite

import (
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// requeueExpiredQuery hands jobs whose lease lapsed back to the queue,
// spending one of their retries in the process. Charging the attempt is
// deliberate: a job that reliably kills the process running it (an OOM on a
// huge payload, a panic in a cgo dependency) would otherwise be recovered,
// re-run, and crash the next worker forever. Bounding recovery by
// max_retries means such a job eventually lands in the dead-letter queue
// like any other repeat failure, where an operator can see it.
//
// The error message is built from the row's own owner column so it names
// the process that vanished, which is the first thing anyone investigating
// wants to know.
const requeueExpiredQuery = `
UPDATE jobs
SET status           = 'pending',
    retry_count      = retry_count + 1,
    last_error       = 'lease expired while held by ' || owner,
    run_at           = ?1,
    updated_at       = ?1,
    owner            = '',
    lease_expires_at = 0
WHERE status = 'running' AND lease_expires_at != 0 AND lease_expires_at <= ?1
  AND retry_count < max_retries;
`

// deadLetterExpiredQuery retires jobs whose lease lapsed with no retries
// left. They have already been run as often as they were allowed to be, so
// there is nowhere to put them but the dead-letter state.
const deadLetterExpiredQuery = `
UPDATE jobs
SET status           = 'dead',
    last_error       = 'lease expired while held by ' || owner || ' (no retries left)',
    updated_at       = ?1,
    owner            = '',
    lease_expires_at = 0
WHERE status = 'running' AND lease_expires_at != 0 AND lease_expires_at <= ?1
  AND retry_count >= max_retries;
`

// expiredCountQuery counts claims that have lapsed but not yet been
// recovered, for monitoring: a number that stays above zero between reaper
// runs means jobs are being orphaned faster than they are being reclaimed.
const expiredCountQuery = `
SELECT COUNT(*) FROM jobs
WHERE status = 'running' AND lease_expires_at != 0 AND lease_expires_at <= ?;
`

var _ queue.Recoverer = (*Queue)(nil)

// Recover reclaims jobs whose lease has expired: their worker claimed them
// and then died without reporting an outcome. Each is either put back in
// the queue for another attempt or, if its retries are already spent, moved
// to the dead-letter state.
//
// Recovery is safe to run from several processes at once and while workers
// are running: both statements are set-based UPDATEs filtered on the lease
// having actually lapsed, so a job whose worker is alive and heartbeating
// is never touched, and two racing reapers cannot both reclaim the same
// job.
func (q *Queue) Recover() (queue.Recovery, error) {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return queue.Recovery{}, queue.ErrClosed
	}

	now := time.Now().UTC().UnixNano()

	tx, err := q.db.Begin()
	if err != nil {
		return queue.Recovery{}, fmt.Errorf("sqlite: recover: %w", err)
	}
	defer tx.Rollback()

	// Dead-lettering runs first: it is the narrower predicate, and running
	// it after the requeue would catch jobs the requeue had just pushed
	// over their retry limit and retire them without ever re-running them.
	dead, err := affected(tx.Exec(deadLetterExpiredQuery, now))
	if err != nil {
		return queue.Recovery{}, fmt.Errorf("sqlite: recover: dead-letter expired: %w", err)
	}

	requeued, err := affected(tx.Exec(requeueExpiredQuery, now))
	if err != nil {
		return queue.Recovery{}, fmt.Errorf("sqlite: recover: requeue expired: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return queue.Recovery{}, fmt.Errorf("sqlite: recover: commit: %w", err)
	}

	if requeued > 0 {
		// Wake anything blocked in Dequeue: the recovered jobs are ready
		// now, and waiting out a poll interval to notice would add
		// latency to work that has already been delayed by a crash.
		q.mu.Lock()
		q.notifyLocked()
		q.mu.Unlock()
	}

	return queue.Recovery{Requeued: requeued, DeadLettered: dead}, nil
}

// ExpiredLeases returns how many claims have lapsed without yet being
// recovered.
func (q *Queue) ExpiredLeases() (int, error) {
	var n int
	err := q.db.QueryRow(expiredCountQuery, time.Now().UTC().UnixNano()).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("sqlite: expired leases: %w", err)
	}
	return n, nil
}

// affected adapts an Exec result pair into a row count, so the two recovery
// statements read as one line each.
func affected(res interface{ RowsAffected() (int64, error) }, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}
