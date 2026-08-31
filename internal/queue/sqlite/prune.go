package sqlite

import (
	"fmt"
	"strings"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// pruneBatchQuery deletes up to one batch of jobs in a terminal status
// that have sat there since before a cutoff.
//
// The delete is driven by a subquery selecting IDs rather than applying
// the predicate directly, because SQLite's DELETE ... LIMIT is a compile
// -time option that is not enabled in the pure-Go driver this package
// uses. Selecting the IDs first gets the same bounded delete out of the
// build we actually have, and costs nothing: the subquery is answered
// from idx_jobs_retention, and the outer delete is a primary-key lookup
// per row.
//
// ORDER BY updated_at makes each batch take the oldest rows first. It is
// free -- it is the order the index already yields -- and it means a pass
// interrupted halfway has deleted the oldest half rather than an
// arbitrary half.
const pruneBatchQuery = `
DELETE FROM jobs WHERE id IN (
    SELECT id FROM jobs
    WHERE status = ?1 AND updated_at < ?2
    ORDER BY updated_at
    LIMIT ?3
);
`

var _ queue.Pruner = (*Queue)(nil)

// Prune deletes terminal jobs older than r allows and reports what it
// removed. A job's updated_at is the moment it reached its terminal
// status, so "older than" means "finished longer ago than", not "was
// created longer ago than": a job that sat in the queue for a week and
// succeeded a minute ago is a minute old for retention purposes.
//
// The pass deletes in batches, each its own statement and so its own
// implicit transaction, rather than issuing one delete for everything
// eligible. A single unbounded delete would hold the write lock for as
// long as it took to remove the whole history, and SQLite has one writer:
// every enqueue, claim, and completion in every process sharing the file
// would block behind it. Batching gives that lock up between rounds, so
// pruning a large backlog slows the queue down instead of stopping it.
//
// Because the batches are separate transactions, an error partway through
// leaves the rows already deleted deleted. That is the right trade for
// this operation -- the work is idempotent, and the next pass simply
// continues -- but it means the returned counts describe what was removed
// before the failure, not a rolled-back nothing.
//
// Prune is safe to run concurrently with workers and with other pruners:
// it only ever touches rows that are already in a terminal state, which
// nothing else in the queue modifies.
func (q *Queue) Prune(r queue.Retention) (queue.Pruned, error) {
	var pruned queue.Pruned

	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return pruned, queue.ErrClosed
	}

	if err := r.Validate(); err != nil {
		return pruned, err
	}
	if !r.Enabled() {
		return pruned, queue.ErrNoRetention
	}

	batch := r.Batch()
	for _, cutoff := range r.Cutoffs(time.Now()) {
		before := cutoff.Before.UnixNano()
		for {
			n, err := affected(q.db.Exec(pruneBatchQuery, string(cutoff.Status), before, batch))
			if err != nil {
				return pruned, fmt.Errorf("sqlite: prune %s: %w", cutoff.Status, err)
			}
			pruned.Add(cutoff.Status, n)

			// A short batch means the cutoff has been reached. The
			// cutoff is fixed for the whole pass, so rows finishing
			// while it runs are never eligible and the loop cannot be
			// kept alive by ongoing work.
			if n < batch {
				break
			}
		}
	}

	return pruned, nil
}

// retainedCountQuery counts the rows a retention policy governs: jobs that
// have finished and are being kept only as history.
var retainedCountQuery = fmt.Sprintf(
	`SELECT COUNT(*) FROM jobs WHERE status IN (%s);`,
	strings.TrimSuffix(strings.Repeat("?,", len(queue.TerminalStatuses)), ","),
)

// terminalArgs is the TerminalStatuses list as query arguments, built once
// since it never changes.
var terminalArgs = func() []any {
	args := make([]any, len(queue.TerminalStatuses))
	for i, status := range queue.TerminalStatuses {
		args[i] = string(status)
	}
	return args
}()

// Retained reports how many finished jobs the queue is currently holding.
//
// It is the number to watch to know whether retention is working: a queue
// with a policy should settle at roughly the amount of history that policy
// permits, while one climbing steadily is either not being pruned or is
// finishing jobs faster than a pass can delete them.
func (q *Queue) Retained() (int, error) {
	var n int
	if err := q.db.QueryRow(retainedCountQuery, terminalArgs...).Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlite: retained: %w", err)
	}
	return n, nil
}

// OldestRetained returns when the oldest finished job the queue still
// holds reached its terminal status, or the zero time if there is no
// finished job at all.
//
// It answers the question the count cannot: whether the history being kept
// is the history the policy asked for. A queue whose oldest succeeded job
// finished three weeks ago under a one-day policy is not being pruned,
// however small the count happens to be.
func (q *Queue) OldestRetained() (time.Time, error) {
	query := fmt.Sprintf(
		`SELECT MIN(updated_at) FROM jobs WHERE status IN (%s);`,
		strings.TrimSuffix(strings.Repeat("?,", len(queue.TerminalStatuses)), ","),
	)

	// MIN over an empty set is SQL NULL, which is the "no finished jobs"
	// case rather than an error.
	var oldest *int64
	if err := q.db.QueryRow(query, terminalArgs...).Scan(&oldest); err != nil {
		return time.Time{}, fmt.Errorf("sqlite: oldest retained: %w", err)
	}
	if oldest == nil {
		return time.Time{}, nil
	}
	return time.Unix(0, *oldest).UTC(), nil
}
