package sqlite

import (
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

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

// List returns up to limit jobs in status, most recently updated first.
// Unlike queue.Memory, Queue never discards terminal jobs (succeeded,
// failed, dead), so List is how a caller inspects them after the fact —
// e.g. a dashboard's dead-letter view.
func (q *Queue) List(status job.Status, limit int) ([]*job.Job, error) {
	rows, err := q.db.Query(
		`SELECT `+jobColumns+` FROM jobs WHERE status = ? ORDER BY updated_at DESC LIMIT ?`,
		string(status), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list %s: %w", status, err)
	}
	defer rows.Close()

	var jobs []*job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: list %s: scan: %w", status, err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list %s: %w", status, err)
	}
	return jobs, nil
}

// Stats returns the number of jobs currently in each status.
func (q *Queue) Stats() (map[job.Status]int, error) {
	rows, err := q.db.Query(`SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[job.Status]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("sqlite: stats: scan: %w", err)
		}
		stats[job.Status(status)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: stats: %w", err)
	}
	return stats, nil
}
