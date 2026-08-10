package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

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
