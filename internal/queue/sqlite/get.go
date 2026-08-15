package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// getQuery fetches a single job row by ID, regardless of status, so callers
// (e.g. the HTTP API) can inspect a job at any point in its lifecycle.
const getQuery = `SELECT ` + jobColumns + ` FROM jobs WHERE id = ?;`

// Get returns the job with the given ID, or queue.ErrNotFound if no such job
// exists.
func (q *Queue) Get(id string) (*job.Job, error) {
	row := q.db.QueryRow(getQuery, id)

	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, queue.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: get %s: %w", id, err)
	}
	return j, nil
}
