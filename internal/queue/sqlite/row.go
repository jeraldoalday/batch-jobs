package sqlite

import (
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// scanner is satisfied by both *sql.Row and *sql.Rows, letting scanJob be
// used from single-row claim queries and multi-row listing queries alike.
type scanner interface {
	Scan(dest ...any) error
}

// jobColumns is the column list, in order, used by every SELECT and
// RETURNING clause that produces a full job row. Keeping it in one place
// keeps scanJob in sync with the queries that feed it.
const jobColumns = "id, queue, payload, status, priority, max_retries, retry_count, last_error, created_at, updated_at, run_at"

// scanJob reads one row in the jobColumns order into a *job.Job.
func scanJob(s scanner) (*job.Job, error) {
	var (
		j         job.Job
		priority  int
		createdAt int64
		updatedAt int64
		runAt     int64
	)

	if err := s.Scan(
		&j.ID,
		&j.Queue,
		&j.Payload,
		&j.Status,
		&priority,
		&j.MaxRetries,
		&j.RetryCount,
		&j.LastError,
		&createdAt,
		&updatedAt,
		&runAt,
	); err != nil {
		return nil, err
	}

	j.Priority = job.Priority(priority)
	j.CreatedAt = time.Unix(0, createdAt).UTC()
	j.UpdatedAt = time.Unix(0, updatedAt).UTC()
	j.RunAt = time.Unix(0, runAt).UTC()
	return &j, nil
}

// insertArgs returns j's fields in jobColumns order, ready to be passed as
// the argument list for an INSERT/upsert statement.
func insertArgs(j *job.Job) []any {
	return []any{
		j.ID,
		j.Queue,
		j.Payload,
		string(j.Status),
		int(j.Priority),
		j.MaxRetries,
		j.RetryCount,
		j.LastError,
		j.CreatedAt.UnixNano(),
		j.UpdatedAt.UnixNano(),
		j.RunAt.UnixNano(),
	}
}
