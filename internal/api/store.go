// Package api exposes a Store of jobs over HTTP: enqueueing, looking up and
// listing jobs by status, cancelling, and summary stats. It is transport
// only — all persistence and lifecycle logic lives in the queue package and
// its backends.
package api

import "github.com/jeraldoalday/batch-jobs/internal/job"

// Store is the persistence surface the HTTP API needs. It is a superset of
// queue.Queue's producer/consumer methods, adding the ID- and status-based
// lookups a REST API requires. queue/sqlite.Queue satisfies it directly;
// queue.Memory does not, since it discards terminal jobs and keeps no index
// by ID.
type Store interface {
	// Enqueue adds j to the store, as queue.Queue.Enqueue does.
	Enqueue(j *job.Job) error

	// Get returns the job with the given ID, or queue.ErrNotFound.
	Get(id string) (*job.Job, error)

	// List returns up to limit jobs in status, most recently updated first.
	List(status job.Status, limit int) ([]*job.Job, error)

	// Cancel marks the job with the given ID as cancelled. It returns
	// queue.ErrNotFound if the job doesn't exist and queue.ErrNotCancelable
	// if it exists but isn't pending or retrying.
	Cancel(id string) error

	// Retry resets the job with the given ID back to pending, clearing its
	// retry count and last error. It returns queue.ErrNotFound if the job
	// doesn't exist and queue.ErrNotRetryable if it exists but isn't dead
	// or failed.
	Retry(id string) error

	// Stats returns the number of jobs currently in each status.
	Stats() (map[job.Status]int, error)
}
