// Package queue defines the Queue abstraction used by workers to pull jobs
// for execution, along with an in-memory reference implementation.
package queue

import (
	"context"
	"errors"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// ErrEmpty is returned by non-blocking dequeue operations when no job is
// currently ready to run.
var ErrEmpty = errors.New("queue: no ready jobs")

// ErrClosed is returned when an operation is attempted on a closed queue.
var ErrClosed = errors.New("queue: closed")

// ErrNotFound is returned by ID-based lookups (e.g. Get, Cancel) when no job
// with the given ID exists.
var ErrNotFound = errors.New("queue: job not found")

// ErrNotCancelable is returned by Cancel when the job exists but is not in a
// cancelable state (already running or already terminal).
var ErrNotCancelable = errors.New("queue: job not cancelable")

// Queue is the interface workers and producers use to exchange jobs. A Queue
// implementation is responsible for ordering ready jobs by priority and
// respecting each job's scheduled RunAt time.
type Queue interface {
	// Enqueue adds j to the queue. It returns ErrClosed if the queue has
	// been closed.
	Enqueue(j *job.Job) error

	// Dequeue blocks until a job is ready to run, ctx is cancelled, or the
	// queue is closed. On success it returns the job with StatusRunning
	// already applied. Returns ctx.Err() on cancellation and ErrClosed if
	// the queue is closed while waiting.
	Dequeue(ctx context.Context) (*job.Job, error)

	// TryDequeue returns immediately. It returns ErrEmpty if no job is
	// currently ready.
	TryDequeue() (*job.Job, error)

	// Len returns the number of jobs currently waiting (pending or
	// scheduled) in the queue.
	Len() int

	// Close shuts the queue down, causing any blocked Dequeue calls to
	// return ErrClosed and future Enqueue/Dequeue calls to fail.
	Close() error
}
