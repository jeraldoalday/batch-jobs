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

// ErrFull is returned by Enqueue when a capacity-limited queue is already
// holding as much newly submitted work as it is allowed to. It is the
// queue's backpressure signal: producers should slow down, retry later, or
// shed the request rather than treating it as a permanent failure.
var ErrFull = errors.New("queue: full")

// ErrNotFound is returned by ID-based lookups (e.g. Get, Cancel) when no job
// with the given ID exists.
var ErrNotFound = errors.New("queue: job not found")

// ErrNotCancelable is returned by Cancel when the job exists but is not in a
// cancelable state (already running or already terminal).
var ErrNotCancelable = errors.New("queue: job not cancelable")

// ErrNotRetryable is returned by Retry when the job exists but is not in a
// retryable state (anything other than dead or failed).
var ErrNotRetryable = errors.New("queue: job not retryable")

// ErrLeaseLost is returned (or wrapped) by a Queue that leases claimed jobs
// when the caller no longer holds the lease on the job it names: recovery
// reclaimed it after the lease expired, or it reached a terminal state some
// other way.
//
// It is a sentinel in this package rather than a concrete type in the
// backend that produces it so that consumers -- worker.Pool above all --
// can react to lease loss without importing a specific queue
// implementation.
var ErrLeaseLost = errors.New("queue: lease lost")

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

// BatchEnqueuer is implemented by queues that can admit several jobs in
// one round trip.
//
// It exists because a durable backend's per-job cost is dominated by the
// transaction around the write, not by the write itself: committing N jobs
// once is far cheaper than committing once per job. A producer with a
// batch in hand -- a nightly fan-out, an import, a retry sweep -- should
// not have to pay N times for something the backend can do once.
//
// Callers that want to work against any Queue should type-assert for this
// and fall back to a loop of Enqueue, which is always correct if slower.
type BatchEnqueuer interface {
	// EnqueueBatch adds every job in jobs, atomically: either all of them
	// are admitted or none are. Each *job.Job is updated in place the way
	// Enqueue updates it, so a job deduped against an existing
	// idempotency key comes back describing the original.
	//
	// An empty slice is a no-op success. A nil element is an error, and
	// leaves the whole batch unadmitted.
	EnqueueBatch(jobs []*job.Job) error
}

// EnqueueBatch adds every job in jobs to q, using q's own batch support if
// it has any and a loop of Enqueue otherwise. It is the call a producer
// should make when it has more than one job in hand and does not know
// which kind of queue it is holding.
//
// The fallback is not atomic: a queue without batch support that fails
// partway leaves the jobs before the failure admitted. That is the same
// thing a caller writing the loop itself would get, so the difference is
// only worth knowing about when the atomicity matters, in which case
// assert for BatchEnqueuer directly.
func EnqueueBatch(q Queue, jobs []*job.Job) error {
	if b, ok := q.(BatchEnqueuer); ok {
		return b.EnqueueBatch(jobs)
	}
	for _, j := range jobs {
		if err := q.Enqueue(j); err != nil {
			return err
		}
	}
	return nil
}
