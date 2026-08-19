package queue

import "github.com/jeraldoalday/batch-jobs/internal/job"

// MemoryOption configures a Memory queue at construction time.
type MemoryOption func(*Memory)

// WithCapacity bounds how much waiting work a Memory queue will accept,
// turning an unbounded queue into one that applies backpressure: once
// capacity jobs are waiting, Enqueue rejects further submissions with
// ErrFull instead of growing without limit until the process runs out of
// memory.
//
// A non-positive n leaves the queue unbounded, which is the default.
func WithCapacity(n int) MemoryOption {
	return func(q *Memory) {
		if n > 0 {
			q.capacity = n
		}
	}
}

// admits reports whether j is subject to capacity-based admission control.
//
// Only jobs still in StatusPending — that is, work being submitted to the
// queue for the first time — are rejected when the queue is full. A job
// coming back around for a retry, or being moved into a dead-letter queue,
// was already admitted once and counted against capacity; bouncing it now
// would drop work the queue has already accepted responsibility for, which
// is exactly the failure mode backpressure is supposed to prevent. Such
// jobs are always let through, so a bounded queue can briefly exceed its
// capacity by the number of jobs currently in flight.
func admits(j *job.Job) bool {
	return j.Status == job.StatusPending
}
