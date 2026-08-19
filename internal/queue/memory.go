package queue

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// Memory is an in-memory, process-local Queue implementation. It is safe
// for concurrent use by multiple goroutines. State is not persisted:
// restarting the process loses any jobs that have not completed.
type Memory struct {
	mu      sync.Mutex
	ready   readyHeap
	delayed delayedHeap
	closed  bool
	// changed is closed and replaced under mu whenever the queue's state
	// changes in a way that might unblock a waiting Dequeue call.
	changed chan struct{}
	// waiting indexes, by dedupeKey, every job currently sitting in ready
	// or delayed that was enqueued with a non-empty IdempotencyKey. It
	// exists so Enqueue can detect a duplicate before it is claimed by a
	// worker; entries are removed the moment a job is dequeued (Memory
	// keeps no record of jobs once a worker has claimed them, so dedupe
	// against in-flight or terminal jobs is out of scope here — see
	// queue/sqlite.Queue for dedupe that persists across a job's full
	// lifecycle).
	waiting map[string]*job.Job
	// capacity, when positive, is the maximum number of newly submitted
	// jobs the queue will hold before Enqueue starts returning ErrFull.
	// Zero means unbounded.
	capacity int
}

// NewMemory returns an empty, ready-to-use in-memory queue, configured by
// any options (e.g. WithCapacity to bound it).
func NewMemory(opts ...MemoryOption) *Memory {
	q := &Memory{changed: make(chan struct{}), waiting: make(map[string]*job.Job)}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Cap returns the queue's capacity, or 0 if it is unbounded.
func (q *Memory) Cap() int { return q.capacity }

// dedupeKey scopes an idempotency key to the job's queue, matching
// queue/sqlite.Queue's (queue, idempotency_key) uniqueness scope.
func dedupeKey(j *job.Job) string {
	return j.Queue + "\x00" + j.IdempotencyKey
}

// removeWaitingLocked drops j from the waiting index once it has been
// claimed by a worker. Callers must hold q.mu.
func (q *Memory) removeWaitingLocked(j *job.Job) {
	if j.IdempotencyKey != "" {
		delete(q.waiting, dedupeKey(j))
	}
}

var _ Queue = (*Memory)(nil)

func (q *Memory) broadcastLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

// promoteLocked moves any delayed jobs whose RunAt has passed into the
// ready heap. Callers must hold q.mu.
func (q *Memory) promoteLocked(now time.Time) {
	for q.delayed.Len() > 0 && !q.delayed[0].RunAt.After(now) {
		j := heap.Pop(&q.delayed).(*job.Job)
		heap.Push(&q.ready, j)
	}
}

// Enqueue implements Queue. If j has a non-empty IdempotencyKey and another
// job with the same (Queue, IdempotencyKey) pair is currently waiting
// (ready or delayed, not yet claimed by a worker), Enqueue does not add a
// new entry: it copies that job's fields into *j and returns nil.
//
// On a queue constructed WithCapacity, Enqueue returns ErrFull once that
// many jobs are already waiting. Deduplicated and already-admitted jobs
// (retries, dead letters) bypass the check, since neither adds new work.
func (q *Memory) Enqueue(j *job.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}

	if j.IdempotencyKey != "" {
		if existing, ok := q.waiting[dedupeKey(j)]; ok {
			*j = *existing
			return nil
		}
	}

	if q.capacity > 0 && admits(j) && q.ready.Len()+q.delayed.Len() >= q.capacity {
		return ErrFull
	}

	if !j.RunAt.After(time.Now().UTC()) {
		heap.Push(&q.ready, j)
	} else {
		heap.Push(&q.delayed, j)
	}
	if j.IdempotencyKey != "" {
		q.waiting[dedupeKey(j)] = j
	}
	q.broadcastLocked()
	return nil
}

// TryDequeue implements Queue.
func (q *Memory) TryDequeue() (*job.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now().UTC()
	q.promoteLocked(now)

	if q.ready.Len() == 0 {
		if q.closed {
			return nil, ErrClosed
		}
		return nil, ErrEmpty
	}

	j := heap.Pop(&q.ready).(*job.Job)
	q.removeWaitingLocked(j)
	j.MarkRunning(now)
	return j, nil
}

// Dequeue implements Queue.
func (q *Memory) Dequeue(ctx context.Context) (*job.Job, error) {
	for {
		q.mu.Lock()
		now := time.Now().UTC()
		q.promoteLocked(now)

		if q.ready.Len() > 0 {
			j := heap.Pop(&q.ready).(*job.Job)
			q.removeWaitingLocked(j)
			j.MarkRunning(now)
			q.mu.Unlock()
			return j, nil
		}

		if q.closed {
			q.mu.Unlock()
			return nil, ErrClosed
		}

		waitCh := q.changed

		var timer *time.Timer
		var timerC <-chan time.Time
		if q.delayed.Len() > 0 {
			d := q.delayed[0].RunAt.Sub(now)
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
			timerC = timer.C
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return nil, ctx.Err()
		case <-waitCh:
			if timer != nil {
				timer.Stop()
			}
		case <-timerC:
		}
	}
}

// Len implements Queue.
func (q *Memory) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ready.Len() + q.delayed.Len()
}

// Close implements Queue.
func (q *Memory) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	q.broadcastLocked()
	return nil
}
