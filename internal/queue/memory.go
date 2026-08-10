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
}

// NewMemory returns an empty, ready-to-use in-memory queue.
func NewMemory() *Memory {
	return &Memory{changed: make(chan struct{})}
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

// Enqueue implements Queue.
func (q *Memory) Enqueue(j *job.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}

	if !j.RunAt.After(time.Now().UTC()) {
		heap.Push(&q.ready, j)
	} else {
		heap.Push(&q.delayed, j)
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
