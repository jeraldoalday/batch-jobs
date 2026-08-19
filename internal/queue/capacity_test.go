package queue

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestMemoryUnboundedByDefault(t *testing.T) {
	q := NewMemory()
	if q.Cap() != 0 {
		t.Fatalf("Cap() = %d, want 0 (unbounded)", q.Cap())
	}
	for i := 0; i < 200; i++ {
		if err := q.Enqueue(job.New("q", nil)); err != nil {
			t.Fatalf("Enqueue %d on unbounded queue: %v", i, err)
		}
	}
}

func TestMemoryWithCapacityIgnoresNonPositive(t *testing.T) {
	for _, n := range []int{0, -1} {
		q := NewMemory(WithCapacity(n))
		if q.Cap() != 0 {
			t.Errorf("WithCapacity(%d): Cap() = %d, want 0 (unbounded)", n, q.Cap())
		}
	}
}

func TestMemoryEnqueueFullReturnsErrFull(t *testing.T) {
	q := NewMemory(WithCapacity(2))
	if q.Cap() != 2 {
		t.Fatalf("Cap() = %d, want 2", q.Cap())
	}

	for i := 0; i < 2; i++ {
		if err := q.Enqueue(job.New("q", nil)); err != nil {
			t.Fatalf("Enqueue %d within capacity: %v", i, err)
		}
	}

	if err := q.Enqueue(job.New("q", nil)); !errors.Is(err, ErrFull) {
		t.Fatalf("Enqueue over capacity: err = %v, want ErrFull", err)
	}
	if got := q.Len(); got != 2 {
		t.Errorf("Len() = %d after a rejected Enqueue, want 2", got)
	}
}

func TestMemoryCapacityCountsDelayedJobs(t *testing.T) {
	q := NewMemory(WithCapacity(1))

	if err := q.Enqueue(job.New("q", nil, job.WithDelay(time.Hour))); err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}
	if err := q.Enqueue(job.New("q", nil)); !errors.Is(err, ErrFull) {
		t.Fatalf("Enqueue with a delayed job filling capacity: err = %v, want ErrFull", err)
	}
}

func TestMemoryCapacityFreedByDequeue(t *testing.T) {
	q := NewMemory(WithCapacity(1))

	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Enqueue(job.New("q", nil)); !errors.Is(err, ErrFull) {
		t.Fatalf("Enqueue at capacity: err = %v, want ErrFull", err)
	}

	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue after draining a slot: %v", err)
	}
}

// A job that has already been admitted once must never be bounced by the
// capacity check: dropping a retry or a dead letter would lose work the
// queue already accepted.
func TestMemoryCapacityAdmitsRetriesAndDeadLetters(t *testing.T) {
	tests := []struct {
		name   string
		status job.Status
	}{
		{"retrying", job.StatusRetrying},
		{"dead", job.StatusDead},
		{"failed", job.StatusFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewMemory(WithCapacity(1))
			if err := q.Enqueue(job.New("q", nil)); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			readmitted := job.New("q", nil)
			readmitted.Status = tt.status
			if err := q.Enqueue(readmitted); err != nil {
				t.Fatalf("Enqueue %s job at capacity: err = %v, want nil", tt.status, err)
			}
			if got := q.Len(); got != 2 {
				t.Errorf("Len() = %d, want 2 (capacity may be exceeded by re-admitted work)", got)
			}
		})
	}
}

// A deduplicated Enqueue adds no new work, so it must succeed even when the
// queue is otherwise full.
func TestMemoryCapacityAllowsIdempotentDuplicate(t *testing.T) {
	q := NewMemory(WithCapacity(1))

	first := job.New("q", nil, job.WithIdempotencyKey("k"))
	if err := q.Enqueue(first); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	dup := job.New("q", nil, job.WithIdempotencyKey("k"))
	if err := q.Enqueue(dup); err != nil {
		t.Fatalf("Enqueue duplicate at capacity: err = %v, want nil", err)
	}
	if dup.ID != first.ID {
		t.Errorf("duplicate got ID %q, want the original %q", dup.ID, first.ID)
	}
	if got := q.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1 (duplicate must not add an entry)", got)
	}
}

// Concurrent producers racing against a bounded queue must never push it
// past its capacity: exactly capacity Enqueue calls may succeed.
func TestMemoryCapacityUnderConcurrentProducers(t *testing.T) {
	const capacity = 10
	const producers = 50

	q := NewMemory(WithCapacity(capacity))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var accepted, rejected int

	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func() {
			defer wg.Done()
			err := q.Enqueue(job.New("q", nil))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrFull):
				rejected++
			default:
				t.Errorf("Enqueue: unexpected error %v", err)
			}
		}()
	}
	wg.Wait()

	if accepted != capacity {
		t.Errorf("accepted %d enqueues, want exactly %d", accepted, capacity)
	}
	if rejected != producers-capacity {
		t.Errorf("rejected %d enqueues, want %d", rejected, producers-capacity)
	}
	if got := q.Len(); got != capacity {
		t.Errorf("Len() = %d, want %d", got, capacity)
	}
}

func TestMemoryCapacityClosedTakesPrecedence(t *testing.T) {
	q := NewMemory(WithCapacity(1))
	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := q.Enqueue(job.New("q", nil)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue on a closed, full queue: err = %v, want ErrClosed", err)
	}
}
