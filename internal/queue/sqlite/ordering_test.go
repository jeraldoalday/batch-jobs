package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestSQLitePriorityOrder(t *testing.T) {
	q := openTestQueue(t)

	low := job.New("q", nil, job.WithPriority(job.PriorityLow))
	normal := job.New("q", nil, job.WithPriority(job.PriorityNormal))
	high := job.New("q", nil, job.WithPriority(job.PriorityHigh))

	// Enqueue out of priority order to prove the query reorders them.
	for _, j := range []*job.Job{low, normal, high} {
		if err := q.Enqueue(j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	wantOrder := []*job.Job{high, normal, low}
	for i, want := range wantOrder {
		got, err := q.TryDequeue()
		if err != nil {
			t.Fatalf("TryDequeue[%d]: %v", i, err)
		}
		if got.ID != want.ID {
			t.Errorf("TryDequeue[%d] = job %q (priority %v), want %q (priority %v)",
				i, got.ID, got.Priority, want.ID, want.Priority)
		}
	}
}

func TestSQLiteEqualPriorityIsFIFO(t *testing.T) {
	q := openTestQueue(t)

	var jobs []*job.Job
	for i := 0; i < 5; i++ {
		j := job.New("q", nil)
		jobs = append(jobs, j)
		if err := q.Enqueue(j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		// Ensure distinct CreatedAt values even on fast filesystems/clocks.
		time.Sleep(time.Millisecond)
	}

	for i, want := range jobs {
		got, err := q.TryDequeue()
		if err != nil {
			t.Fatalf("TryDequeue[%d]: %v", i, err)
		}
		if got.ID != want.ID {
			t.Errorf("TryDequeue[%d] = job %q, want %q (FIFO among equal priority)", i, got.ID, want.ID)
		}
	}
}

func TestSQLiteDelayedJobNotReadyImmediately(t *testing.T) {
	q := openTestQueue(t)
	delayed := job.New("q", nil, job.WithDelay(75*time.Millisecond))
	if err := q.Enqueue(delayed); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, err := q.TryDequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("TryDequeue before delay elapsed: err = %v, want ErrEmpty", err)
	}

	time.Sleep(100 * time.Millisecond)

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue after delay elapsed: %v", err)
	}
	if got.ID != delayed.ID {
		t.Errorf("got job %q, want %q", got.ID, delayed.ID)
	}
}

func TestSQLiteDelayedJobDoesNotBlockReadyJob(t *testing.T) {
	q := openTestQueue(t)

	ready := job.New("q", nil)
	delayed := job.New("q", nil, job.WithDelay(time.Hour))
	if err := q.Enqueue(delayed); err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}
	if err := q.Enqueue(ready); err != nil {
		t.Fatalf("Enqueue ready: %v", err)
	}

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.ID != ready.ID {
		t.Errorf("got job %q, want %q (ready job should not wait behind a delayed one)", got.ID, ready.ID)
	}
}

func TestSQLiteLen(t *testing.T) {
	q := openTestQueue(t)
	if got := q.Len(); got != 0 {
		t.Fatalf("Len on empty queue = %d, want 0", got)
	}

	_ = q.Enqueue(job.New("q", nil))
	_ = q.Enqueue(job.New("q", nil, job.WithDelay(time.Hour)))

	if got := q.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}

	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("Len after one TryDequeue = %d, want 1 (running jobs don't count as waiting)", got)
	}
}
