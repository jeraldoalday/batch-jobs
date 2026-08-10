package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestMemoryEnqueueDequeueFIFO(t *testing.T) {
	q := NewMemory()

	first := job.New("q", []byte("1"))
	second := job.New("q", []byte("2"))
	if err := q.Enqueue(first); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Enqueue(second); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got1, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got1.ID != first.ID {
		t.Errorf("got job %q, want %q (FIFO order)", got1.ID, first.ID)
	}

	got2, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got2.ID != second.ID {
		t.Errorf("got job %q, want %q (FIFO order)", got2.ID, second.ID)
	}
}

func TestMemoryTryDequeueEmpty(t *testing.T) {
	q := NewMemory()
	_, err := q.TryDequeue()
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("TryDequeue on empty queue: err = %v, want ErrEmpty", err)
	}
}

func TestMemoryDequeueMarksRunning(t *testing.T) {
	q := NewMemory()
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusRunning)
	}
}

func TestMemoryPriorityOrder(t *testing.T) {
	q := NewMemory()

	low := job.New("q", nil, job.WithPriority(job.PriorityLow))
	normal := job.New("q", nil, job.WithPriority(job.PriorityNormal))
	high := job.New("q", nil, job.WithPriority(job.PriorityHigh))

	// Enqueue out of priority order to prove the heap reorders them.
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

func TestMemoryDelayedJobNotReadyImmediately(t *testing.T) {
	q := NewMemory()
	delayed := job.New("q", nil, job.WithDelay(50*time.Millisecond))
	if err := q.Enqueue(delayed); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, err := q.TryDequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("TryDequeue before delay elapsed: err = %v, want ErrEmpty", err)
	}

	time.Sleep(75 * time.Millisecond)

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue after delay elapsed: %v", err)
	}
	if got.ID != delayed.ID {
		t.Errorf("got job %q, want %q", got.ID, delayed.ID)
	}
}

func TestMemoryDequeueBlocksUntilEnqueue(t *testing.T) {
	q := NewMemory()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resultCh := make(chan *job.Job, 1)
	errCh := make(chan error, 1)
	go func() {
		j, err := q.Dequeue(ctx)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- j
	}()

	time.Sleep(20 * time.Millisecond) // give the goroutine time to block
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case got := <-resultCh:
		if got.ID != j.ID {
			t.Errorf("got job %q, want %q", got.ID, j.ID)
		}
	case err := <-errCh:
		t.Fatalf("Dequeue returned error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not unblock after Enqueue")
	}
}

func TestMemoryDequeueRespectsContextCancellation(t *testing.T) {
	q := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(ctx)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Dequeue err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after context cancellation")
	}
}

func TestMemoryCloseUnblocksDequeue(t *testing.T) {
	q := NewMemory()
	errCh := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(context.Background())
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Dequeue err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after Close")
	}
}

func TestMemoryEnqueueAfterCloseFails(t *testing.T) {
	q := NewMemory()
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Enqueue(job.New("q", nil)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Close: err = %v, want ErrClosed", err)
	}
}

func TestMemoryLen(t *testing.T) {
	q := NewMemory()
	if got := q.Len(); got != 0 {
		t.Fatalf("Len on empty queue = %d, want 0", got)
	}

	_ = q.Enqueue(job.New("q", nil))
	_ = q.Enqueue(job.New("q", nil, job.WithDelay(time.Hour)))

	if got := q.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
}

func TestMemoryConcurrentEnqueueDequeue(t *testing.T) {
	q := NewMemory()
	const n = 200

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = q.Enqueue(job.New("q", nil))
		}()
	}
	wg.Wait()

	seen := make(map[string]bool)
	var mu sync.Mutex
	var consumers sync.WaitGroup
	consumers.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer consumers.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			j, err := q.Dequeue(ctx)
			if err != nil {
				t.Errorf("Dequeue: %v", err)
				return
			}
			mu.Lock()
			seen[j.ID] = true
			mu.Unlock()
		}()
	}
	consumers.Wait()

	if len(seen) != n {
		t.Fatalf("dequeued %d distinct jobs, want %d", len(seen), n)
	}
}
