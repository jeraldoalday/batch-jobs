package sqlite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestSQLiteDequeueBlocksUntilEnqueue(t *testing.T) {
	q := openTestQueue(t)
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

func TestSQLiteDequeueRespectsContextCancellation(t *testing.T) {
	q := openTestQueue(t)
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

func TestSQLiteCloseUnblocksDequeue(t *testing.T) {
	q := openTestQueue(t)
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
		if !errors.Is(err, queue.ErrClosed) {
			t.Errorf("Dequeue err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after Close")
	}
}

func TestSQLiteDequeueWaitsForDelayedJob(t *testing.T) {
	q := openTestQueue(t)
	delayed := job.New("q", nil, job.WithDelay(50*time.Millisecond))
	if err := q.Enqueue(delayed); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	got, err := q.Dequeue(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.ID != delayed.ID {
		t.Errorf("got job %q, want %q", got.ID, delayed.ID)
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("Dequeue returned after %v, want it to have waited for run_at (~50ms)", elapsed)
	}
}

func TestSQLiteConcurrentEnqueueDequeue(t *testing.T) {
	q := openTestQueue(t)
	const n = 100

	var producers sync.WaitGroup
	producers.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer producers.Done()
			if err := q.Enqueue(job.New("q", nil)); err != nil {
				t.Errorf("Enqueue: %v", err)
			}
		}()
	}
	producers.Wait()

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
		t.Fatalf("dequeued %d distinct jobs, want %d (a duplicate means the claim query raced)", len(seen), n)
	}
}
