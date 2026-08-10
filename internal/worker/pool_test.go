package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestPoolProcessesSuccessfulJob(t *testing.T) {
	q := queue.NewMemory()
	var handled atomic.Int32

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			handled.Add(1)
			return nil
		},
		Concurrency: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return handled.Load() == 1 })
}

func TestPoolRetriesThenSucceeds(t *testing.T) {
	q := queue.NewMemory()
	var attempts atomic.Int32

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			n := attempts.Add(1)
			if n < 3 {
				return errors.New("transient failure")
			}
			return nil
		},
		Concurrency: 1,
		Backoff:     ConstantBackoff(time.Millisecond),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil, job.WithMaxRetries(5))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return attempts.Load() == 3 })
}

func TestPoolSendsExhaustedJobToDeadLetter(t *testing.T) {
	q := queue.NewMemory()
	dlq := queue.NewMemory()

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			return errors.New("always fails")
		},
		Concurrency: 1,
		Backoff:     ConstantBackoff(time.Millisecond),
		DeadLetter:  dlq,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	j := job.New("q", nil, job.WithMaxRetries(1))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return dlq.Len() == 1 })

	got, err := dlq.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue from dead letter queue: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("dead-lettered job ID = %q, want %q", got.ID, j.ID)
	}
	// Note: TryDequeue always marks the returned job StatusRunning (see
	// Queue.Dequeue docs), even when reading from the dead-letter queue
	// for inspection, so we assert on fields that survive that mutation
	// instead of Status.
	if got.RetryCount != 1 {
		t.Errorf("dead-lettered job RetryCount = %d, want 1", got.RetryCount)
	}
	if got.LastError != "always fails" {
		t.Errorf("dead-lettered job LastError = %q, want %q", got.LastError, "always fails")
	}
}

func TestPoolRecoversFromHandlerPanic(t *testing.T) {
	q := queue.NewMemory()
	dlq := queue.NewMemory()
	var calls atomic.Int32

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			calls.Add(1)
			panic("boom")
		},
		Concurrency: 1,
		Backoff:     ConstantBackoff(time.Millisecond),
		DeadLetter:  dlq,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil, job.WithMaxRetries(0))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return dlq.Len() == 1 })
	if calls.Load() == 0 {
		t.Fatal("expected handler to have been called at least once")
	}
}

func TestPoolStopWaitsForInFlightJob(t *testing.T) {
	q := queue.NewMemory()
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			close(started)
			<-release
			finished.Store(true)
			return nil
		},
		Concurrency: 1,
	}

	p.Start(context.Background())

	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	<-started

	stopped := make(chan struct{})
	go func() {
		p.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned before in-flight handler finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after handler finished")
	}

	if !finished.Load() {
		t.Fatal("expected handler to have run to completion")
	}
}

func TestPoolConcurrencyProcessesInParallel(t *testing.T) {
	q := queue.NewMemory()
	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			defer wg.Done()
			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				max := maxInFlight.Load()
				if cur <= max || maxInFlight.CompareAndSwap(max, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		Concurrency: n,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	for i := 0; i < n; i++ {
		if err := q.Enqueue(job.New("q", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	wg.Wait()

	if maxInFlight.Load() < 2 {
		t.Fatalf("maxInFlight = %d, want >= 2 (jobs should run concurrently)", maxInFlight.Load())
	}
}

// waitFor polls cond until it returns true or fails the test after a
// timeout, to avoid flaky sleeps in concurrency tests.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
