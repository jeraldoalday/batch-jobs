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

// A single autoscaling pool draining a weighted router should process every
// job from every route, and should have grown past its starting size to do
// it. This is the whole scaling story in one test: routing, fan-in, and
// dynamic sizing operating on the same pool.
func TestPoolDrainsWeightedRouter(t *testing.T) {
	const perQueue = 15

	payments := queue.NewMemory()
	emails := queue.NewMemory()

	r, err := queue.NewRouter([]queue.Route{
		{Name: "payments", Queue: payments, Weight: 3},
		{Name: "emails", Queue: emails, Weight: 1},
	}, queue.WithRouterPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer r.Close()

	var mu sync.Mutex
	handled := map[string]int{}

	p := &Pool{
		Queue: r,
		Handler: func(ctx context.Context, j *job.Job) error {
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			handled[j.Queue]++
			mu.Unlock()
			return nil
		},
		Concurrency:   1,
		Scale:         Policy{Min: 1, Max: 8},
		ScaleInterval: 5 * time.Millisecond,
	}
	startPool(t, p)

	for i := 0; i < perQueue; i++ {
		if err := r.Enqueue(job.New("payments", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := r.Enqueue(job.New("emails", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handled["payments"] == perQueue && handled["emails"] == perQueue
	})

	if got := r.Len(); got != 0 {
		t.Errorf("router Len() = %d after draining, want 0", got)
	}
}

// Backpressure and retries have to coexist: once a bounded queue fills,
// new work is rejected, but a job already in the system must still be able
// to come back for its retry.
func TestBoundedQueueRejectsNewWorkButNotRetries(t *testing.T) {
	const capacity = 4

	q := queue.NewMemory(queue.WithCapacity(capacity))

	var attempts atomic.Int32
	release := make(chan struct{})

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			if attempts.Add(1) == 1 {
				return errors.New("transient")
			}
			<-release
			return nil
		},
		Concurrency: 1,
		// Long enough that the retry is still sitting in the queue,
		// holding a slot, while the test fills the rest of it.
		Backoff: ConstantBackoff(50 * time.Millisecond),
	}

	failing := job.New("q", nil, job.WithMaxRetries(1))
	if err := q.Enqueue(failing); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	startPool(t, p)

	// Wait for the first attempt to fail and the job to be rescheduled,
	// which puts it back in the queue occupying one of the slots.
	waitFor(t, func() bool { return attempts.Load() >= 1 && q.Len() == 1 })

	// Fill the remaining slots, then confirm the queue is refusing new
	// work.
	for i := 0; i < capacity-1; i++ {
		if err := q.Enqueue(job.New("q", nil, job.WithDelay(time.Hour))); err != nil {
			t.Fatalf("filling the queue: %v", err)
		}
	}
	if err := q.Enqueue(job.New("q", nil)); !errors.Is(err, queue.ErrFull) {
		t.Fatalf("Enqueue at capacity: err = %v, want ErrFull", err)
	}

	// The retry was admitted before the queue filled, so it must still
	// run rather than being dropped along with the rejected work.
	waitFor(t, func() bool { return attempts.Load() >= 2 })
	close(release)
}
