package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// startPool starts p against a background context and registers Stop.
func startPool(t *testing.T, p *Pool) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	t.Cleanup(func() {
		cancel()
		p.Stop()
	})
}

// noopHandler succeeds immediately.
func noopHandler(context.Context, *job.Job) error { return nil }

func TestPoolFixedSizeByDefault(t *testing.T) {
	q := queue.NewMemory()
	p := &Pool{Queue: q, Handler: noopHandler, Concurrency: 3}
	startPool(t, p)

	if got := p.Workers(); got != 3 {
		t.Fatalf("Workers() = %d, want 3", got)
	}

	// A backlog must not move a pool that never opted into scaling.
	for i := 0; i < 50; i++ {
		if err := q.Enqueue(job.New("q", nil, job.WithDelay(time.Hour))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	time.Sleep(20 * time.Millisecond)

	if got := p.Workers(); got != 3 {
		t.Errorf("Workers() = %d after a backlog built up, want 3 (autoscaling disabled)", got)
	}
}

func TestPoolInitialSizeRespectsPolicyBounds(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		scale       Policy
		want        int
	}{
		{"no scaling uses Concurrency", 4, Policy{}, 4},
		{"below Min is raised", 1, Policy{Min: 3, Max: 10}, 3},
		{"above Max is lowered", 20, Policy{Min: 1, Max: 5}, 5},
		{"within bounds is kept", 4, Policy{Min: 1, Max: 10}, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Queue:       queue.NewMemory(),
				Handler:     noopHandler,
				Concurrency: tt.concurrency,
				Scale:       tt.scale,
				// Long enough that no tick fires during the assertion.
				ScaleInterval: time.Hour,
			}
			startPool(t, p)

			if got := p.Workers(); got != tt.want {
				t.Errorf("Workers() = %d, want %d", got, tt.want)
			}
		})
	}
}

// A backlog should grow the pool on the very first sizing decision.
func TestPoolScalesUpOnBacklog(t *testing.T) {
	q := queue.NewMemory()
	p := &Pool{
		Queue:         q,
		Handler:       noopHandler,
		Concurrency:   1,
		Scale:         Policy{Min: 1, Max: 8},
		ScaleInterval: time.Hour,
	}
	startPool(t, p)

	// Delayed so the workers cannot drain the backlog out from under the
	// sample and make the test depend on timing.
	for i := 0; i < 5; i++ {
		if err := q.Enqueue(job.New("q", nil, job.WithDelay(time.Hour))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	p.rescale(&Scaler{Policy: p.Scale})

	// No worker is busy (the jobs are not eligible yet), so the target is
	// one worker per waiting job.
	if got := p.Workers(); got != 5 {
		t.Errorf("Workers() = %d after a backlog of 5, want 5", got)
	}
}

func TestPoolScaleUpIsCappedAtMax(t *testing.T) {
	q := queue.NewMemory()
	p := &Pool{
		Queue:         q,
		Handler:       noopHandler,
		Concurrency:   1,
		Scale:         Policy{Min: 1, Max: 3},
		ScaleInterval: time.Hour,
	}
	startPool(t, p)

	for i := 0; i < 100; i++ {
		if err := q.Enqueue(job.New("q", nil, job.WithDelay(time.Hour))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	p.rescale(&Scaler{Policy: p.Scale})

	if got := p.Workers(); got != 3 {
		t.Errorf("Workers() = %d under a backlog of 100, want 3 (Max)", got)
	}
}

func TestPoolScalesDownWhenIdle(t *testing.T) {
	p := &Pool{
		Queue:         queue.NewMemory(),
		Handler:       noopHandler,
		Concurrency:   5,
		Scale:         Policy{Min: 1, Max: 10, ScaleDownAfter: 2},
		ScaleInterval: time.Hour,
	}
	startPool(t, p)

	scaler := &Scaler{Policy: p.Scale}

	p.rescale(scaler)
	if got := p.Workers(); got != 5 {
		t.Fatalf("Workers() = %d after one idle sample, want 5 (hysteresis)", got)
	}

	p.rescale(scaler)
	if got := p.Workers(); got != 4 {
		t.Fatalf("Workers() = %d after two idle samples, want 4", got)
	}

	// Keep going: the pool must settle at Min and stay there.
	for i := 0; i < 20; i++ {
		p.rescale(scaler)
	}
	if got := p.Workers(); got != 1 {
		t.Errorf("Workers() = %d after a long idle period, want 1 (Min)", got)
	}
}

// Retiring a worker must never cancel the job it is already running: the
// scale-down signal targets the worker's Dequeue, not its handler.
func TestPoolScaleDownLetsInFlightJobFinish(t *testing.T) {
	q := queue.NewMemory()

	started := make(chan struct{})
	release := make(chan struct{})
	var completed, cancelled atomic.Int32

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			close(started)
			select {
			case <-release:
				completed.Add(1)
			case <-ctx.Done():
				cancelled.Add(1)
			}
			return nil
		},
		Concurrency:   2,
		Scale:         Policy{Min: 1, Max: 4, ScaleDownAfter: 1},
		ScaleInterval: time.Hour,
	}
	startPool(t, p)

	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	<-started

	p.scaleTo(1)
	waitFor(t, func() bool { return p.Workers() == 1 })

	// The handler is still running, unaware it belongs to a retired
	// worker; only now do we let it finish.
	close(release)
	waitFor(t, func() bool { return completed.Load() == 1 })

	if got := cancelled.Load(); got != 0 {
		t.Errorf("%d handlers saw a cancelled context, want 0", got)
	}
}

// Busy counts workers inside a handler, which is what stops the scaler
// retiring workers that are doing something.
func TestPoolBusyTracksRunningHandlers(t *testing.T) {
	q := queue.NewMemory()
	release := make(chan struct{})

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			<-release
			return nil
		},
		Concurrency: 3,
	}
	startPool(t, p)

	if got := p.Busy(); got != 0 {
		t.Fatalf("Busy() = %d on an idle pool, want 0", got)
	}

	for i := 0; i < 2; i++ {
		if err := q.Enqueue(job.New("q", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	waitFor(t, func() bool { return p.Busy() == 2 })

	close(release)
	waitFor(t, func() bool { return p.Busy() == 0 })
}

// A busy pool holds its size even with an empty queue: those workers are
// accounted for by Sample.Busy, not by the backlog.
func TestPoolDoesNotRetireBusyWorkers(t *testing.T) {
	q := queue.NewMemory()
	release := make(chan struct{})
	defer close(release)

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			<-release
			return nil
		},
		Concurrency:   3,
		Scale:         Policy{Min: 1, Max: 5, ScaleDownAfter: 1},
		ScaleInterval: time.Hour,
	}
	startPool(t, p)

	for i := 0; i < 3; i++ {
		if err := q.Enqueue(job.New("q", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	waitFor(t, func() bool { return p.Busy() == 3 })

	scaler := &Scaler{Policy: p.Scale}
	for i := 0; i < 5; i++ {
		p.rescale(scaler)
	}

	if got := p.Workers(); got != 3 {
		t.Errorf("Workers() = %d while all three are busy, want 3", got)
	}
}

// End to end: an autoscaling pool started at one worker should grow on its
// own and drain a burst of slow jobs faster than a single worker could.
func TestPoolAutoscalesUnderLoad(t *testing.T) {
	const jobs = 20

	q := queue.NewMemory()
	var done atomic.Int32

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			time.Sleep(20 * time.Millisecond)
			done.Add(1)
			return nil
		},
		Concurrency:   1,
		Scale:         Policy{Min: 1, Max: 10},
		ScaleInterval: 5 * time.Millisecond,
	}
	startPool(t, p)

	for i := 0; i < jobs; i++ {
		if err := q.Enqueue(job.New("q", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	waitFor(t, func() bool { return p.Workers() > 1 })
	waitFor(t, func() bool { return done.Load() == jobs })
}

// Stop must wait for the autoscaler goroutine too, not just the workers.
func TestPoolStopShutsDownAutoscaler(t *testing.T) {
	p := &Pool{
		Queue:         queue.NewMemory(),
		Handler:       noopHandler,
		Concurrency:   2,
		Scale:         Policy{Min: 1, Max: 4},
		ScaleInterval: time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	cancel()
	p.Stop()

	if got := p.Workers(); got != 0 {
		t.Errorf("Workers() = %d after Stop, want 0", got)
	}
}
