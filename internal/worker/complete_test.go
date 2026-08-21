package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// completingQueue is an in-memory queue that also records outcomes, so the
// pool's write-back can be observed without a database.
type completingQueue struct {
	queue.Queue

	mu        sync.Mutex
	completed []*job.Job
	err       error
}

func newCompletingQueue() *completingQueue {
	return &completingQueue{Queue: queue.NewMemory()}
}

func (q *completingQueue) Complete(j *job.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.err != nil {
		return q.err
	}
	// Store a copy: the pool keeps using the job after completing it.
	clone := *j
	q.completed = append(q.completed, &clone)
	return nil
}

func (q *completingQueue) outcomes() []*job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]*job.Job(nil), q.completed...)
}

func TestPoolCompletesSucceededJob(t *testing.T) {
	q := newCompletingQueue()
	p := quietPool(&Pool{
		Queue:       q,
		Handler:     func(ctx context.Context, j *job.Job) error { return nil },
		Concurrency: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return len(q.outcomes()) == 1 })
	if got := q.outcomes()[0].Status; got != job.StatusSucceeded {
		t.Fatalf("recorded status = %q, want succeeded", got)
	}
}

func TestPoolCompletesPermanentFailure(t *testing.T) {
	q := newCompletingQueue()
	p := quietPool(&Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			return Permanent(errors.New("malformed payload"))
		},
		Concurrency: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil, job.WithMaxRetries(5))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return len(q.outcomes()) == 1 })
	outcome := q.outcomes()[0]
	if outcome.Status != job.StatusFailed {
		t.Fatalf("recorded status = %q, want failed", outcome.Status)
	}
	if outcome.LastError == "" {
		t.Fatal("recorded outcome has no error message")
	}
}

func TestPoolCompletesDeadJobOnce(t *testing.T) {
	q := newCompletingQueue()
	p := quietPool(&Pool{
		Queue:       q,
		Handler:     func(ctx context.Context, j *job.Job) error { return errors.New("boom") },
		Concurrency: 1,
		Backoff:     ConstantBackoff(time.Millisecond),
	})

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	// One retry, then the job is dead: the retry goes back through
	// Enqueue and only the final state is completed.
	if err := q.Enqueue(job.New("q", nil, job.WithMaxRetries(1))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return len(q.outcomes()) == 1 })
	if got := q.outcomes()[0].Status; got != job.StatusDead {
		t.Fatalf("recorded status = %q, want dead", got)
	}

	time.Sleep(50 * time.Millisecond)
	if n := len(q.outcomes()); n != 1 {
		t.Fatalf("recorded %d outcomes, want exactly 1 (retries are enqueued, not completed)", n)
	}
}

func TestPoolSurvivesCompleteFailure(t *testing.T) {
	q := newCompletingQueue()
	q.err = errors.New("database is gone")

	var handled int32
	var mu sync.Mutex
	p := quietPool(&Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			mu.Lock()
			handled++
			mu.Unlock()
			return nil
		},
		Concurrency: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	// A store that cannot record outcomes must not stop the pool from
	// draining its queue.
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(job.New("q", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handled == 3
	})
}

func TestPoolSkipsQueuesThatDoNotComplete(t *testing.T) {
	// queue.Memory is not a Completer; the pool must run jobs exactly as
	// it did before outcomes were recorded.
	q := queue.NewMemory()
	done := make(chan struct{})
	p := quietPool(&Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			close(done)
			return nil
		},
		Concurrency: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job was never handled")
	}
}
