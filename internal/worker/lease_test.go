package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// leasingQueue wraps an in-memory queue with the Heartbeater interface, so
// the pool's lease handling can be exercised without a database. Its
// heartbeat behaviour is programmable: err, when set, is what every
// heartbeat returns.
type leasingQueue struct {
	queue.Queue

	ttl   time.Duration
	beats atomic.Int32
	err   atomic.Value // error
}

func newLeasingQueue(ttl time.Duration) *leasingQueue {
	return &leasingQueue{Queue: queue.NewMemory(), ttl: ttl}
}

func (q *leasingQueue) LeaseTTL() time.Duration { return q.ttl }

func (q *leasingQueue) Heartbeat(id string) error {
	q.beats.Add(1)
	if err, ok := q.err.Load().(error); ok && err != nil {
		return err
	}
	return nil
}

func (q *leasingQueue) failWith(err error) { q.err.Store(err) }

// quietPool is a pool whose logs are discarded: several of these tests
// deliberately provoke warn- and error-level lines.
func quietPool(p *Pool) *Pool {
	p.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return p
}

func TestPoolHeartbeatsWhileHandlerRuns(t *testing.T) {
	q := newLeasingQueue(time.Minute)
	release := make(chan struct{})
	var running atomic.Bool

	p := quietPool(&Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			running.Store(true)
			<-release
			return nil
		},
		Concurrency:       1,
		HeartbeatInterval: 5 * time.Millisecond,
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

	waitFor(t, func() bool { return running.Load() })
	waitFor(t, func() bool { return q.beats.Load() >= 3 })

	close(release)

	// Once the handler returns, the heartbeat goroutine must stop too:
	// the pool is no longer responsible for the job.
	waitFor(t, func() bool { return p.Busy() == 0 })
	settled := q.beats.Load()
	time.Sleep(30 * time.Millisecond)
	if got := q.beats.Load(); got != settled {
		t.Fatalf("heartbeats continued after the job finished: %d then %d", settled, got)
	}
}

func TestPoolInterruptsHandlerOnLostLease(t *testing.T) {
	q := newLeasingQueue(time.Minute)
	q.failWith(queue.ErrLeaseLost)

	interrupted := make(chan struct{})
	p := quietPool(&Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			<-ctx.Done()
			close(interrupted)
			return ctx.Err()
		},
		Concurrency:       1,
		HeartbeatInterval: 5 * time.Millisecond,
		Backoff:           ConstantBackoff(time.Hour), // no second attempt during the test
	})

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil, job.WithMaxRetries(1))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-interrupted:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not interrupted after the lease was lost")
	}
}

func TestPoolKeepsRunningThroughTransientHeartbeatErrors(t *testing.T) {
	q := newLeasingQueue(time.Minute)
	q.failWith(errors.New("database is busy"))

	done := make(chan struct{})
	p := quietPool(&Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			// A transient heartbeat failure must not cancel the
			// attempt, and must not stop the pool retrying the
			// heartbeat either.
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				if err := ctx.Err(); err != nil {
					return err
				}
				time.Sleep(5 * time.Millisecond)
			}
			close(done)
			return nil
		},
		Concurrency:       1,
		HeartbeatInterval: 5 * time.Millisecond,
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
		t.Fatal("handler was cancelled or never finished despite only transient heartbeat errors")
	}
	if q.beats.Load() < 3 {
		t.Fatalf("heartbeats attempted = %d, want the loop to keep retrying after a failure", q.beats.Load())
	}
}

func TestPoolIgnoresQueuesWithoutLeases(t *testing.T) {
	// queue.Memory is not a Heartbeater; the pool must run jobs exactly
	// as it did before leases existed.
	q := queue.NewMemory()
	var handled atomic.Int32

	p := quietPool(&Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			handled.Add(1)
			return nil
		},
		Concurrency:       1,
		HeartbeatInterval: time.Millisecond,
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
	waitFor(t, func() bool { return handled.Load() == 1 })
}

func TestHeartbeatIntervalDerivedFromLeaseTTL(t *testing.T) {
	p := &Pool{}

	if got, want := p.heartbeatInterval(newLeasingQueue(30*time.Second)), 10*time.Second; got != want {
		t.Errorf("derived heartbeat interval = %v, want %v (TTL/%d)", got, want, DefaultHeartbeatDivisor)
	}

	// An explicit interval wins over the derived one.
	p.HeartbeatInterval = time.Second
	if got := p.heartbeatInterval(newLeasingQueue(30 * time.Second)); got != time.Second {
		t.Errorf("configured heartbeat interval = %v, want 1s", got)
	}

	// A queue that does not expire claims needs no heartbeat at all.
	p.HeartbeatInterval = 0
	if got := p.heartbeatInterval(newLeasingQueue(0)); got != 0 {
		t.Errorf("heartbeat interval for a non-expiring queue = %v, want 0", got)
	}
}
