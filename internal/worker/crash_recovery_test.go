package worker

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
)

// stallableQueue is a durable queue whose heartbeats can be switched off
// mid-test, standing in for a worker process that stops responding: the
// job it holds keeps "running" from the database's point of view, but
// nothing is renewing the claim any more.
type stallableQueue struct {
	*sqlite.Queue

	stalled atomic.Bool
}

func (q *stallableQueue) Heartbeat(id string) error {
	if q.stalled.Load() {
		return nil
	}
	return q.Queue.Heartbeat(id)
}

// TestCrashedWorkerJobIsRecoveredAndRerun is the whole Day 11 story end to
// end: a worker claims a job, keeps its claim alive while it works, dies
// holding it, and another process notices and finishes the job.
func TestCrashedWorkerJobIsRecoveredAndRerun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	const leaseTTL = 150 * time.Millisecond

	// Process A: the worker that is about to die.
	dbA, err := sqlite.Open(path, sqlite.Options{LeaseTTL: leaseTTL, Owner: "worker-a"})
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer dbA.Close()
	qA := &stallableQueue{Queue: dbA}

	claimed := make(chan struct{})
	release := make(chan struct{})
	var claims atomic.Int32

	poolA := &Pool{
		Queue: qA,
		Handler: func(ctx context.Context, j *job.Job) error {
			if claims.Add(1) == 1 {
				close(claimed)
			}
			// Runs until the test lets go, far longer than one lease.
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		},
		Concurrency:       1,
		HeartbeatInterval: 25 * time.Millisecond,
		Logger:            logger,
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	poolA.Start(ctxA)
	defer func() {
		close(release)
		cancelA()
		poolA.Stop()
	}()

	j := job.New("q", []byte("work"), job.WithMaxRetries(3))
	if err := dbA.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-claimed:
	case <-time.After(2 * time.Second):
		t.Fatal("job was never claimed")
	}

	// Process B: a peer running a reaper against the same database.
	dbB, err := sqlite.Open(path, sqlite.Options{LeaseTTL: leaseTTL, Owner: "worker-b"})
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer dbB.Close()

	reaper := &sqlite.Reaper{Queue: dbB, Interval: 20 * time.Millisecond, Logger: logger}
	reaper.Start(context.Background())
	defer reaper.Stop()

	// A live worker's claim survives the reaper for as long as it keeps
	// heartbeating, however long the job runs.
	time.Sleep(3 * leaseTTL)
	held, err := dbB.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if held.Status != job.StatusRunning {
		t.Fatalf("status while the worker is alive = %q, want running", held.Status)
	}

	// Process A goes away: no more heartbeats, and no outcome is ever
	// reported.
	qA.stalled.Store(true)

	waitForStatus(t, dbB, j.ID, job.StatusPending, "the reaper to reclaim the orphaned job")

	recovered, err := dbB.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if recovered.RetryCount != 1 {
		t.Fatalf("retry count after recovery = %d, want 1", recovered.RetryCount)
	}

	// Process B picks the recovered job up and finishes it.
	var handledByB atomic.Int32
	poolB := &Pool{
		Queue: dbB,
		Handler: func(ctx context.Context, j *job.Job) error {
			handledByB.Add(1)
			return nil
		},
		Concurrency:       1,
		HeartbeatInterval: 25 * time.Millisecond,
		Logger:            logger,
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	poolB.Start(ctxB)
	defer func() {
		cancelB()
		poolB.Stop()
	}()

	waitForStatus(t, dbB, j.ID, job.StatusSucceeded, "the recovered job to be re-run and completed")

	// And having been completed, it stays completed: a finished job holds
	// no lease, so the reaper has nothing to reclaim.
	time.Sleep(3 * leaseTTL)
	final, err := dbB.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.Status != job.StatusSucceeded {
		t.Fatalf("status after further reaper sweeps = %q, want succeeded", final.Status)
	}
	if got := handledByB.Load(); got != 1 {
		t.Fatalf("process B ran the job %d times, want 1", got)
	}
}

// waitForStatus polls until the job reaches want, or the test times out.
func waitForStatus(t *testing.T, q *sqlite.Queue, id string, want job.Status, what string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var last job.Status
	for time.Now().Before(deadline) {
		got, err := q.Get(id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Status == want {
			return
		}
		last = got.Status
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: status is %q, want %q", what, last, want)
}
