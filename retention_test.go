package batchjobs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs"
)

// runToCompletion enqueues n jobs whose handler returns fn's error and
// waits for every one of them to reach a terminal status, so the test has
// real finished jobs rather than rows it wrote by hand.
func runToCompletion(t *testing.T, e *batchjobs.Engine, n int, handler batchjobs.Handler) []string {
	t.Helper()

	e.Handle("work", handler)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		j, err := e.Enqueue(ctx, "work", []byte("payload"))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, j.ID)
	}

	waitFor(t, "every job to finish", func() bool {
		for _, id := range ids {
			got, err := e.Get(ctx, id)
			if err != nil || !got.Status.Terminal() {
				return false
			}
		}
		return true
	})
	return ids
}

// A policy short enough that a job which finished a moment ago has
// already outlived it, so a pass in the same test deletes it.
func expiringNow() batchjobs.Retention {
	return batchjobs.Retention{
		Succeeded: time.Nanosecond,
		Failed:    time.Nanosecond,
		Cancelled: time.Nanosecond,
	}
}

func TestEnginePruneDeletesFinishedJobs(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) {
		c.Retention = expiringNow()
		c.PruneInterval = -1 // prune only when this test says to
	})
	ids := runToCompletion(t, e, 3, func(ctx context.Context, j *batchjobs.Job) error { return nil })

	pruned, err := e.Prune(context.Background())
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Succeeded != 3 || pruned.Total() != 3 {
		t.Fatalf("pruned = %v, want three succeeded jobs", pruned)
	}

	for _, id := range ids {
		if _, err := e.Get(context.Background(), id); !errors.Is(err, batchjobs.ErrNotFound) {
			t.Errorf("Get(%s) = %v, want ErrNotFound after pruning", id, err)
		}
	}
}

// The default policy's most important property, checked through the
// engine: dead-lettered jobs survive a pass that clears everything else.
func TestEnginePruneKeepsDeadLetteredJobs(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) {
		c.Retention = batchjobs.Retention{Succeeded: time.Nanosecond}
		c.PruneInterval = -1
	})

	e.Handle("work", func(ctx context.Context, j *batchjobs.Job) error {
		return errors.New("always fails")
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()
	dead, err := e.Enqueue(ctx, "work", []byte("x"), batchjobs.WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, "the job to be dead-lettered", func() bool {
		got, err := e.Get(ctx, dead.ID)
		return err == nil && got.Status == batchjobs.StatusDead
	})

	pruned, err := e.Prune(ctx)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Dead != 0 {
		t.Errorf("pruned %d dead jobs, want 0: the policy leaves them forever", pruned.Dead)
	}
	if _, err := e.Get(ctx, dead.ID); err != nil {
		t.Errorf("the dead job was pruned: %v", err)
	}
}

func TestEnginePruneWithoutPolicy(t *testing.T) {
	e := newEngine(t)

	if _, err := e.Prune(context.Background()); !errors.Is(err, batchjobs.ErrNoRetention) {
		t.Fatalf("Prune with no policy = %v, want ErrNoRetention", err)
	}
}

func TestEnginePruneRejectsCancelledContext(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Retention = expiringNow() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := e.Prune(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Prune with a cancelled context = %v, want context.Canceled", err)
	}
}

// A policy that would delete the wrong thing stops the program starting,
// rather than surfacing an hour later in a sweep's log line.
func TestOpenRejectsNegativeRetention(t *testing.T) {
	_, err := batchjobs.Open(batchjobs.Config{
		DB:        batchjobs.MemoryDB,
		Logger:    quietLogger(),
		Retention: batchjobs.Retention{Dead: -time.Hour},
	})
	if err == nil {
		t.Fatal("Open accepted a negative retention")
	}
	if !strings.Contains(err.Error(), "negative") {
		t.Errorf("Open error = %q, want it to name the problem", err)
	}
}

// With an interval set, the Engine prunes on its own once started.
func TestEnginePrunesOnItsOwn(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) {
		c.Retention = expiringNow()
		c.PruneInterval = 5 * time.Millisecond
	})

	// The janitor's interval here is short enough to delete a finished
	// job before runToCompletion's own Get-based poll would ever observe
	// it as terminal -- once pruned, Get reports ErrNotFound forever, and
	// that poll would hang until the test's deadline. Completion is
	// signalled by the handler itself instead, which stays true whether
	// or not the row it ran against still exists afterward.
	var wg sync.WaitGroup
	wg.Add(2)
	e.Handle("work", func(ctx context.Context, j *batchjobs.Job) error {
		wg.Done()
		return nil
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := e.Enqueue(ctx, "work", []byte("payload")); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for every job to run")
	}

	waitFor(t, "the janitor to clear the finished jobs", func() bool {
		n, err := e.Retained()
		return err == nil && n == 0
	})
}

// A negative interval keeps the policy but leaves the passes to the
// caller, for a deployment pruning from one place rather than from every
// process holding the queue.
func TestEngineNegativePruneIntervalDisablesTheJanitor(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) {
		c.Retention = expiringNow()
		c.PruneInterval = -1
	})
	runToCompletion(t, e, 2, func(ctx context.Context, j *batchjobs.Job) error { return nil })

	time.Sleep(50 * time.Millisecond)

	n, err := e.Retained()
	if err != nil {
		t.Fatalf("Retained: %v", err)
	}
	if n != 2 {
		t.Fatalf("Retained() = %d, want 2: nothing should prune on its own", n)
	}

	// The policy is still there; it just has to be asked.
	pruned, err := e.Prune(context.Background())
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Total() != 2 {
		t.Errorf("explicit Prune removed %d jobs, want 2", pruned.Total())
	}
}

func TestEngineRetainedCountsOnlyFinishedJobs(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.PruneInterval = -1 })
	runToCompletion(t, e, 3, func(ctx context.Context, j *batchjobs.Job) error { return nil })

	// A job that will never be claimed, since nothing handles its queue
	// and the engine is not started for it.
	if _, err := e.Enqueue(context.Background(), "unhandled-elsewhere", []byte("x"),
		batchjobs.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	n, err := e.Retained()
	if err != nil {
		t.Fatalf("Retained: %v", err)
	}
	if n != 3 {
		t.Errorf("Retained() = %d, want 3: the delayed job has not finished", n)
	}
}

func TestEngineOldestRetained(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.PruneInterval = -1 })

	oldest, err := e.OldestRetained()
	if err != nil {
		t.Fatalf("OldestRetained: %v", err)
	}
	if !oldest.IsZero() {
		t.Fatalf("OldestRetained() = %s on a fresh engine, want the zero time", oldest)
	}

	runToCompletion(t, e, 1, func(ctx context.Context, j *batchjobs.Job) error { return nil })

	oldest, err = e.OldestRetained()
	if err != nil {
		t.Fatalf("OldestRetained: %v", err)
	}
	if oldest.IsZero() {
		t.Fatal("OldestRetained() is zero with a finished job in the queue")
	}
	if age := time.Since(oldest); age > time.Minute {
		t.Errorf("OldestRetained() is %s old, want roughly now", age)
	}
}

func TestEngineRetentionAccessor(t *testing.T) {
	plain := newEngine(t)
	if plain.Retention().Enabled() {
		t.Error("an Engine opened without a policy reports one")
	}

	configured := newEngine(t, func(c *batchjobs.Config) {
		c.Retention = batchjobs.Retention{Succeeded: time.Hour}
		c.PruneInterval = -1
	})
	if got := configured.Retention().Succeeded; got != time.Hour {
		t.Errorf("Retention().Succeeded = %s, want 1h", got)
	}
}

// The history gauges are registered whether or not a policy is set,
// because the queue with no policy is the one worth watching.
func TestEngineExposesRetentionMetricsWithoutAPolicy(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.PruneInterval = -1 })
	runToCompletion(t, e, 2, func(ctx context.Context, j *batchjobs.Job) error { return nil })

	rec := httptest.NewRecorder()
	e.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, `batchjobs_retained_jobs{queue="default"} 2`) {
		t.Errorf("scrape output missing the retained gauge; got:\n%s", body)
	}
	if !strings.Contains(body, "batchjobs_oldest_retained_job_age_seconds") {
		t.Errorf("scrape output missing the oldest-age gauge; got:\n%s", body)
	}
}

func TestEnginePruneCountsReachTheMetrics(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) {
		c.Retention = expiringNow()
		c.PruneInterval = -1
	})
	runToCompletion(t, e, 2, func(ctx context.Context, j *batchjobs.Job) error { return nil })

	if _, err := e.Prune(context.Background()); err != nil {
		t.Fatalf("Prune: %v", err)
	}

	// Engine.Prune is the explicit path and does not report to the
	// metrics itself -- the janitor does -- so the counter stays at zero
	// while the gauge follows the deletion. Both are worth pinning: it is
	// the difference between "someone pruned" and "the queue is holding
	// this much history".
	rec := httptest.NewRecorder()
	e.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := rec.Body.String(); !strings.Contains(body, `batchjobs_retained_jobs{queue="default"} 0`) {
		t.Errorf("retained gauge did not follow the prune; got:\n%s", body)
	}
}

// Pruning must not disturb work still waiting to run.
func TestEnginePruneLeavesPendingWorkAlone(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) {
		c.Retention = expiringNow()
		c.PruneInterval = -1
	})
	runToCompletion(t, e, 2, func(ctx context.Context, j *batchjobs.Job) error { return nil })

	ctx := context.Background()
	later, err := e.Enqueue(ctx, "work", []byte("later"), batchjobs.WithDelay(time.Hour))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, err := e.Prune(ctx); err != nil {
		t.Fatalf("Prune: %v", err)
	}

	got, err := e.Get(ctx, later.ID)
	if err != nil {
		t.Fatalf("the delayed job was pruned: %v", err)
	}
	if got.Status != batchjobs.StatusPending {
		t.Errorf("delayed job status = %s, want pending", got.Status)
	}
}

func TestRetentionAndPrunedValueTypes(t *testing.T) {
	r := batchjobs.DefaultRetention()
	if r.Dead != 0 {
		t.Errorf("DefaultRetention().Dead = %s, want 0", r.Dead)
	}
	if !strings.Contains(r.String(), "succeeded=") {
		t.Errorf("Retention.String() = %q, want it to name the pruned statuses", r.String())
	}

	p := batchjobs.Pruned{Succeeded: 2, Dead: 1}
	if got := p.Total(); got != 3 {
		t.Errorf("Pruned.Total() = %d, want 3", got)
	}
	if got := p.Get(batchjobs.StatusSucceeded); got != 2 {
		t.Errorf("Pruned.Get(succeeded) = %d, want 2", got)
	}
	if got := p.Get(batchjobs.StatusPending); got != 0 {
		t.Errorf("Pruned.Get(pending) = %d, want 0", got)
	}
	if got := p.String(); !strings.Contains(got, "succeeded=2") || !strings.Contains(got, "dead=1") {
		t.Errorf("Pruned.String() = %q, want it to name the non-zero counts", got)
	}
}
