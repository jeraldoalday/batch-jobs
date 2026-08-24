package batchjobs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs"
	"github.com/jeraldoalday/batch-jobs/client"
)

func TestEngineStartTwiceIsAnError(t *testing.T) {
	e := newEngine(t)
	e.Handle("emails", func(context.Context, *batchjobs.Job) error { return nil })

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if err := e.Start(ctx); err == nil {
		t.Error("second Start = nil error, want one")
	}
}

func TestEngineStopAndCloseAreIdempotent(t *testing.T) {
	e := newEngine(t)
	e.Handle("emails", func(context.Context, *batchjobs.Job) error { return nil })

	// Stop before Start is a no-op, not a panic: a program that fails
	// during setup runs its deferred Stop anyway.
	e.Stop()

	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Stop()
	e.Stop()

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	if err := e.Start(context.Background()); err == nil {
		t.Error("Start after Close = nil error, want one")
	}
}

func TestEngineStopDrainsInFlightJobs(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })

	started := make(chan struct{})
	var finished bool
	var mu sync.Mutex
	e.Handle("slow", func(ctx context.Context, _ *batchjobs.Job) error {
		close(started)
		// Long enough that a Stop which did not wait would return first.
		time.Sleep(150 * time.Millisecond)
		mu.Lock()
		finished = true
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := e.Enqueue(ctx, "slow", nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	<-started
	e.Stop()

	mu.Lock()
	defer mu.Unlock()
	if !finished {
		t.Error("Stop returned while a handler was still running, want it to drain")
	}
}

func TestEngineEnqueueWorksAfterStop(t *testing.T) {
	e := newEngine(t)
	e.Handle("emails", func(context.Context, *batchjobs.Job) error { return nil })

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Stop()

	// Stop retires the workers, not the queue: a process that has drained
	// its pool can still record work for the next one to pick up.
	if _, err := e.Enqueue(ctx, "emails", []byte("later")); err != nil {
		t.Errorf("Enqueue after Stop = %v, want nil", err)
	}
	if e.Len() != 1 {
		t.Errorf("queue holds %d jobs, want 1", e.Len())
	}
}

func TestEngineJobsSurviveRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	ctx := context.Background()

	producer, err := batchjobs.Open(batchjobs.Config{
		DB: dbPath, Logger: quietLogger(), ReapInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	created, err := producer.Enqueue(ctx, "reports", []byte("nightly"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A second Engine over the same file is a restarted process: the job
	// is still there, and a worker picks it up without it being re-queued.
	consumer, err := batchjobs.Open(batchjobs.Config{
		DB: dbPath, Workers: 1, Logger: quietLogger(), ReapInterval: -1,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer consumer.Close()

	ran := make(chan string, 1)
	consumer.Handle("reports", func(_ context.Context, j *batchjobs.Job) error {
		ran <- string(j.Payload)
		return nil
	})
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer consumer.Stop()

	select {
	case payload := <-ran:
		if payload != "nightly" {
			t.Errorf("handler saw payload %q, want nightly", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the job enqueued before the restart never ran")
	}

	j, err := consumer.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if j.ID != created.ID {
		t.Errorf("job ID = %s, want the one from before the restart %s", j.ID, created.ID)
	}
}

func TestEngineJobTimeoutCancelsHandler(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })

	interrupted := make(chan error, 1)
	e.Handle("slow", func(ctx context.Context, _ *batchjobs.Job) error {
		select {
		case <-ctx.Done():
			interrupted <- ctx.Err()
			return ctx.Err()
		case <-time.After(10 * time.Second):
			interrupted <- nil
			return nil
		}
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if _, err := e.Enqueue(ctx, "slow", nil, batchjobs.WithTimeout(50*time.Millisecond)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case err := <-interrupted:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("handler context error = %v, want DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler's context was never cancelled by the job timeout")
	}
}

func TestEngineRecoverOnAnIdleQueue(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	rec, err := e.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Total() != 0 {
		t.Errorf("Recover on an idle queue = %+v, want nothing reclaimed", rec)
	}

	n, err := e.ExpiredLeases()
	if err != nil {
		t.Fatalf("ExpiredLeases: %v", err)
	}
	if n != 0 {
		t.Errorf("ExpiredLeases = %d, want 0", n)
	}
}

func TestEngineMetricsHandler(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })
	e.Handle("emails", func(context.Context, *batchjobs.Job) error { return nil })

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if _, err := e.Enqueue(ctx, "emails", nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, "the job to be recorded", func() bool {
		stats, err := e.Stats(ctx)
		return err == nil && stats.Get(batchjobs.StatusSucceeded) == 1
	})

	rec := httptest.NewRecorder()
	e.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"batchjobs_jobs_processed_total", "batchjobs_queue_depth", "batchjobs_pool_workers"} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape output is missing %s", want)
		}
	}
}

func TestEngineAPIHandlerServesTheSameQueue(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	ts := httptest.NewServer(e.APIHandler())
	defer ts.Close()

	c, err := client.New(ts.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	// Enqueued over HTTP, visible in-process: one queue, two doors.
	remote, err := c.Enqueue(ctx, "emails", []byte("hello"))
	if err != nil {
		t.Fatalf("client Enqueue: %v", err)
	}
	local, err := e.Get(ctx, remote.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(local.Payload) != "hello" {
		t.Errorf("payload = %q, want hello", local.Payload)
	}

	// And the reverse.
	direct, err := e.Enqueue(ctx, "reports", []byte("nightly"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := c.Get(ctx, direct.ID); err != nil {
		t.Errorf("client Get on a locally enqueued job = %v, want nil", err)
	}

	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatalf("client Stats: %v", err)
	}
	if got := stats.Get(client.StatusPending); got != 2 {
		t.Errorf("pending over HTTP = %d, want 2", got)
	}
}

func TestEngineAPIHandlerOptions(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	ts := httptest.NewServer(e.APIHandler(batchjobs.APIOptions{
		AuthToken: "secret",
		Dashboard: true,
	}))
	defer ts.Close()

	anonymous, err := client.New(ts.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if _, err := anonymous.Stats(ctx); !errors.Is(err, client.ErrUnauthorized) {
		t.Errorf("Stats without a key = %v, want ErrUnauthorized", err)
	}

	authorized, err := client.New(ts.URL, client.WithAPIKey("secret"))
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if _, err := authorized.Stats(ctx); err != nil {
		t.Errorf("Stats with the key = %v, want nil", err)
	}

	// The auth middleware covers everything but /healthz, the dashboard's
	// assets included, so a browser cannot load the UI from a
	// token-protected handler -- which is why the token is documented as a
	// development stub rather than a way to publish a dashboard.
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-API-Key", "secret")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / = %d, want 200 with the dashboard mounted", resp.StatusCode)
	}
}

func TestEngineAPIHandlerWithoutDashboard(t *testing.T) {
	e := newEngine(t)

	// The zero APIOptions leaves the root unmounted: an application that
	// only wants the REST API should not find a web UI on its listener.
	ts := httptest.NewServer(e.APIHandler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET / = %d, want 404 with no dashboard requested", resp.StatusCode)
	}
}

func TestEngineAutoscalingPoolRunsJobs(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) {
		c.Workers = 1
		c.MinWorkers = 1
		c.MaxWorkers = 4
	})

	var mu sync.Mutex
	done := 0
	e.Handle("bulk", func(context.Context, *batchjobs.Job) error {
		mu.Lock()
		done++
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	const total = 25
	for range total {
		if _, err := e.Enqueue(ctx, "bulk", nil); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	waitFor(t, "the autoscaling pool to drain the backlog", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return done == total
	})
}
