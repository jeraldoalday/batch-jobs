package batchjobs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs"
)

// quietLogger keeps the engine's operational logging out of test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newEngine opens an Engine on a private database file in the test's temp
// directory, closed when the test ends. A file rather than MemoryDB, so
// tests that restart an engine see the same jobs the first one wrote.
func newEngine(t *testing.T, configure ...func(*batchjobs.Config)) *batchjobs.Engine {
	t.Helper()

	cfg := batchjobs.Config{
		DB:           filepath.Join(t.TempDir(), "jobs.db"),
		Workers:      2,
		Logger:       quietLogger(),
		RetryBase:    time.Millisecond,
		RetryMax:     5 * time.Millisecond,
		ReapInterval: -1,
	}
	for _, fn := range configure {
		fn(&cfg)
	}

	e, err := batchjobs.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// waitFor polls cond until it holds or the deadline passes, so tests
// synchronize on the engine's observable state rather than on sleeps.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestOpenValidatesConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  batchjobs.Config
	}{
		{"no database", batchjobs.Config{}},
		{"negative workers", batchjobs.Config{DB: batchjobs.MemoryDB, Workers: -1}},
		{"min above max", batchjobs.Config{DB: batchjobs.MemoryDB, MinWorkers: 9, MaxWorkers: 2}},
		{"jitter above one", batchjobs.Config{DB: batchjobs.MemoryDB, RetryJitter: 1.5}},
		{"jitter below zero", batchjobs.Config{DB: batchjobs.MemoryDB, RetryJitter: -0.1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := batchjobs.Open(tt.cfg)
			if err == nil {
				e.Close()
				t.Fatal("Open with an invalid config = nil error, want one")
			}
		})
	}
}

func TestEngineRunsJobs(t *testing.T) {
	e := newEngine(t)

	var mu sync.Mutex
	var seen []string
	e.Handle("emails", func(_ context.Context, j *batchjobs.Job) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, string(j.Payload))
		return nil
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	for _, payload := range []string{"a", "b", "c"} {
		if _, err := e.Enqueue(ctx, "emails", []byte(payload)); err != nil {
			t.Fatalf("Enqueue(%s): %v", payload, err)
		}
	}

	// Wait on the stored outcome, not on the handler having been called:
	// the pool writes a job's terminal state back after the handler
	// returns, so the last handler can be done while its row still says
	// running.
	waitFor(t, "all three jobs to be recorded as succeeded", func() bool {
		stats, err := e.Stats(ctx)
		return err == nil && stats.Get(batchjobs.StatusSucceeded) == 3
	})

	mu.Lock()
	ran := len(seen)
	mu.Unlock()
	if ran != 3 {
		t.Errorf("handler ran %d times, want 3", ran)
	}

	stats, err := e.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got := stats.Get(batchjobs.StatusSucceeded); got != 3 {
		t.Errorf("succeeded = %d, want 3", got)
	}
	if got := stats.Active(); got != 0 {
		t.Errorf("Active() = %d, want 0 once everything has run", got)
	}
}

func TestEngineRetriesThenDeadLetters(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })

	var attempts int
	var mu sync.Mutex
	e.Handle("flaky", func(_ context.Context, j *batchjobs.Job) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return fmt.Errorf("attempt %d failed", j.Attempt())
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	created, err := e.Enqueue(ctx, "flaky", []byte("x"), batchjobs.WithMaxRetries(2))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// One attempt plus two retries, then the job is dead.
	waitFor(t, "the job to exhaust its retries", func() bool {
		j, err := e.Get(ctx, created.ID)
		return err == nil && j.Status == batchjobs.StatusDead
	})

	mu.Lock()
	got := attempts
	mu.Unlock()
	if got != 3 {
		t.Errorf("handler ran %d times, want 3 (one attempt + two retries)", got)
	}

	dead, err := e.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if dead.LastError == "" {
		t.Error("dead job has no LastError, want the handler's message")
	}
	if dead.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", dead.RetryCount)
	}
	if !dead.Status.Terminal() {
		t.Error("dead is not reported as terminal")
	}
}

func TestEnginePermanentErrorSkipsRetries(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })

	var attempts int
	var mu sync.Mutex
	e.Handle("bad-payload", func(context.Context, *batchjobs.Job) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return batchjobs.Permanent(errors.New("payload will never parse"))
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	// Retries are available and must go unused: the handler said the error
	// is not the kind that another attempt fixes.
	created, err := e.Enqueue(ctx, "bad-payload", []byte("x"), batchjobs.WithMaxRetries(5))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, "the job to fail permanently", func() bool {
		j, err := e.Get(ctx, created.ID)
		return err == nil && j.Status == batchjobs.StatusFailed
	})

	mu.Lock()
	got := attempts
	mu.Unlock()
	if got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}

	// StatusFailed, not StatusDead: "this job is wrong" rather than "we
	// could not get through".
	j, err := e.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if j.Status != batchjobs.StatusFailed {
		t.Errorf("status = %s, want failed", j.Status)
	}
}

func TestEngineUnhandledQueueFailsPermanently(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })
	e.Handle("emails", func(context.Context, *batchjobs.Job) error { return nil })

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	created, err := e.Enqueue(ctx, "nobody-handles-this", nil, batchjobs.WithMaxRetries(3))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, "the unhandled job to fail", func() bool {
		j, err := e.Get(ctx, created.ID)
		return err == nil && j.Status == batchjobs.StatusFailed
	})

	j, _ := e.Get(ctx, created.ID)
	// The error names the queue, so an operator reading it knows which
	// registration is missing rather than only that one is.
	if j.LastError == "" || !contains(j.LastError, "nobody-handles-this") {
		t.Errorf("LastError = %q, want it to name the unhandled queue", j.LastError)
	}
	if j.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0: retrying cannot make a handler appear", j.RetryCount)
	}
}

func TestEngineFallbackHandler(t *testing.T) {
	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })

	var mu sync.Mutex
	var handled []string
	e.HandleFallback(func(_ context.Context, j *batchjobs.Job) error {
		mu.Lock()
		defer mu.Unlock()
		handled = append(handled, j.Queue)
		return nil
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if _, err := e.Enqueue(ctx, "anything", nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, "the fallback to run", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 1
	})
	if handled[0] != "anything" {
		t.Errorf("fallback saw queue %q, want anything", handled[0])
	}
}

func TestEngineJobOptions(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	runAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	j, err := e.Enqueue(ctx, "later", []byte("payload"),
		batchjobs.WithPriority(batchjobs.PriorityHigh),
		batchjobs.WithMaxRetries(4),
		batchjobs.WithTimeout(90*time.Second),
		batchjobs.WithIdempotencyKey("key-1"),
		batchjobs.WithRunAt(runAt),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if j.Priority != batchjobs.PriorityHigh {
		t.Errorf("Priority = %s, want high", j.Priority)
	}
	if j.MaxRetries != 4 {
		t.Errorf("MaxRetries = %d, want 4", j.MaxRetries)
	}
	// Sub-second precision survives in-process, unlike over the HTTP API.
	if j.Timeout != 90*time.Second {
		t.Errorf("Timeout = %v, want 90s", j.Timeout)
	}
	if j.IdempotencyKey != "key-1" {
		t.Errorf("IdempotencyKey = %q, want key-1", j.IdempotencyKey)
	}
	if !j.RunAt.Equal(runAt) {
		t.Errorf("RunAt = %v, want %v", j.RunAt, runAt)
	}
	if got := j.AttemptsLeft(); got != 4 {
		t.Errorf("AttemptsLeft() = %d, want 4", got)
	}

	// An explicit instant wins over a relative delay regardless of the
	// order the two options were given in.
	both, err := e.Enqueue(ctx, "later", nil,
		batchjobs.WithRunAt(runAt),
		batchjobs.WithDelay(time.Minute),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !both.RunAt.Equal(runAt) {
		t.Errorf("RunAt with both options = %v, want the explicit instant %v", both.RunAt, runAt)
	}
}

func TestEngineIdempotentEnqueue(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	first, created, err := e.EnqueueResult(ctx, "payments", []byte("charge"),
		batchjobs.WithIdempotencyKey("order-42"))
	if err != nil {
		t.Fatalf("first EnqueueResult: %v", err)
	}
	if !created {
		t.Error("first enqueue reported a deduplication, want a creation")
	}

	second, created, err := e.EnqueueResult(ctx, "payments", []byte("charge"),
		batchjobs.WithIdempotencyKey("order-42"))
	if err != nil {
		t.Fatalf("second EnqueueResult: %v", err)
	}
	if created {
		t.Error("second enqueue reported a creation, want a deduplication")
	}
	if second.ID != first.ID {
		t.Errorf("deduplicated ID = %s, want the original %s", second.ID, first.ID)
	}
	if e.Len() != 1 {
		t.Errorf("queue holds %d jobs, want 1", e.Len())
	}
}

func TestEngineEnqueueJSONRoundTrip(t *testing.T) {
	type message struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}

	e := newEngine(t, func(c *batchjobs.Config) { c.Workers = 1 })

	got := make(chan message, 1)
	e.Handle("emails", func(_ context.Context, j *batchjobs.Job) error {
		var msg message
		if err := j.Decode(&msg); err != nil {
			return batchjobs.Permanent(err)
		}
		got <- msg
		return nil
	})

	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	want := message{To: "a@example.com", Subject: "hi"}
	if _, err := e.EnqueueJSON(ctx, "emails", want); err != nil {
		t.Fatalf("EnqueueJSON: %v", err)
	}

	select {
	case msg := <-got:
		if msg != want {
			t.Errorf("handler decoded %+v, want %+v", msg, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
	}
}

func TestEngineCancelAndRetryJob(t *testing.T) {
	// No workers started, so the job stays pending and cancellable.
	e := newEngine(t)
	ctx := context.Background()

	created, err := e.Enqueue(ctx, "reports", nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.Cancel(ctx, created.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	cancelled, err := e.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if cancelled.Status != batchjobs.StatusCancelled {
		t.Errorf("status = %s, want cancelled", cancelled.Status)
	}

	// A cancelled job is terminal but not dead or failed, so there is
	// nothing to revive.
	if err := e.Cancel(ctx, created.ID); !errors.Is(err, batchjobs.ErrNotCancelable) {
		t.Errorf("second Cancel = %v, want ErrNotCancelable", err)
	}
	if err := e.RetryJob(ctx, created.ID); !errors.Is(err, batchjobs.ErrNotRetryable) {
		t.Errorf("RetryJob on a cancelled job = %v, want ErrNotRetryable", err)
	}
}

func TestEngineNotFound(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	if _, err := e.Get(ctx, "missing"); !errors.Is(err, batchjobs.ErrNotFound) {
		t.Errorf("Get(missing) = %v, want ErrNotFound", err)
	}
	if err := e.Cancel(ctx, "missing"); !errors.Is(err, batchjobs.ErrNotFound) {
		t.Errorf("Cancel(missing) = %v, want ErrNotFound", err)
	}
	if err := e.RetryJob(ctx, "missing"); !errors.Is(err, batchjobs.ErrNotFound) {
		t.Errorf("RetryJob(missing) = %v, want ErrNotFound", err)
	}
}

func TestEngineArgumentValidation(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	if _, err := e.Enqueue(ctx, "", nil); err == nil {
		t.Error("Enqueue with an empty queue = nil error, want one")
	}
	if _, err := e.Get(ctx, ""); err == nil {
		t.Error("Get with an empty ID = nil error, want one")
	}
	if _, err := e.List(ctx, batchjobs.Status("archived"), 10); err == nil {
		t.Error("List with an unknown status = nil error, want one")
	}
	if _, err := e.List(ctx, batchjobs.StatusPending, 0); err == nil {
		t.Error("List with a zero limit = nil error, want one")
	}
	if err := e.Cancel(ctx, ""); err == nil {
		t.Error("Cancel with an empty ID = nil error, want one")
	}
}

func TestEngineRejectsCancelledContext(t *testing.T) {
	e := newEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := e.Enqueue(ctx, "emails", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Enqueue with a cancelled context = %v, want context.Canceled", err)
	}
	// Nothing was written: a caller whose request is gone should not still
	// be creating work.
	if e.Len() != 0 {
		t.Errorf("queue holds %d jobs after a cancelled enqueue, want 0", e.Len())
	}
	if _, err := e.Stats(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Stats with a cancelled context = %v, want context.Canceled", err)
	}
}

func TestEngineListByStatus(t *testing.T) {
	e := newEngine(t)
	ctx := context.Background()

	for range 3 {
		if _, err := e.Enqueue(ctx, "reports", nil); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	jobs, err := e.List(ctx, batchjobs.StatusPending, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != 3 {
		t.Errorf("List returned %d jobs, want 3", len(jobs))
	}

	// The limit is honored, not just the filter.
	jobs, err = e.List(ctx, batchjobs.StatusPending, 2)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("List with limit 2 returned %d jobs, want 2", len(jobs))
	}

	// A status with nothing in it is an empty result, not an error.
	jobs, err = e.List(ctx, batchjobs.StatusDead, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("List(dead) returned %d jobs, want 0", len(jobs))
	}
}

func TestEngineQueuesAndDuplicateHandlerPanics(t *testing.T) {
	e := newEngine(t)
	noop := func(context.Context, *batchjobs.Job) error { return nil }

	e.Handle("reports", noop)
	e.Handle("emails", noop)

	want := []string{"emails", "reports"}
	got := e.Queues()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("Queues() = %v, want %v", got, want)
	}

	defer func() {
		if recover() == nil {
			t.Error("registering a second handler for the same queue did not panic")
		}
	}()
	e.Handle("emails", noop)
}

func TestEngineNilHandlerPanics(t *testing.T) {
	e := newEngine(t)

	defer func() {
		if recover() == nil {
			t.Error("Handle with a nil handler did not panic")
		}
	}()
	e.Handle("emails", nil)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
