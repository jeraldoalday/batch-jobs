package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/client"
	"github.com/jeraldoalday/batch-jobs/internal/api"
	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
)

// newServer wires the real HTTP API to a real SQLite-backed queue and
// serves it over an actual listener, so these tests exercise the client
// against the same code a deployment runs -- not a stub that agrees with
// the client's assumptions by construction.
func newServer(t *testing.T, configure ...func(*api.Server)) (*httptest.Server, *sqlite.Queue) {
	t.Helper()

	q, err := sqlite.Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	srv := &api.Server{
		Store:  q,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		// A short push interval keeps the event-stream test quick.
		EventInterval: 50 * time.Millisecond,
	}
	for _, fn := range configure {
		fn(srv)
	}

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, q
}

func newClient(t *testing.T, ts *httptest.Server, opts ...client.Option) *client.Client {
	t.Helper()
	c, err := client.New(ts.URL, opts...)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

func TestIntegrationJobLifecycle(t *testing.T) {
	ts, _ := newServer(t)
	c := newClient(t, ts)
	ctx := context.Background()

	created, err := c.Enqueue(ctx, "emails", []byte("hello"),
		client.WithPriority(client.PriorityHigh),
		client.WithMaxRetries(2),
		client.WithTimeout(45*time.Second),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if created.ID == "" {
		t.Fatal("Enqueue returned a job with no ID")
	}
	if created.Status != client.StatusPending {
		t.Errorf("status = %s, want pending", created.Status)
	}
	if created.Priority != client.PriorityHigh {
		t.Errorf("priority = %s, want high", created.Priority)
	}
	// The round trip through the API's whole-second encoding must come
	// back as the duration the caller asked for.
	if created.Timeout != 45*time.Second {
		t.Errorf("timeout = %v, want 45s", created.Timeout)
	}
	if string(created.Payload) != "hello" {
		t.Errorf("payload = %q, want hello", created.Payload)
	}

	got, err := c.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID || string(got.Payload) != "hello" {
		t.Errorf("Get returned %+v, want the job just created", got)
	}

	jobs, err := c.List(ctx, client.StatusPending, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != created.ID {
		t.Fatalf("List returned %d jobs, want the one just created", len(jobs))
	}

	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got := stats.Get(client.StatusPending); got != 1 {
		t.Errorf("pending = %d, want 1", got)
	}
	if got := stats.Backlog(); got != 1 {
		t.Errorf("Backlog() = %d, want 1", got)
	}

	if err := c.Cancel(ctx, created.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	cancelled, err := c.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get after Cancel: %v", err)
	}
	if cancelled.Status != client.StatusCancelled {
		t.Errorf("status after Cancel = %s, want cancelled", cancelled.Status)
	}
	// Cancelling twice loses the race with itself: the job is already in a
	// terminal state, which the API reports as a conflict.
	if err := c.Cancel(ctx, created.ID); !errors.Is(err, client.ErrConflict) {
		t.Errorf("second Cancel = %v, want ErrConflict", err)
	}
}

func TestIntegrationIdempotentEnqueue(t *testing.T) {
	ts, _ := newServer(t)
	c := newClient(t, ts)
	ctx := context.Background()

	first, created, err := c.EnqueueResult(ctx, "payments", []byte("charge"),
		client.WithIdempotencyKey("order-42"))
	if err != nil {
		t.Fatalf("first EnqueueResult: %v", err)
	}
	if !created {
		t.Error("first enqueue reported a deduplication, want a creation")
	}

	second, created, err := c.EnqueueResult(ctx, "payments", []byte("charge"),
		client.WithIdempotencyKey("order-42"))
	if err != nil {
		t.Fatalf("second EnqueueResult: %v", err)
	}
	if created {
		t.Error("second enqueue reported a creation, want a deduplication")
	}
	if second.ID != first.ID {
		t.Errorf("deduplicated job ID = %s, want the original %s", second.ID, first.ID)
	}

	// The point of the key: exactly one job exists, not two.
	jobs, err := c.List(ctx, client.StatusPending, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != 1 {
		t.Errorf("queue holds %d jobs, want 1", len(jobs))
	}
}

func TestIntegrationNotFound(t *testing.T) {
	ts, _ := newServer(t)
	c := newClient(t, ts)
	ctx := context.Background()

	if _, err := c.Get(ctx, "missing"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("Get(missing) = %v, want ErrNotFound", err)
	}
	if err := c.Cancel(ctx, "missing"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("Cancel(missing) = %v, want ErrNotFound", err)
	}
	if err := c.Retry(ctx, "missing"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("Retry(missing) = %v, want ErrNotFound", err)
	}
}

func TestIntegrationInvalidRequestsRejected(t *testing.T) {
	ts, _ := newServer(t)
	c := newClient(t, ts)
	ctx := context.Background()

	// A status this client knows but the server does not is the shape of a
	// version skew; an outright unknown one must be a 400, not a 500.
	if _, err := c.List(ctx, client.Status("archived"), 10); !errors.Is(err, client.ErrInvalidRequest) {
		t.Errorf("List(archived) = %v, want ErrInvalidRequest", err)
	}
	if _, err := c.Enqueue(ctx, "emails", nil, client.WithPriority(client.Priority(7))); !errors.Is(err, client.ErrInvalidRequest) {
		t.Errorf("Enqueue with priority 7 = %v, want ErrInvalidRequest", err)
	}
	if _, err := c.Enqueue(ctx, "emails", nil, client.WithMaxRetries(-1)); !errors.Is(err, client.ErrInvalidRequest) {
		t.Errorf("Enqueue with max_retries -1 = %v, want ErrInvalidRequest", err)
	}
}

func TestIntegrationAuthToken(t *testing.T) {
	ts, _ := newServer(t, func(s *api.Server) { s.AuthToken = "secret" })
	ctx := context.Background()

	anonymous := newClient(t, ts)
	if _, err := anonymous.Stats(ctx); !errors.Is(err, client.ErrUnauthorized) {
		t.Errorf("Stats without a key = %v, want ErrUnauthorized", err)
	}
	// Health is exempt, so it works as a reachability probe before
	// credentials are configured.
	if err := anonymous.Health(ctx); err != nil {
		t.Errorf("Health without a key = %v, want it to be exempt", err)
	}

	authorized := newClient(t, ts, client.WithAPIKey("secret"))
	if _, err := authorized.Stats(ctx); err != nil {
		t.Errorf("Stats with the right key = %v, want nil", err)
	}
	wrong := newClient(t, ts, client.WithAPIKey("nope"))
	if _, err := wrong.Stats(ctx); !errors.Is(err, client.ErrUnauthorized) {
		t.Errorf("Stats with the wrong key = %v, want ErrUnauthorized", err)
	}
}

func TestIntegrationRetryRevivesDeadJob(t *testing.T) {
	ts, q := newServer(t)
	c := newClient(t, ts)
	ctx := context.Background()

	created, err := c.Enqueue(ctx, "reports", []byte("nightly"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Drive the job to a dead state through the engine, the way a worker
	// that exhausted its retries would.
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	claimed.MarkFailed(time.Now().UTC(), errors.New("nightly report failed"), 0)
	if err := q.Complete(claimed); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	dead, err := c.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if dead.Status != client.StatusDead {
		t.Fatalf("status = %s, want dead", dead.Status)
	}
	if dead.LastError == "" {
		t.Error("dead job carries no LastError, want the handler's message")
	}
	if !dead.Status.Terminal() {
		t.Error("dead is not reported as terminal")
	}

	if err := c.Retry(ctx, created.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	revived, err := c.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get after Retry: %v", err)
	}
	if revived.Status != client.StatusPending {
		t.Errorf("status after Retry = %s, want pending", revived.Status)
	}
	if revived.RetryCount != 0 || revived.LastError != "" {
		t.Errorf("revived job = retries %d / error %q, want both cleared", revived.RetryCount, revived.LastError)
	}

	// A pending job is not retryable, so a second Retry is a conflict.
	if err := c.Retry(ctx, created.ID); !errors.Is(err, client.ErrConflict) {
		t.Errorf("Retry on a pending job = %v, want ErrConflict", err)
	}
}

func TestIntegrationRecover(t *testing.T) {
	ts, _ := newServer(t)
	c := newClient(t, ts)

	// Nothing is orphaned, so the pass is a clean no-op -- the point is
	// that the route exists and decodes, since a store that does lease
	// jobs must not answer 501.
	rec, err := c.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Total() != 0 || rec.Remaining != 0 {
		t.Errorf("Recover on an idle queue = %+v, want zeros", rec)
	}
}

func TestIntegrationEventStream(t *testing.T) {
	ts, _ := newServer(t)
	c := newClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := c.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	defer stream.Close()

	// The server pushes a snapshot immediately, so the first frame does
	// not wait out an interval.
	first, err := stream.Next()
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if first.Total() != 0 {
		t.Errorf("first snapshot = %s, want an empty queue", first)
	}

	if _, err := c.Enqueue(ctx, "emails", []byte("hello")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Later snapshots pick the new job up. Read until one does rather than
	// assuming the very next frame was produced after the enqueue landed.
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats, err := stream.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if stats.Get(client.StatusPending) == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats never reflected the enqueued job, last = %s", stats)
		}
	}
}
