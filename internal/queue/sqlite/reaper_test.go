package sqlite

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
)

// quietLogger keeps the reaper's warn-level "recovered orphaned jobs"
// lines out of the test output, where they read as failures.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitFor polls cond until it holds or the deadline passes, so the tests
// do not depend on a particular sweep landing within a fixed sleep.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func TestReaperRecoversOnStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	id := crashedQueue(t, path, job.WithMaxRetries(2))

	q, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	// A long interval: if the job comes back, it can only be from the
	// immediate sweep Start performs, which is the one that matters when a
	// process is replacing a crashed peer.
	r := &Reaper{Queue: q, Interval: time.Hour, Logger: quietLogger()}
	r.Start(context.Background())
	defer r.Stop()

	waitFor(t, 2*time.Second, "the orphaned job to be requeued", func() bool {
		got, err := q.Get(id)
		return err == nil && got.Status == job.StatusPending
	})
}

func TestReaperSweepsOnInterval(t *testing.T) {
	q := openLeasedQueue(t, Options{LeaseTTL: time.Nanosecond})

	r := &Reaper{Queue: q, Interval: 10 * time.Millisecond, Logger: quietLogger()}
	r.Start(context.Background())
	defer r.Stop()

	// Orphan a job after the reaper is already running, so only a later
	// tick can rescue it.
	j := job.New("q", []byte("work"), job.WithMaxRetries(2))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	waitFor(t, 2*time.Second, "a periodic sweep to requeue the job", func() bool {
		got, err := q.Get(j.ID)
		return err == nil && got.Status == job.StatusPending
	})
}

func TestReaperCountsRecoveries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	crashedQueue(t, path, job.WithMaxRetries(2))

	q, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	m := metrics.New(prometheus.NewRegistry())
	r := &Reaper{Queue: q, Interval: time.Hour, Logger: quietLogger(), Metrics: m}
	r.Start(context.Background())
	defer r.Stop()

	waitFor(t, 2*time.Second, "the recovery counter to move", func() bool {
		return strings.Contains(scrape(m), `batchjobs_jobs_recovered_total{outcome="requeued"} 1`)
	})
}

// scrape renders m in Prometheus text format, which is all this package can
// see of another package's unexported collectors.
func scrape(m *metrics.Metrics) string {
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestReaperStopIsIdempotent(t *testing.T) {
	q := openLeasedQueue(t, Options{})

	r := &Reaper{Queue: q, Interval: 10 * time.Millisecond, Logger: quietLogger()}
	r.Start(context.Background())
	r.Stop()
	r.Stop()
}

func TestReaperStopsWithContext(t *testing.T) {
	q := openLeasedQueue(t, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	r := &Reaper{Queue: q, Interval: 10 * time.Millisecond, Logger: quietLogger()}
	r.Start(ctx)

	cancel()
	done := make(chan struct{})
	go func() {
		r.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Reaper did not exit after its context was cancelled")
	}
}

func TestReaperUsesDefaultInterval(t *testing.T) {
	q := openLeasedQueue(t, Options{})

	r := &Reaper{Queue: q, Logger: quietLogger()}
	r.Start(context.Background())
	defer r.Stop()

	if r.Interval != DefaultReapInterval {
		t.Fatalf("Interval = %v, want DefaultReapInterval (%v)", r.Interval, DefaultReapInterval)
	}
}
