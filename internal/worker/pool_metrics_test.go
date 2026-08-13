package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestPoolRecordsMetricsForSucceededJob(t *testing.T) {
	q := queue.NewMemory()
	m := metrics.New(prometheus.NewRegistry())

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			return nil
		},
		Concurrency: 1,
		Metrics:     m,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("emails", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitForScrapeContains(t, m, `batchjobs_jobs_processed_total{queue="emails",status="succeeded"} 1`)
}

func TestPoolRecordsMetricsForDeadLetteredJob(t *testing.T) {
	q := queue.NewMemory()
	dlq := queue.NewMemory()
	m := metrics.New(prometheus.NewRegistry())

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			return errors.New("always fails")
		},
		Concurrency: 1,
		Backoff:     ConstantBackoff(time.Millisecond),
		DeadLetter:  dlq,
		Metrics:     m,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("emails", nil, job.WithMaxRetries(1))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return dlq.Len() == 1 })

	waitForScrapeContains(t, m, `batchjobs_jobs_processed_total{queue="emails",status="retrying"} 1`)
	waitForScrapeContains(t, m, `batchjobs_jobs_processed_total{queue="emails",status="dead"} 1`)
}

func TestPoolMetricsFieldOptional(t *testing.T) {
	q := queue.NewMemory()

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			return nil
		},
		Concurrency: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("emails", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A Pool with no Metrics set must process jobs normally rather than
	// panicking on the nil field.
	waitFor(t, func() bool { return q.Len() == 0 })
}

// waitForScrapeContains polls m's Prometheus scrape output until it
// contains want or fails the test after a timeout, since Pool records
// metrics from worker goroutines asynchronously to the enqueue call.
func waitForScrapeContains(t *testing.T, m *metrics.Metrics, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec := httptest.NewRecorder()
		m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if strings.Contains(rec.Body.String(), want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scrape output never contained %q", want)
}
