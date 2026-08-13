package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

func TestSchedulerRecordsMetricsForSuccessfulFire(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	mustDueSchedule(t, store)
	m := metrics.New(prometheus.NewRegistry())

	s := &Scheduler{Store: store, Queue: q, PollInterval: 5 * time.Millisecond, Metrics: m}
	s.Start(context.Background())
	defer s.Stop()

	waitForScrapeContains(t, m, "batchjobs_schedule_fires_total 1")
}

func TestSchedulerRecordsMetricsForFireError(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	mustDueSchedule(t, store)
	m := metrics.New(prometheus.NewRegistry())

	// corruptingStore rewrites the CronExpr on every schedule Due returns
	// to an unparsable value, so Scheduler.fire's Advance step fails
	// deterministically without a Store implementation that skips New's
	// cron validation.
	s := &Scheduler{Store: &corruptingStore{Store: store}, Queue: q, PollInterval: 5 * time.Millisecond, Metrics: m}
	s.Start(context.Background())
	defer s.Stop()

	waitForScrapeContains(t, m, "batchjobs_schedule_errors_total 1")
}

type corruptingStore struct {
	schedule.Store
}

func (c *corruptingStore) Due(now time.Time) ([]*schedule.Schedule, error) {
	due, err := c.Store.Due(now)
	if err != nil {
		return nil, err
	}
	for _, sch := range due {
		sch.CronExpr = "not a cron expression"
	}
	return due, nil
}

// waitForScrapeContains polls m's Prometheus scrape output until it
// contains want or fails the test after a timeout, since Scheduler records
// metrics from its polling goroutine asynchronously.
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
