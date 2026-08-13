package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestSchedulerRecordsTickDuration(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	mustDueSchedule(t, store)
	m := metrics.New(prometheus.NewRegistry())

	// A long PollInterval keeps the periodic ticker from firing during
	// the test, so only Start's immediate first tick runs and the
	// histogram count is deterministic.
	s := &Scheduler{Store: store, Queue: q, PollInterval: time.Hour, Metrics: m}
	s.Start(context.Background())
	defer s.Stop()

	waitForScrapeContains(t, m, "batchjobs_scheduler_tick_duration_seconds_count 1")
}
