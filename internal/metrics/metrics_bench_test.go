package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// BenchmarkObserveJob measures the per-call cost of recording a job
// outcome, since this runs on Pool's hot path once per processed job.
func BenchmarkObserveJob(b *testing.B) {
	m := New(prometheus.NewRegistry())

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.ObserveJob("emails", job.StatusSucceeded, 10*time.Millisecond)
	}
}

// BenchmarkObserveJob_ManyQueues stresses the CounterVec/HistogramVec label
// cache with a rotating set of queue names, closer to a multi-tenant
// deployment than a single fixed queue label.
func BenchmarkObserveJob_ManyQueues(b *testing.B) {
	m := New(prometheus.NewRegistry())
	queues := []string{"emails", "reports", "webhooks", "exports", "imports"}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.ObserveJob(queues[i%len(queues)], job.StatusSucceeded, 10*time.Millisecond)
	}
}
