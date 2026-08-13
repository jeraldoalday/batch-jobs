// Package metrics provides Prometheus-compatible instrumentation for job
// and schedule lifecycle events emitted by worker.Pool and
// scheduler.Scheduler.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// namespace prefixes every metric name registered by this package, e.g.
// batchjobs_jobs_processed_total.
const namespace = "batchjobs"

// Metrics collects Prometheus metrics for a running Pool and/or Scheduler.
// A nil *Metrics is safe to use as a Pool's or Scheduler's Metrics field:
// every method on a nil receiver is a no-op, so instrumentation is entirely
// optional and does not require a distinct no-op implementation.
type Metrics struct {
	reg *prometheus.Registry

	jobsProcessed *prometheus.CounterVec
	jobDuration   *prometheus.HistogramVec

	scheduleFires  prometheus.Counter
	scheduleErrors prometheus.Counter
}

// New creates a Metrics collector and registers its collectors on reg.
// Callers typically pass prometheus.NewRegistry() to keep the collector set
// isolated (e.g. per-test), or their process's existing registry to expose
// batch-jobs metrics alongside others.
func New(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		reg: reg,
		jobsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_processed_total",
			Help:      "Total number of jobs processed, labeled by queue and terminal status.",
		}, []string{"queue", "status"}),
		jobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_duration_seconds",
			Help:      "Time spent in a job's handler, labeled by queue and outcome status.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"queue", "status"}),
		scheduleFires: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "schedule_fires_total",
			Help:      "Total number of due schedules successfully fired: job enqueued and next run time advanced.",
		}),
		scheduleErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "schedule_errors_total",
			Help:      "Total number of errors encountered while firing a due schedule.",
		}),
	}
	reg.MustRegister(m.jobsProcessed, m.jobDuration, m.scheduleFires, m.scheduleErrors)
	return m
}

// ObserveJob records the outcome of a single job execution: it increments
// the per-queue, per-status processed counter and records d in the
// matching duration histogram. status is expected to be one of the
// terminal-ish outcomes worker.Pool assigns after running a handler
// (StatusSucceeded, StatusRetrying, or StatusDead).
func (m *Metrics) ObserveJob(queue string, status job.Status, d time.Duration) {
	if m == nil {
		return
	}
	m.jobsProcessed.WithLabelValues(queue, string(status)).Inc()
	m.jobDuration.WithLabelValues(queue, string(status)).Observe(d.Seconds())
}
