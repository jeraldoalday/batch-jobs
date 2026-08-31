package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// ObservePruned records that n jobs were deleted from a terminal status by
// a retention pass.
//
// The status label is the point of the metric rather than decoration. A
// queue steadily pruning successes is healthy housekeeping; the same rate
// against the dead-letter status means work is being discarded before
// anyone looked at it, which is a policy someone should have chosen
// deliberately and may not have.
func (m *Metrics) ObservePruned(status job.Status, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.jobsPruned.WithLabelValues(string(status)).Add(float64(n))
}

// RegisterRetainedJobs registers a gauge reporting how many finished jobs
// the queue is holding, sampled at scrape time via retained (typically a
// queue/sqlite.Queue's Retained method with its error folded away).
//
// Under a working policy this settles at roughly the amount of history the
// policy permits. A line that climbs without flattening is the shape of
// retention not running at all, or of jobs finishing faster than a pass
// can delete them.
func (m *Metrics) RegisterRetainedJobs(queueName string, retained func() int) error {
	if m == nil {
		return nil
	}
	g := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   namespace,
		Name:        "retained_jobs",
		Help:        "Number of finished jobs currently kept as history, which a retention policy governs.",
		ConstLabels: prometheus.Labels{"queue": queueName},
	}, func() float64 { return float64(retained()) })
	return m.reg.Register(g)
}

// RegisterOldestRetainedAge registers a gauge reporting the age, in
// seconds, of the oldest finished job still held.
//
// It is the counterpart to the count, and catches what the count cannot: a
// queue whose history is small but has aged far past what its policy
// allows -- pruning that stopped some time ago on a queue that has since
// gone quiet. Zero means no finished jobs are being held at all.
func (m *Metrics) RegisterOldestRetainedAge(queueName string, ageSeconds func() float64) error {
	if m == nil {
		return nil
	}
	g := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   namespace,
		Name:        "oldest_retained_job_age_seconds",
		Help:        "Age of the oldest finished job still kept as history, or 0 if none is.",
		ConstLabels: prometheus.Labels{"queue": queueName},
	}, ageSeconds)
	return m.reg.Register(g)
}
