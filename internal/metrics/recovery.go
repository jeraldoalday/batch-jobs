package metrics

import "github.com/prometheus/client_golang/prometheus"

// ObserveRecovery records the outcome of one lease-recovery pass:
// how many orphaned jobs were put back in the queue and how many had no
// retries left and were dead-lettered.
//
// The two are counted separately because they mean different things
// operationally. A steady trickle of requeues is a cluster losing workers
// and healing itself; dead-lettered recoveries are jobs that will never run
// again without someone intervening, and are worth alerting on.
func (m *Metrics) ObserveRecovery(requeued, deadLettered int) {
	if m == nil {
		return
	}
	if requeued > 0 {
		m.jobsRecovered.WithLabelValues("requeued").Add(float64(requeued))
	}
	if deadLettered > 0 {
		m.jobsRecovered.WithLabelValues("dead").Add(float64(deadLettered))
	}
}

// RegisterExpiredLeases registers a gauge reporting how many claims have
// lapsed without yet being recovered, sampled at scrape time via staleFunc
// (typically a queue/sqlite.Queue's ExpiredLeases method, with its error
// folded away).
//
// Unlike the recovery counter, which only moves when a reaper runs, this
// gauge stays elevated for as long as orphans are going unreclaimed --
// which is what a stuck or crashed reaper looks like from the outside.
func (m *Metrics) RegisterExpiredLeases(queueName string, staleFunc func() int) error {
	if m == nil {
		return nil
	}
	g := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   namespace,
		Name:        "expired_leases",
		Help:        "Number of claimed jobs whose lease has expired and that have not yet been recovered.",
		ConstLabels: prometheus.Labels{"queue": queueName},
	}, func() float64 { return float64(staleFunc()) })
	return m.reg.Register(g)
}
