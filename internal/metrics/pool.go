package metrics

// SetPoolWorkers records how many worker goroutines the named pool is
// currently running. It is pushed by the pool whenever its size changes
// rather than pulled at scrape time, since the pool's roster is guarded by
// a mutex a scrape has no business taking.
func (m *Metrics) SetPoolWorkers(pool string, n int) {
	if m == nil {
		return
	}
	m.poolWorkers.WithLabelValues(pool).Set(float64(n))
}

// ObservePoolScale records one applied autoscaling decision, labeled by
// direction, and updates the worker-count gauge to match.
//
// The counter answers a question the gauge cannot: a pool that sits at
// eight workers all day looks identical to one that oscillates between six
// and ten every minute if all you have is the current size. A high scale
// event rate is the signal that the policy's hysteresis is too loose for
// the workload's shape.
func (m *Metrics) ObservePoolScale(pool string, from, to int) {
	if m == nil {
		return
	}

	direction := "up"
	if to < from {
		direction = "down"
	}
	m.poolScaleEvents.WithLabelValues(pool, direction).Inc()
	m.poolWorkers.WithLabelValues(pool).Set(float64(to))
}
