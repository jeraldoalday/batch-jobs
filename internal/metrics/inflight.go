package metrics

// IncJobsInFlight increments the number of jobs currently executing in a
// handler for queue. Callers should pair every increment with a matching
// DecJobsInFlight once the handler returns.
func (m *Metrics) IncJobsInFlight(queue string) {
	if m == nil {
		return
	}
	m.jobsInFlight.WithLabelValues(queue).Inc()
}

// DecJobsInFlight decrements the number of jobs currently executing in a
// handler for queue.
func (m *Metrics) DecJobsInFlight(queue string) {
	if m == nil {
		return
	}
	m.jobsInFlight.WithLabelValues(queue).Dec()
}
