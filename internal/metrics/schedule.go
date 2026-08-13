package metrics

// IncScheduleFired increments the count of schedules successfully fired
// (job enqueued and next run time advanced).
func (m *Metrics) IncScheduleFired() {
	if m == nil {
		return
	}
	m.scheduleFires.Inc()
}

// IncScheduleError increments the count of errors encountered while firing
// a due schedule (advancing its cron expression, enqueueing its job, or
// persisting the fire).
func (m *Metrics) IncScheduleError() {
	if m == nil {
		return
	}
	m.scheduleErrors.Inc()
}
