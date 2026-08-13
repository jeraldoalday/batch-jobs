package metrics

import "time"

// ObserveTick records d, the time a single scheduler poll took to query and
// fire all due schedules, in the tick duration histogram.
func (m *Metrics) ObserveTick(d time.Duration) {
	if m == nil {
		return
	}
	m.tickDuration.Observe(d.Seconds())
}
