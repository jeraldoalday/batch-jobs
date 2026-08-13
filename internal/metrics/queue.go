package metrics

import "github.com/prometheus/client_golang/prometheus"

// RegisterQueueDepth registers a gauge that reports queueName's current
// backlog size on every scrape, by calling lenFunc (typically a
// queue.Queue's Len method). Unlike the counters and histograms recorded
// via ObserveJob and friends, which are pushed as events happen, this is a
// pull-based gauge: it always reflects the queue's state at scrape time,
// with no background polling goroutine required.
//
// RegisterQueueDepth is a no-op returning nil for a nil *Metrics, and
// returns an error if a queue depth gauge for queueName has already been
// registered.
func (m *Metrics) RegisterQueueDepth(queueName string, lenFunc func() int) error {
	if m == nil {
		return nil
	}
	g := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   namespace,
		Name:        "queue_depth",
		Help:        "Number of jobs currently waiting (pending or retrying) in the queue.",
		ConstLabels: prometheus.Labels{"queue": queueName},
	}, func() float64 { return float64(lenFunc()) })
	return m.reg.Register(g)
}
