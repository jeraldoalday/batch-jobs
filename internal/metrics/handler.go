package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler returns an http.Handler that serves m's collectors in the
// Prometheus text exposure format, suitable for mounting at a scrape path
// such as "/metrics". It returns nil for a nil *Metrics, since there is
// nothing to serve.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return nil
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
