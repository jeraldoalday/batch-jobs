package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSetPoolWorkers(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.SetPoolWorkers("default", 4)
	m.SetPoolWorkers("reports", 2)
	m.SetPoolWorkers("default", 7)

	if got, want := testutil.ToFloat64(m.poolWorkers.WithLabelValues("default")), 7.0; got != want {
		t.Errorf("default pool workers = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.poolWorkers.WithLabelValues("reports")), 2.0; got != want {
		t.Errorf("reports pool workers = %v, want %v", got, want)
	}
}

func TestObservePoolScaleCountsDirection(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObservePoolScale("default", 2, 5)
	m.ObservePoolScale("default", 5, 9)
	m.ObservePoolScale("default", 9, 8)

	if got, want := testutil.ToFloat64(m.poolScaleEvents.WithLabelValues("default", "up")), 2.0; got != want {
		t.Errorf("scale-up events = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.poolScaleEvents.WithLabelValues("default", "down")), 1.0; got != want {
		t.Errorf("scale-down events = %v, want %v", got, want)
	}
}

// The gauge must follow the decisions the counter records, so a dashboard
// never shows a pool size that contradicts its own scaling history.
func TestObservePoolScaleUpdatesGauge(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObservePoolScale("default", 2, 5)

	if got, want := testutil.ToFloat64(m.poolWorkers.WithLabelValues("default")), 5.0; got != want {
		t.Errorf("pool workers = %v, want %v", got, want)
	}
}

func TestPoolMetricsAppearInScrape(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.SetPoolWorkers("default", 3)
	m.ObservePoolScale("default", 3, 1)

	body := scrape(t, m)
	for _, want := range []string{
		`batchjobs_pool_workers{pool="default"} 1`,
		`batchjobs_pool_scale_events_total{direction="down",pool="default"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape output missing %q\n%s", want, body)
		}
	}
}

// Every method must stay usable on a nil *Metrics, since that is how a
// Pool with instrumentation disabled calls them.
func TestNilPoolMetricsAreNoOps(t *testing.T) {
	var m *Metrics
	m.SetPoolWorkers("default", 3)
	m.ObservePoolScale("default", 3, 4)
}
