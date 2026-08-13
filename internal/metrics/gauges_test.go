package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// scrape renders m's collectors in Prometheus text format via its scrape
// Handler, the same path a real /metrics endpoint would take.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestJobsInFlightGauge(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.IncJobsInFlight("emails")
	m.IncJobsInFlight("emails")
	m.IncJobsInFlight("reports")
	m.DecJobsInFlight("emails")

	if got, want := testutil.ToFloat64(m.jobsInFlight.WithLabelValues("emails")), 1.0; got != want {
		t.Errorf("emails in-flight = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.jobsInFlight.WithLabelValues("reports")), 1.0; got != want {
		t.Errorf("reports in-flight = %v, want %v", got, want)
	}
}

func TestObserveTick(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObserveTick(100 * time.Millisecond)
	m.ObserveTick(200 * time.Millisecond)

	if got, want := testutil.CollectAndCount(m.tickDuration), 1; got != want {
		t.Fatalf("CollectAndCount = %d, want %d", got, want)
	}
}

func TestRegisterQueueDepth(t *testing.T) {
	m := New(prometheus.NewRegistry())

	depth := 3
	if err := m.RegisterQueueDepth("emails", func() int { return depth }); err != nil {
		t.Fatalf("RegisterQueueDepth: %v", err)
	}

	body := scrape(t, m)
	if !strings.Contains(body, `batchjobs_queue_depth{queue="emails"} 3`) {
		t.Errorf("scrape output missing initial depth; got:\n%s", body)
	}

	// The gauge is pull-based: it must reflect depth's current value on
	// every scrape, not just the value at registration time.
	depth = 7
	body = scrape(t, m)
	if !strings.Contains(body, `batchjobs_queue_depth{queue="emails"} 7`) {
		t.Errorf("scrape output did not reflect updated depth; got:\n%s", body)
	}
}

func TestRegisterQueueDepth_DuplicateNameErrors(t *testing.T) {
	m := New(prometheus.NewRegistry())

	if err := m.RegisterQueueDepth("emails", func() int { return 0 }); err != nil {
		t.Fatalf("first RegisterQueueDepth: %v", err)
	}
	if err := m.RegisterQueueDepth("emails", func() int { return 0 }); err == nil {
		t.Fatal("expected second RegisterQueueDepth for the same queue to fail")
	}
}

func TestNilMetrics_GaugesAndTickAreNoop(t *testing.T) {
	var m *Metrics

	m.IncJobsInFlight("emails")
	m.DecJobsInFlight("emails")
	m.ObserveTick(time.Second)

	if err := m.RegisterQueueDepth("emails", func() int { return 0 }); err != nil {
		t.Errorf("RegisterQueueDepth on nil Metrics = %v, want nil", err)
	}
}
