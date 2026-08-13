package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestObserveJob_IncrementsCounter(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObserveJob("emails", job.StatusSucceeded, 250*time.Millisecond)
	m.ObserveJob("emails", job.StatusSucceeded, 100*time.Millisecond)
	m.ObserveJob("emails", job.StatusDead, time.Second)

	if got, want := testutil.ToFloat64(m.jobsProcessed.WithLabelValues("emails", "succeeded")), 2.0; got != want {
		t.Errorf("succeeded counter = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.jobsProcessed.WithLabelValues("emails", "dead")), 1.0; got != want {
		t.Errorf("dead counter = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.jobsProcessed.WithLabelValues("reports", "succeeded")), 0.0; got != want {
		t.Errorf("unrelated queue counter = %v, want %v", got, want)
	}
}

func TestObserveJob_RecordsDuration(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObserveJob("emails", job.StatusSucceeded, 250*time.Millisecond)
	m.ObserveJob("emails", job.StatusSucceeded, 750*time.Millisecond)

	h, ok := m.jobDuration.WithLabelValues("emails", "succeeded").(prometheus.Metric)
	if !ok {
		t.Fatalf("histogram observer does not implement prometheus.Metric")
	}
	var pb dto.Metric
	if err := h.Write(&pb); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got, want := pb.GetHistogram().GetSampleCount(), uint64(2); got != want {
		t.Errorf("sample count = %v, want %v", got, want)
	}
	if got, want := pb.GetHistogram().GetSampleSum(), 1.0; got != want {
		t.Errorf("sample sum = %v, want %v", got, want)
	}
}

func TestScheduleCounters(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.IncScheduleFired()
	m.IncScheduleFired()
	m.IncScheduleError()

	if got, want := testutil.ToFloat64(m.scheduleFires), 2.0; got != want {
		t.Errorf("scheduleFires = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.scheduleErrors), 1.0; got != want {
		t.Errorf("scheduleErrors = %v, want %v", got, want)
	}
}

func TestHandler_ServesPrometheusFormat(t *testing.T) {
	m := New(prometheus.NewRegistry())
	m.ObserveJob("emails", job.StatusSucceeded, time.Millisecond)
	m.IncScheduleFired()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`batchjobs_jobs_processed_total{queue="emails",status="succeeded"} 1`,
		`batchjobs_schedule_fires_total 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("response body missing %q; got:\n%s", want, body)
		}
	}
}

func TestNilMetrics_IsNoop(t *testing.T) {
	var m *Metrics

	m.ObserveJob("emails", job.StatusSucceeded, time.Second)
	m.IncScheduleFired()
	m.IncScheduleError()

	if h := m.Handler(); h != nil {
		t.Errorf("Handler() on nil Metrics = %v, want nil", h)
	}
}
