package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestObservePrunedSplitsByStatus(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObservePruned(job.StatusSucceeded, 10)
	m.ObservePruned(job.StatusSucceeded, 5)
	m.ObservePruned(job.StatusDead, 1)

	if got, want := testutil.ToFloat64(m.jobsPruned.WithLabelValues("succeeded")), 15.0; got != want {
		t.Errorf("succeeded prunes = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.jobsPruned.WithLabelValues("dead")), 1.0; got != want {
		t.Errorf("dead prunes = %v, want %v", got, want)
	}
}

// Most passes delete nothing, and most policies never touch some
// statuses. Neither should create an always-zero series.
func TestObservePrunedIgnoresEmptyPasses(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObservePruned(job.StatusFailed, 0)

	if got := testutil.CollectAndCount(m.jobsPruned); got != 0 {
		t.Fatalf("jobsPruned has %d series after an empty pass, want 0", got)
	}
}

func TestObservePrunedOnNilMetrics(t *testing.T) {
	var m *Metrics
	m.ObservePruned(job.StatusSucceeded, 5)

	if err := m.RegisterRetainedJobs("emails", func() int { return 1 }); err != nil {
		t.Errorf("RegisterRetainedJobs on nil Metrics = %v, want nil", err)
	}
	if err := m.RegisterOldestRetainedAge("emails", func() float64 { return 1 }); err != nil {
		t.Errorf("RegisterOldestRetainedAge on nil Metrics = %v, want nil", err)
	}
}

func TestRegisterRetainedJobsReportsAtScrapeTime(t *testing.T) {
	m := New(prometheus.NewRegistry())

	retained := 0
	if err := m.RegisterRetainedJobs("emails", func() int { return retained }); err != nil {
		t.Fatalf("RegisterRetainedJobs: %v", err)
	}

	if body := scrape(t, m); !strings.Contains(body, `batchjobs_retained_jobs{queue="emails"} 0`) {
		t.Errorf("scrape output missing initial retained gauge; got:\n%s", body)
	}

	retained = 1200
	if body := scrape(t, m); !strings.Contains(body, `batchjobs_retained_jobs{queue="emails"} 1200`) {
		t.Errorf("gauge did not follow the queue's history; got:\n%s", body)
	}
}

func TestRegisterOldestRetainedAgeReportsSeconds(t *testing.T) {
	m := New(prometheus.NewRegistry())

	age := 90 * time.Minute
	if err := m.RegisterOldestRetainedAge("emails", func() float64 { return age.Seconds() }); err != nil {
		t.Fatalf("RegisterOldestRetainedAge: %v", err)
	}

	if body := scrape(t, m); !strings.Contains(body, `batchjobs_oldest_retained_job_age_seconds{queue="emails"} 5400`) {
		t.Errorf("scrape output missing the oldest-age gauge; got:\n%s", body)
	}
}

func TestRegisterRetentionGaugesRejectDuplicateQueue(t *testing.T) {
	m := New(prometheus.NewRegistry())

	if err := m.RegisterRetainedJobs("emails", func() int { return 0 }); err != nil {
		t.Fatalf("first RegisterRetainedJobs: %v", err)
	}
	if err := m.RegisterRetainedJobs("emails", func() int { return 0 }); err == nil {
		t.Error("second RegisterRetainedJobs for the same queue: want an error")
	}

	if err := m.RegisterOldestRetainedAge("emails", func() float64 { return 0 }); err != nil {
		t.Fatalf("first RegisterOldestRetainedAge: %v", err)
	}
	if err := m.RegisterOldestRetainedAge("emails", func() float64 { return 0 }); err == nil {
		t.Error("second RegisterOldestRetainedAge for the same queue: want an error")
	}
}
