package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveRecoverySplitsByOutcome(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.ObserveRecovery(3, 1)
	m.ObserveRecovery(2, 0)

	if got, want := testutil.ToFloat64(m.jobsRecovered.WithLabelValues("requeued")), 5.0; got != want {
		t.Errorf("requeued recoveries = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(m.jobsRecovered.WithLabelValues("dead")), 1.0; got != want {
		t.Errorf("dead-lettered recoveries = %v, want %v", got, want)
	}
}

func TestObserveRecoveryIgnoresEmptyPasses(t *testing.T) {
	m := New(prometheus.NewRegistry())

	// The common case: the reaper runs and finds nothing. It should not
	// create label series for outcomes that never happened, since an
	// always-zero series is noise in every dashboard that lists them.
	m.ObserveRecovery(0, 0)

	if got := testutil.CollectAndCount(m.jobsRecovered); got != 0 {
		t.Fatalf("jobsRecovered has %d series after an empty pass, want 0", got)
	}
}

func TestObserveRecoveryOnNilMetrics(t *testing.T) {
	var m *Metrics
	m.ObserveRecovery(1, 1)
	if err := m.RegisterExpiredLeases("emails", func() int { return 1 }); err != nil {
		t.Fatalf("RegisterExpiredLeases on nil *Metrics = %v, want nil", err)
	}
}

func TestRegisterExpiredLeasesReportsAtScrapeTime(t *testing.T) {
	m := New(prometheus.NewRegistry())

	stale := 0
	if err := m.RegisterExpiredLeases("emails", func() int { return stale }); err != nil {
		t.Fatalf("RegisterExpiredLeases: %v", err)
	}

	if body := scrape(t, m); !strings.Contains(body, `batchjobs_expired_leases{queue="emails"} 0`) {
		t.Errorf("scrape output missing initial expired-lease gauge; got:\n%s", body)
	}

	stale = 4
	if body := scrape(t, m); !strings.Contains(body, `batchjobs_expired_leases{queue="emails"} 4`) {
		t.Errorf("gauge did not follow the queue's state; got:\n%s", body)
	}
}

func TestRegisterExpiredLeasesRejectsDuplicateQueue(t *testing.T) {
	m := New(prometheus.NewRegistry())

	if err := m.RegisterExpiredLeases("emails", func() int { return 0 }); err != nil {
		t.Fatalf("first RegisterExpiredLeases: %v", err)
	}
	if err := m.RegisterExpiredLeases("emails", func() int { return 0 }); err == nil {
		t.Fatal("second RegisterExpiredLeases for the same queue: want an error")
	}
}
