package queue

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestRetentionTTLPerStatus(t *testing.T) {
	r := Retention{
		Succeeded: time.Hour,
		Failed:    2 * time.Hour,
		Dead:      3 * time.Hour,
		Cancelled: 4 * time.Hour,
	}

	tests := []struct {
		status job.Status
		want   time.Duration
	}{
		{job.StatusSucceeded, time.Hour},
		{job.StatusFailed, 2 * time.Hour},
		{job.StatusDead, 3 * time.Hour},
		{job.StatusCancelled, 4 * time.Hour},
	}
	for _, tc := range tests {
		if got := r.TTL(tc.status); got != tc.want {
			t.Errorf("TTL(%s) = %s, want %s", tc.status, got, tc.want)
		}
	}
}

// A job that has not finished is not history. Whatever the policy says,
// no in-flight status may be pruned, so its TTL is always zero.
func TestRetentionTTLNonTerminalIsAlwaysForever(t *testing.T) {
	r := Retention{Succeeded: time.Hour, Failed: time.Hour, Dead: time.Hour, Cancelled: time.Hour}

	for _, status := range []job.Status{job.StatusPending, job.StatusRunning, job.StatusRetrying} {
		if got := r.TTL(status); got != 0 {
			t.Errorf("TTL(%s) = %s, want 0: non-terminal jobs are never prunable", status, got)
		}
	}
}

func TestRetentionZeroValueKeepsEverything(t *testing.T) {
	var r Retention

	if r.Enabled() {
		t.Error("zero Retention reports Enabled; the default must delete nothing")
	}
	if got := r.Cutoffs(time.Now()); len(got) != 0 {
		t.Errorf("zero Retention produced %d cutoffs, want none", len(got))
	}
	if got := r.String(); got != "keep everything" {
		t.Errorf("String() = %q, want %q", got, "keep everything")
	}
}

func TestRetentionEnabledOnAnySingleStatus(t *testing.T) {
	for _, status := range TerminalStatuses {
		var r Retention
		switch status {
		case job.StatusSucceeded:
			r.Succeeded = time.Hour
		case job.StatusFailed:
			r.Failed = time.Hour
		case job.StatusDead:
			r.Dead = time.Hour
		case job.StatusCancelled:
			r.Cancelled = time.Hour
		}
		if !r.Enabled() {
			t.Errorf("Retention with only %s set reports disabled", status)
		}
	}
}

func TestRetentionValidateRejectsNegative(t *testing.T) {
	tests := []struct {
		name string
		r    Retention
	}{
		{"succeeded", Retention{Succeeded: -time.Hour}},
		{"failed", Retention{Failed: -time.Hour}},
		{"dead", Retention{Dead: -time.Hour}},
		{"cancelled", Retention{Cancelled: -time.Hour}},
		{"batch size", Retention{Succeeded: time.Hour, BatchSize: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.r.Validate(); err == nil {
				t.Fatal("Validate accepted a negative duration; it must be rejected rather than guessed at")
			}
		})
	}
}

func TestRetentionValidateAcceptsZeroAndPositive(t *testing.T) {
	for _, r := range []Retention{{}, DefaultRetention(), {Dead: time.Hour, BatchSize: 10}} {
		if err := r.Validate(); err != nil {
			t.Errorf("Validate(%v) = %v, want nil", r, err)
		}
	}
}

func TestRetentionBatchDefaults(t *testing.T) {
	if got := (Retention{}).Batch(); got != DefaultPruneBatch {
		t.Errorf("Batch() with no size = %d, want %d", got, DefaultPruneBatch)
	}
	if got := (Retention{BatchSize: 7}).Batch(); got != 7 {
		t.Errorf("Batch() = %d, want 7", got)
	}
}

// Cutoffs must omit statuses kept forever, and order the rest so the
// bulkiest, least interesting status is cleared first.
func TestRetentionCutoffsOrderAndOmission(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	r := Retention{Succeeded: time.Hour, Cancelled: 2 * time.Hour, Failed: 3 * time.Hour}

	cutoffs := r.Cutoffs(now)
	want := []Cutoff{
		{job.StatusSucceeded, now.Add(-time.Hour)},
		{job.StatusCancelled, now.Add(-2 * time.Hour)},
		{job.StatusFailed, now.Add(-3 * time.Hour)},
	}
	if len(cutoffs) != len(want) {
		t.Fatalf("got %d cutoffs, want %d (dead is kept forever and must be omitted)", len(cutoffs), len(want))
	}
	for i, w := range want {
		if cutoffs[i].Status != w.Status {
			t.Errorf("cutoff %d is %s, want %s", i, cutoffs[i].Status, w.Status)
		}
		if !cutoffs[i].Before.Equal(w.Before) {
			t.Errorf("cutoff for %s = %s, want %s", w.Status, cutoffs[i].Before, w.Before)
		}
	}
}

// The cutoffs a pass works from are computed against a fixed clock, so
// two calls a moment apart differ by exactly that moment rather than by
// anything the policy does.
func TestRetentionCutoffsAreRelativeToTheGivenTime(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := Retention{Dead: 48 * time.Hour}

	got := r.Cutoffs(base)
	if len(got) != 1 {
		t.Fatalf("got %d cutoffs, want 1", len(got))
	}
	if want := base.Add(-48 * time.Hour); !got[0].Before.Equal(want) {
		t.Errorf("cutoff = %s, want %s", got[0].Before, want)
	}
	if got[0].Before.Location() != time.UTC {
		t.Errorf("cutoff location = %s, want UTC", got[0].Before.Location())
	}
}

func TestDefaultRetentionKeepsDeadJobsForever(t *testing.T) {
	r := DefaultRetention()

	if r.Dead != 0 {
		t.Errorf("DefaultRetention().Dead = %s, want 0: a dead-letter queue is a work list, not a log", r.Dead)
	}
	if !r.Enabled() {
		t.Error("DefaultRetention reports disabled")
	}
	if r.Succeeded >= r.Failed {
		t.Errorf("successes (%s) are kept at least as long as failures (%s); failures carry more evidence", r.Succeeded, r.Failed)
	}
}

func TestRetentionString(t *testing.T) {
	r := Retention{Succeeded: time.Hour, Dead: 30 * time.Minute}

	got := r.String()
	if !strings.Contains(got, "succeeded=1h0m0s") || !strings.Contains(got, "dead=30m0s") {
		t.Errorf("String() = %q, want it to name both configured statuses", got)
	}
	if strings.Contains(got, "failed") {
		t.Errorf("String() = %q, want statuses kept forever omitted", got)
	}
}

func TestPrunedAccounting(t *testing.T) {
	var p Pruned
	p.Add(job.StatusSucceeded, 3)
	p.Add(job.StatusSucceeded, 2)
	p.Add(job.StatusFailed, 1)
	p.Add(job.StatusDead, 4)
	p.Add(job.StatusCancelled, 5)

	if got := p.Total(); got != 15 {
		t.Errorf("Total() = %d, want 15", got)
	}
	for status, want := range map[job.Status]int{
		job.StatusSucceeded: 5,
		job.StatusFailed:    1,
		job.StatusDead:      4,
		job.StatusCancelled: 5,
	} {
		if got := p.Get(status); got != want {
			t.Errorf("Get(%s) = %d, want %d", status, got, want)
		}
	}
}

// Adding against a status that cannot be pruned must not quietly land in
// some other field, and must not appear in the total.
func TestPrunedAddIgnoresNonTerminal(t *testing.T) {
	var p Pruned
	p.Add(job.StatusRunning, 9)
	p.Add(job.StatusPending, 9)

	if got := p.Total(); got != 0 {
		t.Errorf("Total() = %d after adding non-terminal statuses, want 0", got)
	}
	if got := p.Get(job.StatusRunning); got != 0 {
		t.Errorf("Get(running) = %d, want 0", got)
	}
}

func TestPrunedString(t *testing.T) {
	if got := (Pruned{}).String(); got != "nothing pruned" {
		t.Errorf("String() = %q, want %q", got, "nothing pruned")
	}

	got := Pruned{Succeeded: 2, Dead: 1}.String()
	if !strings.Contains(got, "succeeded=2") || !strings.Contains(got, "dead=1") {
		t.Errorf("String() = %q, want it to name both non-zero counts", got)
	}
	if strings.Contains(got, "failed") {
		t.Errorf("String() = %q, want zero counts omitted", got)
	}
}

func TestTerminalStatusesAreAllTerminal(t *testing.T) {
	for _, status := range TerminalStatuses {
		if !status.Terminal() {
			t.Errorf("%s is in TerminalStatuses but Status.Terminal reports false", status)
		}
	}

	// The reverse direction: a status added to the job package later
	// must be added here too, or a policy silently stops covering it.
	all := []job.Status{
		job.StatusPending, job.StatusRunning, job.StatusSucceeded, job.StatusFailed,
		job.StatusRetrying, job.StatusDead, job.StatusCancelled,
	}
	for _, status := range all {
		if !status.Terminal() {
			continue
		}
		found := false
		for _, listed := range TerminalStatuses {
			if listed == status {
				found = true
			}
		}
		if !found {
			t.Errorf("%s is terminal but missing from TerminalStatuses", status)
		}
	}
}

func TestErrNoRetentionIsDistinct(t *testing.T) {
	if errors.Is(ErrNoRetention, ErrNotFound) || errors.Is(ErrNoRetention, ErrClosed) {
		t.Error("ErrNoRetention matches another sentinel")
	}
}
