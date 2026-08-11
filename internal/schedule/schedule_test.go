package schedule

import (
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestNewAssignsIDAndNextRunAt(t *testing.T) {
	s, err := New("emails", "* * * * *", []byte("payload"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.ID == "" {
		t.Error("ID is empty, want a generated UUID")
	}
	if s.Queue != "emails" {
		t.Errorf("Queue = %q, want %q", s.Queue, "emails")
	}
	if string(s.Payload) != "payload" {
		t.Errorf("Payload = %q, want %q", s.Payload, "payload")
	}
	if s.NextRunAt.IsZero() {
		t.Error("NextRunAt is zero, want a computed next fire time")
	}
	if !s.Enabled {
		t.Error("Enabled = false, want true for a new schedule")
	}
	if !s.LastRunAt.IsZero() {
		t.Errorf("LastRunAt = %v, want zero for a schedule that has never fired", s.LastRunAt)
	}
}

func TestNewDefaultsPriorityAndRetries(t *testing.T) {
	s, err := New("q", "* * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Priority != job.PriorityNormal {
		t.Errorf("Priority = %v, want %v", s.Priority, job.PriorityNormal)
	}
	if s.MaxRetries != 0 {
		t.Errorf("MaxRetries = %d, want 0", s.MaxRetries)
	}
}

func TestNewAppliesOptions(t *testing.T) {
	s, err := New("q", "* * * * *", nil,
		WithPriority(job.PriorityHigh),
		WithMaxRetries(5),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Priority != job.PriorityHigh {
		t.Errorf("Priority = %v, want %v", s.Priority, job.PriorityHigh)
	}
	if s.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", s.MaxRetries)
	}
}

func TestNewRejectsInvalidCron(t *testing.T) {
	if _, err := New("q", "not a cron expr", nil); err == nil {
		t.Fatal("New: want error for invalid cron expression")
	}
}

func TestNewRejectsCronThatNeverFires(t *testing.T) {
	if _, err := New("q", "0 0 30 2 *", nil); err == nil {
		t.Fatal("New: want error for cron expression that never fires")
	}
}

func TestNewJobPropagatesFields(t *testing.T) {
	s, err := New("reports", "* * * * *", []byte("payload"),
		WithPriority(job.PriorityHigh),
		WithMaxRetries(3),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	j := s.NewJob()
	if j.Queue != s.Queue {
		t.Errorf("Queue = %q, want %q", j.Queue, s.Queue)
	}
	if string(j.Payload) != string(s.Payload) {
		t.Errorf("Payload = %q, want %q", j.Payload, s.Payload)
	}
	if j.Priority != s.Priority {
		t.Errorf("Priority = %v, want %v", j.Priority, s.Priority)
	}
	if j.MaxRetries != s.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", j.MaxRetries, s.MaxRetries)
	}
	if j.Status != job.StatusPending {
		t.Errorf("Status = %q, want %q", j.Status, job.StatusPending)
	}
}

func TestNewJobProducesDistinctIDs(t *testing.T) {
	s, err := New("q", "* * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first := s.NewJob()
	second := s.NewJob()
	if first.ID == second.ID {
		t.Error("NewJob returned two jobs with the same ID; each firing should produce a distinct job")
	}
}

func TestAdvanceComputesNextFireTime(t *testing.T) {
	s, err := New("q", "0 * * * *", nil) // top of every hour
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	from := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	next, err := s.Advance(from)
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	want := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("Advance(%v) = %v, want %v", from, next, want)
	}
}

func TestAdvanceErrorsOnCorruptedCronExpr(t *testing.T) {
	s, err := New("q", "* * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Simulate a Store row loaded with a corrupted expression.
	s.CronExpr = "garbage"
	if _, err := s.Advance(time.Now()); err == nil {
		t.Fatal("Advance: want error for unparsable CronExpr")
	}
}
