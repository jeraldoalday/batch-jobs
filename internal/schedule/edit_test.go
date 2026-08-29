package schedule

import (
	"testing"
	"time"
)

// noonUTC is a fixed instant every test in this file measures from, so the
// expected fire times can be written out literally rather than derived
// from whatever time the suite happens to run at.
var noonUTC = time.Date(2026, time.March, 14, 12, 0, 0, 0, time.UTC)

func TestRescheduleReplacesExpressionAndNextRun(t *testing.T) {
	s, err := New("reports", "0 * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	before := s.CreatedAt

	if err := s.Reschedule("30 3 * * *", noonUTC); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	if s.CronExpr != "30 3 * * *" {
		t.Errorf("CronExpr = %q, want %q", s.CronExpr, "30 3 * * *")
	}
	want := time.Date(2026, time.March, 15, 3, 30, 0, 0, time.UTC)
	if !s.NextRunAt.Equal(want) {
		t.Errorf("NextRunAt = %v, want %v", s.NextRunAt, want)
	}
	if !s.UpdatedAt.Equal(noonUTC) {
		t.Errorf("UpdatedAt = %v, want %v", s.UpdatedAt, noonUTC)
	}
	if !s.CreatedAt.Equal(before) {
		t.Errorf("CreatedAt = %v, want it left at %v", s.CreatedAt, before)
	}
}

func TestRescheduleRejectsBadExpressionWithoutMutating(t *testing.T) {
	s, err := New("reports", "0 * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	wantExpr, wantNext := s.CronExpr, s.NextRunAt

	if err := s.Reschedule("not a cron expression", noonUTC); err == nil {
		t.Fatal("Reschedule with an unparsable expression returned nil error")
	}

	// A rejected edit must leave the schedule exactly as it was: a store
	// that persists the struct afterwards would otherwise write a half-
	// applied change.
	if s.CronExpr != wantExpr {
		t.Errorf("CronExpr = %q, want it left at %q", s.CronExpr, wantExpr)
	}
	if !s.NextRunAt.Equal(wantNext) {
		t.Errorf("NextRunAt = %v, want it left at %v", s.NextRunAt, wantNext)
	}
}

func TestRescheduleRejectsExpressionThatNeverFires(t *testing.T) {
	s, err := New("reports", "0 * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// February 30th exists in no year, so the expression parses but never
	// comes due.
	if err := s.Reschedule("0 0 30 2 *", noonUTC); err == nil {
		t.Fatal("Reschedule with a never-firing expression returned nil error")
	}
}

func TestUpcomingListsNextFireTimes(t *testing.T) {
	s, err := New("reports", "0 * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.NextRunAt = time.Date(2026, time.March, 14, 13, 0, 0, 0, time.UTC)

	got, err := s.Upcoming(3)
	if err != nil {
		t.Fatalf("Upcoming: %v", err)
	}

	want := []time.Time{
		time.Date(2026, time.March, 14, 13, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 14, 14, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 14, 15, 0, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("Upcoming(3) returned %d times, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("Upcoming()[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestUpcomingStartsAtNextRunEvenWhenOverdue(t *testing.T) {
	s, err := New("reports", "0 * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A schedule the scheduler has not caught up with yet.
	overdue := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Hour)
	s.NextRunAt = overdue

	got, err := s.Upcoming(2)
	if err != nil {
		t.Fatalf("Upcoming: %v", err)
	}
	if len(got) == 0 || !got[0].Equal(overdue) {
		t.Fatalf("Upcoming()[0] = %v, want the overdue fire time %v", got, overdue)
	}
}

func TestUpcomingZeroOrNegativeReturnsNothing(t *testing.T) {
	s, err := New("reports", "0 * * * *", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for _, n := range []int{0, -1} {
		got, err := s.Upcoming(n)
		if err != nil {
			t.Fatalf("Upcoming(%d): %v", n, err)
		}
		if len(got) != 0 {
			t.Errorf("Upcoming(%d) = %v, want no times", n, got)
		}
	}
}

func TestUpcomingRejectsUnparsableExpression(t *testing.T) {
	s := &Schedule{CronExpr: "nonsense", NextRunAt: noonUTC}
	if _, err := s.Upcoming(3); err == nil {
		t.Fatal("Upcoming on an unparsable expression returned nil error")
	}
}
