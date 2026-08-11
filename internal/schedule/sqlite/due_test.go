package sqlite

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

func TestDueExcludesFutureAndDisabled(t *testing.T) {
	s := openTestStore(t)
	now := time.Now().UTC()

	overdue := mustNewSchedule(t)
	overdue.NextRunAt = now.Add(-time.Minute)
	if err := s.Create(overdue); err != nil {
		t.Fatalf("Create overdue: %v", err)
	}

	future := mustNewSchedule(t)
	future.NextRunAt = now.Add(time.Hour)
	if err := s.Create(future); err != nil {
		t.Fatalf("Create future: %v", err)
	}

	disabled := mustNewSchedule(t)
	disabled.NextRunAt = now.Add(-time.Minute)
	disabled.Enabled = false
	if err := s.Create(disabled); err != nil {
		t.Fatalf("Create disabled: %v", err)
	}

	due, err := s.Due(now)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("Due returned %d schedules, want 1", len(due))
	}
	if due[0].ID != overdue.ID {
		t.Errorf("Due()[0].ID = %q, want %q", due[0].ID, overdue.ID)
	}
}

func TestDueOrdersSoonestFirst(t *testing.T) {
	s := openTestStore(t)
	now := time.Now().UTC()

	later := mustNewSchedule(t)
	later.NextRunAt = now.Add(-time.Minute)
	if err := s.Create(later); err != nil {
		t.Fatalf("Create later: %v", err)
	}

	soonest := mustNewSchedule(t)
	soonest.NextRunAt = now.Add(-time.Hour)
	if err := s.Create(soonest); err != nil {
		t.Fatalf("Create soonest: %v", err)
	}

	due, err := s.Due(now)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("Due returned %d schedules, want 2", len(due))
	}
	if due[0].ID != soonest.ID {
		t.Errorf("Due()[0].ID = %q, want %q (most overdue first)", due[0].ID, soonest.ID)
	}
	if due[1].ID != later.ID {
		t.Errorf("Due()[1].ID = %q, want %q", due[1].ID, later.ID)
	}
}

func TestMarkFiredRemovesFromDueUntilNextFireTime(t *testing.T) {
	s := openTestStore(t)
	now := time.Now().UTC()

	sch := mustNewSchedule(t)
	sch.NextRunAt = now.Add(-time.Minute)
	if err := s.Create(sch); err != nil {
		t.Fatalf("Create: %v", err)
	}

	next := now.Add(time.Hour)
	if err := s.MarkFired(sch.ID, now, next); err != nil {
		t.Fatalf("MarkFired: %v", err)
	}

	due, err := s.Due(now)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("Due returned %d schedules, want 0 (schedule was just advanced past now)", len(due))
	}

	got, err := s.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.LastRunAt.Equal(now) {
		t.Errorf("LastRunAt = %v, want %v", got.LastRunAt, now)
	}
	if !got.NextRunAt.Equal(next) {
		t.Errorf("NextRunAt = %v, want %v", got.NextRunAt, next)
	}
}

func TestMarkFiredMissingReturnsErrNotFound(t *testing.T) {
	s := openTestStore(t)
	now := time.Now().UTC()
	if err := s.MarkFired("does-not-exist", now, now.Add(time.Minute)); !errors.Is(err, schedule.ErrNotFound) {
		t.Fatalf("MarkFired: err = %v, want ErrNotFound", err)
	}
}

// TestConcurrentDueAndMarkFired simulates several scheduler goroutines
// racing to claim and fire distinct due schedules against a shared store,
// the access pattern internal/scheduler's loop produces when run with
// concurrency. It exists to be run with -race.
func TestConcurrentDueAndMarkFired(t *testing.T) {
	s := openTestStore(t)
	now := time.Now().UTC()

	const n = 20
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		sch := mustNewSchedule(t)
		sch.NextRunAt = now.Add(-time.Minute)
		sch.Queue = fmt.Sprintf("q%d", i)
		if err := s.Create(sch); err != nil {
			t.Fatalf("Create: %v", err)
		}
		ids[i] = sch.ID
	}

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, err := s.Due(time.Now().UTC()); err != nil {
				errs <- fmt.Errorf("Due: %w", err)
				return
			}
			if err := s.MarkFired(id, time.Now().UTC(), time.Now().UTC().Add(time.Hour)); err != nil {
				errs <- fmt.Errorf("MarkFired(%s): %w", id, err)
			}
		}(id)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	due, err := s.Due(time.Now().UTC())
	if err != nil {
		t.Fatalf("final Due: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("Due returned %d schedules after all were fired, want 0", len(due))
	}
}
