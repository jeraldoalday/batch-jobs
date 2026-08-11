package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// openTestStore returns a Store backed by a private, in-memory SQLite
// database that is closed automatically at the end of the test.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustNewSchedule(t *testing.T, opts ...schedule.Option) *schedule.Schedule {
	t.Helper()
	sch, err := schedule.New("reports", "0 * * * *", []byte("payload"), opts...)
	if err != nil {
		t.Fatalf("schedule.New: %v", err)
	}
	return sch
}

func TestCreateAndGetRoundTrip(t *testing.T) {
	s := openTestStore(t)
	want := mustNewSchedule(t, schedule.WithPriority(job.PriorityHigh), schedule.WithMaxRetries(4))

	if err := s.Create(want); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(want.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Queue != want.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, want.Queue)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, want.Payload)
	}
	if got.Priority != want.Priority {
		t.Errorf("Priority = %v, want %v", got.Priority, want.Priority)
	}
	if got.MaxRetries != want.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", got.MaxRetries, want.MaxRetries)
	}
	if got.CronExpr != want.CronExpr {
		t.Errorf("CronExpr = %q, want %q", got.CronExpr, want.CronExpr)
	}
	if got.Enabled != want.Enabled {
		t.Errorf("Enabled = %v, want %v", got.Enabled, want.Enabled)
	}
	if !got.NextRunAt.Equal(want.NextRunAt) {
		t.Errorf("NextRunAt = %v, want %v", got.NextRunAt, want.NextRunAt)
	}
	if !got.LastRunAt.IsZero() {
		t.Errorf("LastRunAt = %v, want zero (schedule has never fired)", got.LastRunAt)
	}
}

func TestGetMissingReturnsErrNotFound(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Get("does-not-exist"); !errors.Is(err, schedule.ErrNotFound) {
		t.Fatalf("Get: err = %v, want ErrNotFound", err)
	}
}

func TestListReturnsMostRecentFirst(t *testing.T) {
	s := openTestStore(t)

	first := mustNewSchedule(t)
	first.CreatedAt = first.CreatedAt.Add(-time.Minute) // force a stable ordering
	if err := s.Create(first); err != nil {
		t.Fatalf("Create: %v", err)
	}

	second := mustNewSchedule(t)
	if err := s.Create(second); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List returned %d schedules, want 2", len(got))
	}
	if got[0].ID != second.ID {
		t.Errorf("List()[0].ID = %q, want %q (most recently created first)", got[0].ID, second.ID)
	}
}

func TestSetEnabledTogglesFlag(t *testing.T) {
	s := openTestStore(t)
	sch := mustNewSchedule(t)
	if err := s.Create(sch); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.SetEnabled(sch.ID, false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}

	got, err := s.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Enabled {
		t.Error("Enabled = true, want false after SetEnabled(id, false)")
	}
}

func TestSetEnabledMissingReturnsErrNotFound(t *testing.T) {
	s := openTestStore(t)
	if err := s.SetEnabled("does-not-exist", false); !errors.Is(err, schedule.ErrNotFound) {
		t.Fatalf("SetEnabled: err = %v, want ErrNotFound", err)
	}
}

func TestDeleteRemovesSchedule(t *testing.T) {
	s := openTestStore(t)
	sch := mustNewSchedule(t)
	if err := s.Create(sch); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(sch.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := s.Get(sch.ID); !errors.Is(err, schedule.ErrNotFound) {
		t.Fatalf("Get after Delete: err = %v, want ErrNotFound", err)
	}
}

func TestDeleteMissingIsNotAnError(t *testing.T) {
	s := openTestStore(t)
	if err := s.Delete("does-not-exist"); err != nil {
		t.Fatalf("Delete of missing schedule: %v, want nil", err)
	}
}
