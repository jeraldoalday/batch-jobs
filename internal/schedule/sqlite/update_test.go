package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

func TestUpdatePersistsEveryMutableField(t *testing.T) {
	s := openTestStore(t)
	sch := mustNewSchedule(t)
	if err := s.Create(sch); err != nil {
		t.Fatalf("Create: %v", err)
	}

	sch.Queue = "digests"
	sch.Payload = []byte(`{"window":"daily"}`)
	sch.Priority = job.PriorityHigh
	sch.MaxRetries = 7
	sch.Timeout = 90 * time.Second
	if err := sch.Reschedule("15 4 * * *", time.Now().UTC()); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	if err := s.Update(sch); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := s.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Queue != "digests" {
		t.Errorf("Queue = %q, want %q", got.Queue, "digests")
	}
	if string(got.Payload) != `{"window":"daily"}` {
		t.Errorf("Payload = %q, want %q", got.Payload, `{"window":"daily"}`)
	}
	if got.Priority != job.PriorityHigh {
		t.Errorf("Priority = %v, want %v", got.Priority, job.PriorityHigh)
	}
	if got.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7", got.MaxRetries)
	}
	if got.Timeout != 90*time.Second {
		t.Errorf("Timeout = %v, want %v", got.Timeout, 90*time.Second)
	}
	if got.CronExpr != "15 4 * * *" {
		t.Errorf("CronExpr = %q, want %q", got.CronExpr, "15 4 * * *")
	}
	if !got.NextRunAt.Equal(sch.NextRunAt) {
		t.Errorf("NextRunAt = %v, want %v", got.NextRunAt, sch.NextRunAt)
	}
}

func TestUpdateLeavesEnabledAndLastRunAlone(t *testing.T) {
	s := openTestStore(t)
	sch := mustNewSchedule(t)
	if err := s.Create(sch); err != nil {
		t.Fatalf("Create: %v", err)
	}

	fired := time.Now().UTC().Truncate(time.Second)
	if err := s.MarkFired(sch.ID, fired, fired.Add(time.Hour)); err != nil {
		t.Fatalf("MarkFired: %v", err)
	}
	if err := s.SetEnabled(sch.ID, false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}

	// The caller edits a copy read before the firing, which is the shape
	// of a read-modify-write over HTTP: it still must not roll the
	// schedule's history back or quietly re-enable it.
	sch.Enabled = true
	sch.Queue = "digests"
	if err := s.Update(sch); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := s.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Enabled {
		t.Error("Enabled = true, want the schedule left paused by SetEnabled")
	}
	if !got.LastRunAt.Equal(fired) {
		t.Errorf("LastRunAt = %v, want it left at %v", got.LastRunAt, fired)
	}
	if got.Queue != "digests" {
		t.Errorf("Queue = %q, want the edit applied", got.Queue)
	}
}

func TestUpdateUnknownScheduleReportsNotFound(t *testing.T) {
	s := openTestStore(t)
	sch := mustNewSchedule(t)

	err := s.Update(sch)
	if !errors.Is(err, schedule.ErrNotFound) {
		t.Fatalf("Update of an uncreated schedule = %v, want schedule.ErrNotFound", err)
	}
}

func TestUpdateBumpsUpdatedAt(t *testing.T) {
	s := openTestStore(t)
	sch := mustNewSchedule(t)
	if err := s.Create(sch); err != nil {
		t.Fatalf("Create: %v", err)
	}

	before, err := s.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	sch.MaxRetries = 2
	if err := s.Update(sch); err != nil {
		t.Fatalf("Update: %v", err)
	}

	after, err := s.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.UpdatedAt.Before(before.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want at or after %v", after.UpdatedAt, before.UpdatedAt)
	}
}
