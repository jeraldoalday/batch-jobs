package batchjobs

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

// This file is in the package rather than beside it because the finest
// granularity a cron expression has is one minute, and a test cannot wait
// that long. Reaching into the Engine's own store to backdate a
// schedule's next fire time is what makes the real scheduler loop --
// polling, enqueueing, advancing -- observable in milliseconds. Everything
// else about the schedule surface is tested through the public API in
// schedule_test.go.

// firingEngine opens a started Engine with scheduling on and a tight poll
// interval, plus a handler that reports each job it runs on the returned
// channel.
func firingEngine(t *testing.T) (*Engine, <-chan *Job) {
	t.Helper()

	e, err := Open(Config{
		DB:           filepath.Join(t.TempDir(), "jobs.db"),
		Workers:      2,
		Schedules:    true,
		SchedulePoll: 5 * time.Millisecond,
		ReapInterval: -1,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { e.Close() })

	// Buffered generously: a schedule firing every minute can legitimately
	// produce several jobs while a test is looking at the first.
	ran := make(chan *Job, 16)
	e.Handle("reports", func(ctx context.Context, j *Job) error {
		select {
		case ran <- j:
		default:
		}
		return nil
	})

	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(e.Stop)
	return e, ran
}

// backdate drags a schedule's next fire time into the past, so the very
// next poll finds it due.
func backdate(t *testing.T, e *Engine, id string) {
	t.Helper()

	stored, err := e.schedules.Get(id)
	if err != nil {
		t.Fatalf("read schedule %s: %v", id, err)
	}
	stored.NextRunAt = time.Now().UTC().Add(-time.Minute)
	if err := e.schedules.Update(stored); err != nil {
		t.Fatalf("backdate schedule %s: %v", id, err)
	}
}

func TestStartedEngineFiresADueSchedule(t *testing.T) {
	e, ran := firingEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "* * * * *", []byte("nightly"),
		WithPriority(PriorityHigh),
		WithMaxRetries(2),
	)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	backdate(t, e, created.ID)

	select {
	case j := <-ran:
		if j.Queue != "reports" {
			t.Errorf("Queue = %q, want %q", j.Queue, "reports")
		}
		if string(j.Payload) != "nightly" {
			t.Errorf("Payload = %q, want %q", j.Payload, "nightly")
		}
		if j.Priority != PriorityHigh {
			t.Errorf("Priority = %v, want the schedule's %v", j.Priority, PriorityHigh)
		}
		if j.MaxRetries != 2 {
			t.Errorf("MaxRetries = %d, want the schedule's 2", j.MaxRetries)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the scheduler to fire a due schedule")
	}
}

func TestFiringAdvancesTheSchedule(t *testing.T) {
	e, ran := firingEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "* * * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	backdate(t, e, created.ID)

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the schedule to fire")
	}

	// The firing is recorded and the schedule moved on, which is what
	// stops it firing again on the very next poll.
	now := time.Now().UTC()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := e.GetSchedule(ctx, created.ID)
		if err != nil {
			t.Fatalf("GetSchedule: %v", err)
		}
		if !got.LastRunAt.IsZero() && got.NextRunAt.After(now) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("schedule not advanced: LastRunAt = %v, NextRunAt = %v, want a recorded firing and a future next run",
				got.LastRunAt, got.NextRunAt)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAPausedScheduleDoesNotFire(t *testing.T) {
	e, ran := firingEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "* * * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if err := e.PauseSchedule(ctx, created.ID); err != nil {
		t.Fatalf("PauseSchedule: %v", err)
	}
	backdate(t, e, created.ID)

	// Long enough for many polls at the 5ms cadence: a paused schedule
	// that was going to fire would have done so several times over.
	select {
	case j := <-ran:
		t.Fatalf("paused schedule fired job %s, want no firing", j.ID)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestADeletedScheduleDoesNotFire(t *testing.T) {
	e, ran := firingEngine(t)
	ctx := context.Background()

	created, err := e.AddSchedule(ctx, "reports", "* * * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	backdate(t, e, created.ID)
	if err := e.DeleteSchedule(ctx, created.ID); err != nil {
		t.Fatalf("DeleteSchedule: %v", err)
	}

	select {
	case <-ran:
		// A firing that beat the delete is legitimate: the schedule was
		// genuinely due when the poll read it. What must not happen is
		// a second one.
		select {
		case j := <-ran:
			t.Fatalf("deleted schedule fired job %s again, want no further firing", j.ID)
		case <-time.After(250 * time.Millisecond):
		}
	case <-time.After(250 * time.Millisecond):
	}
}

func TestSchedulerDoesNotRunWithoutStart(t *testing.T) {
	e, err := Open(Config{
		DB:           filepath.Join(t.TempDir(), "jobs.db"),
		Schedules:    true,
		SchedulePoll: 5 * time.Millisecond,
		ReapInterval: -1,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	ctx := context.Background()
	created, err := e.AddSchedule(ctx, "reports", "* * * * *", nil)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	backdate(t, e, created.ID)

	// An unstarted Engine can define schedules -- which is what a
	// producer-only or administrative process does -- but must not fire
	// them.
	time.Sleep(100 * time.Millisecond)
	if n := e.Len(); n != 0 {
		t.Fatalf("Len() = %d after an unstarted engine's schedule came due, want 0", n)
	}
}
