package batchjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// scheduleManager returns the Engine's schedule manager, or
// ErrSchedulingDisabled if it was opened without one. Every method below
// starts here, so the check lives in exactly one place.
func (e *Engine) scheduleManager() (*schedule.Manager, error) {
	if e.scheduleMgr == nil {
		return nil, ErrSchedulingDisabled
	}
	return e.scheduleMgr, nil
}

// AddSchedule creates a recurring job: every time cronExpr comes due, a
// fresh job carrying payload is enqueued onto the named queue.
//
// The options are the same ones Enqueue takes, and describe the template
// each firing is stamped from -- WithPriority, WithMaxRetries and
// WithTimeout. The three that describe a single submission are rejected:
// see scheduleOptions.
//
// A new schedule is enabled, so it starts firing as soon as its expression
// next comes due on a running Engine. Use TriggerSchedule if the first job
// should not wait.
func (e *Engine) AddSchedule(ctx context.Context, name, cronExpr string, payload []byte, opts ...Option) (*Schedule, error) {
	mgr, err := e.scheduleManager()
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	internalOpts, err := scheduleOptions(opts)
	if err != nil {
		return nil, err
	}

	sch, err := mgr.Add(name, cronExpr, payload, internalOpts...)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: add schedule on %q: %w", name, err)
	}
	return convertSchedule(sch), nil
}

// AddScheduleJSON marshals v as the payload of every job the schedule
// produces, the recurring counterpart of EnqueueJSON. Job.Decode is its
// mirror in the handler.
func (e *Engine) AddScheduleJSON(ctx context.Context, name, cronExpr string, v any, opts ...Option) (*Schedule, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: encode payload for schedule on %q: %w", name, err)
	}
	return e.AddSchedule(ctx, name, cronExpr, payload, opts...)
}

// GetSchedule returns the schedule with the given ID, or an error wrapping
// ErrScheduleNotFound.
func (e *Engine) GetSchedule(ctx context.Context, id string) (*Schedule, error) {
	mgr, err := e.scheduleManager()
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	sch, err := mgr.Get(id)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: get schedule %s: %w", id, err)
	}
	return convertSchedule(sch), nil
}

// ListSchedules returns every schedule, most recently created first.
//
// Unlike List, it takes no limit: a schedule set is written by people and
// stays small, and an operator asking what is scheduled wants all of it.
func (e *Engine) ListSchedules(ctx context.Context) ([]*Schedule, error) {
	mgr, err := e.scheduleManager()
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	schedules, err := mgr.List()
	if err != nil {
		return nil, fmt.Errorf("batchjobs: list schedules: %w", err)
	}
	return convertSchedules(schedules), nil
}

// UpdateSchedule reads the schedule, hands it to edit, and stores what
// edit leaves behind. Only the template fields and the cron expression may
// be changed -- Queue, Cron, Payload, Priority, MaxRetries and Timeout --
// and setting any other field has no effect, since whether a schedule is
// enabled belongs to Pause and Resume and its history belongs to the
// scheduler.
//
//	s, err := engine.UpdateSchedule(ctx, id, func(s *batchjobs.Schedule) {
//	    s.Cron = "*/5 * * * *"
//	})
//
// A changed expression recomputes NextRunAt from now, and is validated
// before anything is written: an edit that will not parse leaves the
// stored schedule exactly as it was.
func (e *Engine) UpdateSchedule(ctx context.Context, id string, edit func(*Schedule)) (*Schedule, error) {
	mgr, err := e.scheduleManager()
	if err != nil {
		return nil, err
	}
	if edit == nil {
		return nil, errors.New("batchjobs: UpdateSchedule requires an edit function")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	// The edit runs against the public type, so a caller never sees the
	// internal one; only the fields an edit may change are copied back.
	updated, err := mgr.Update(id, func(s *schedule.Schedule) error {
		edited := convertSchedule(s)
		edit(edited)

		s.Queue = edited.Queue
		s.Payload = edited.Payload
		s.Priority = jobPriority(edited.Priority)
		s.MaxRetries = edited.MaxRetries
		s.Timeout = edited.Timeout
		s.CronExpr = edited.Cron
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("batchjobs: update schedule %s: %w", id, err)
	}
	return convertSchedule(updated), nil
}

// PauseSchedule stops a schedule firing without deleting it. A paused
// schedule accumulates no missed firings: the time it was paused for
// simply does not happen.
func (e *Engine) PauseSchedule(ctx context.Context, id string) error {
	mgr, err := e.scheduleManager()
	if err != nil {
		return err
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := mgr.Pause(id); err != nil {
		return fmt.Errorf("batchjobs: pause schedule %s: %w", id, err)
	}
	return nil
}

// ResumeSchedule re-enables a paused schedule, recomputing its next fire
// time from now.
//
// Recomputing matters: a schedule paused for a week would otherwise come
// back a week overdue and fire the instant it is resumed, which is exactly
// the surprise someone pausing a schedule was trying to avoid. Resuming an
// already-enabled schedule changes nothing, so it is safe to repeat.
func (e *Engine) ResumeSchedule(ctx context.Context, id string) error {
	mgr, err := e.scheduleManager()
	if err != nil {
		return err
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := mgr.Resume(id); err != nil {
		return fmt.Errorf("batchjobs: resume schedule %s: %w", id, err)
	}
	return nil
}

// DeleteSchedule removes a schedule permanently. Jobs it has already
// produced are untouched -- they are ordinary jobs by then, and deleting
// the recipe does not recall the meal.
//
// Deleting a schedule that does not exist is not an error, so a cleanup
// path does not have to look first.
func (e *Engine) DeleteSchedule(ctx context.Context, id string) error {
	mgr, err := e.scheduleManager()
	if err != nil {
		return err
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := mgr.Delete(id); err != nil {
		return fmt.Errorf("batchjobs: delete schedule %s: %w", id, err)
	}
	return nil
}

// TriggerSchedule enqueues the job a schedule produces immediately, and
// returns it. The schedule's own timetable is untouched: its next fire
// time does not move, and the firing is not recorded as one.
//
// It is what "run it now" means -- checking that a newly written schedule
// does what its author intended, or re-running a nightly job after fixing
// whatever it was waiting on -- without either waiting until tomorrow or
// pretending the scheduled run already happened.
//
// It works on a paused schedule too: pausing stops the timetable, not the
// operator.
func (e *Engine) TriggerSchedule(ctx context.Context, id string) (*Job, error) {
	mgr, err := e.scheduleManager()
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	j, err := mgr.Trigger(id)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: trigger schedule %s: %w", id, err)
	}
	return convert(j), nil
}
