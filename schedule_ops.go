package batchjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// scheduleStore returns the Engine's schedule store, or
// ErrSchedulingDisabled if it was opened without one. Every method below
// starts here, so the check lives in exactly one place.
func (e *Engine) scheduleStore() (schedule.Store, error) {
	if e.schedules == nil {
		return nil, ErrSchedulingDisabled
	}
	return e.schedules, nil
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
// next comes due on a running Engine. Enqueue the first job yourself if it
// should not wait, or use TriggerSchedule.
func (e *Engine) AddSchedule(ctx context.Context, name, cronExpr string, payload []byte, opts ...Option) (*Schedule, error) {
	store, err := e.scheduleStore()
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, errors.New("batchjobs: queue name is required")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	internalOpts, err := scheduleOptions(opts)
	if err != nil {
		return nil, err
	}

	sch, err := schedule.New(name, cronExpr, payload, internalOpts...)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: add schedule on %q: %w", name, err)
	}
	if err := store.Create(sch); err != nil {
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
	store, err := e.scheduleStore()
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errors.New("batchjobs: schedule ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	sch, err := store.Get(id)
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
	store, err := e.scheduleStore()
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	schedules, err := store.List()
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
	store, err := e.scheduleStore()
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errors.New("batchjobs: schedule ID is required")
	}
	if edit == nil {
		return nil, errors.New("batchjobs: UpdateSchedule requires an edit function")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	stored, err := store.Get(id)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: update schedule %s: %w", id, err)
	}

	edited := convertSchedule(stored)
	edit(edited)
	if edited.Queue == "" {
		return nil, fmt.Errorf("batchjobs: update schedule %s: queue name is required", id)
	}

	stored.Queue = edited.Queue
	stored.Payload = edited.Payload
	stored.Priority = jobPriority(edited.Priority)
	stored.MaxRetries = edited.MaxRetries
	stored.Timeout = edited.Timeout

	// Only a genuinely changed expression moves the next fire time.
	// Rewriting it on every edit would let a caller adjusting the retry
	// budget silently postpone the schedule.
	if edited.Cron != stored.CronExpr {
		if err := stored.Reschedule(edited.Cron, time.Now().UTC()); err != nil {
			return nil, fmt.Errorf("batchjobs: update schedule %s: %w", id, err)
		}
	}

	if err := store.Update(stored); err != nil {
		return nil, fmt.Errorf("batchjobs: update schedule %s: %w", id, err)
	}
	return convertSchedule(stored), nil
}

// PauseSchedule stops a schedule firing without deleting it. A paused
// schedule accumulates no missed firings: the time it was paused for
// simply does not happen.
func (e *Engine) PauseSchedule(ctx context.Context, id string) error {
	return e.setScheduleEnabled(ctx, id, false)
}

// ResumeSchedule re-enables a paused schedule, recomputing its next fire
// time from now.
//
// Recomputing matters: a schedule paused for a week would otherwise come
// back a week overdue and fire the instant it is resumed, which is exactly
// the surprise someone pausing a schedule was trying to avoid. Resuming an
// already-enabled schedule leaves its next fire time alone, so it is safe
// to call twice.
func (e *Engine) ResumeSchedule(ctx context.Context, id string) error {
	store, err := e.scheduleStore()
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("batchjobs: schedule ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	stored, err := store.Get(id)
	if err != nil {
		return fmt.Errorf("batchjobs: resume schedule %s: %w", id, err)
	}
	if stored.Enabled {
		return nil
	}

	now := time.Now().UTC()
	next, err := stored.Advance(now)
	if err != nil {
		return fmt.Errorf("batchjobs: resume schedule %s: %w", id, err)
	}
	stored.NextRunAt = next

	// The fire time is written before the schedule is re-enabled, so the
	// scheduler cannot see an enabled schedule still carrying the stale
	// time it was paused with.
	if err := store.Update(stored); err != nil {
		return fmt.Errorf("batchjobs: resume schedule %s: %w", id, err)
	}
	if err := store.SetEnabled(id, true); err != nil {
		return fmt.Errorf("batchjobs: resume schedule %s: %w", id, err)
	}
	return nil
}

// setScheduleEnabled is the shared body of Pause and the enabling half of
// Resume.
func (e *Engine) setScheduleEnabled(ctx context.Context, id string, enabled bool) error {
	store, err := e.scheduleStore()
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("batchjobs: schedule ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := store.SetEnabled(id, enabled); err != nil {
		return fmt.Errorf("batchjobs: set schedule %s enabled=%v: %w", id, enabled, err)
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
	store, err := e.scheduleStore()
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("batchjobs: schedule ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := store.Delete(id); err != nil {
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
	store, err := e.scheduleStore()
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errors.New("batchjobs: schedule ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	stored, err := store.Get(id)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: trigger schedule %s: %w", id, err)
	}

	j := stored.NewJob()
	if err := e.queue.Enqueue(j); err != nil {
		return nil, fmt.Errorf("batchjobs: trigger schedule %s: %w", id, err)
	}
	return convert(j), nil
}
