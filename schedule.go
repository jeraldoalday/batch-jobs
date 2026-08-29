package batchjobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// Schedule is a recurring job: a cron expression plus the template every
// firing is stamped from. The Engine's scheduler enqueues a fresh job each
// time a schedule comes due, so a handler sees an ordinary Job and never
// has to know it was produced by a schedule.
//
// As with Job, a Schedule handed to a caller is a copy. Change one through
// Engine.UpdateSchedule rather than by mutating the struct.
type Schedule struct {
	// ID uniquely identifies the schedule. It is what every lookup takes.
	ID string

	// Queue is the queue each produced job is enqueued onto, and so what
	// decides which handler runs it.
	Queue string

	// Cron is the 5-field cron expression -- minute, hour, day of month,
	// month, day of week -- deciding when the schedule fires. See
	// ValidateCron for the supported syntax.
	Cron string

	// Payload is copied onto every job the schedule produces.
	Payload []byte

	// Priority is applied to every job the schedule produces.
	Priority Priority

	// MaxRetries is applied to every job the schedule produces.
	MaxRetries int

	// Timeout is applied to every job the schedule produces, bounding a
	// single attempt. Zero means no per-attempt timeout.
	Timeout time.Duration

	// Enabled reports whether the schedule is eligible to fire. A paused
	// schedule is skipped rather than accumulating missed firings, so
	// resuming one does not unleash a backlog.
	Enabled bool

	// NextRunAt is when the schedule is next due. On a running Engine it
	// is normally in the future; a time in the past means the scheduler
	// has not caught up yet.
	NextRunAt time.Time

	// LastRunAt is when the schedule last fired, or the zero time if it
	// never has.
	LastRunAt time.Time

	// CreatedAt is when the schedule was created.
	CreatedAt time.Time

	// UpdatedAt is when the schedule last changed, whether by an edit, a
	// pause, or a firing.
	UpdatedAt time.Time
}

// Decode unmarshals the schedule's payload as JSON into v, the mirror of
// Engine.AddScheduleJSON.
func (s *Schedule) Decode(v any) error {
	if len(s.Payload) == 0 {
		return fmt.Errorf("batchjobs: schedule %s has an empty payload", s.ID)
	}
	if err := json.Unmarshal(s.Payload, v); err != nil {
		return fmt.Errorf("batchjobs: decode schedule %s payload: %w", s.ID, err)
	}
	return nil
}

// Upcoming returns the next n times the schedule fires, soonest first,
// starting from NextRunAt. It is the answer to "did I write that cron
// expression correctly?", which is worth asking before a schedule that
// meant to run nightly turns out to run every minute.
//
// A schedule that stops firing yields fewer than n times rather than an
// error.
func (s *Schedule) Upcoming(n int) ([]time.Time, error) {
	internal := &schedule.Schedule{CronExpr: s.Cron, NextRunAt: s.NextRunAt}
	times, err := internal.Upcoming(n)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: upcoming fire times for schedule %s: %w", s.ID, err)
	}
	return times, nil
}

// Overdue reports whether the schedule was due before now and has not
// fired yet. A schedule that stays overdue means the scheduler is not
// running, or is not keeping up.
func (s *Schedule) Overdue(now time.Time) bool {
	return s.Enabled && s.NextRunAt.Before(now)
}

// ValidateCron reports whether expr is a cron expression this package
// accepts, returning a descriptive error if not.
//
// The syntax is the standard five fields -- minute, hour, day of month,
// month, day of week -- where each accepts "*", a single value, a
// comma-separated list, a "lo-hi" range, or any of those with a "/step"
// suffix. "*/15 * * * *" is every quarter hour; "0 3 * * 1-5" is 3am on
// weekdays.
//
// Use it to check user-supplied input before offering to save it. Engine's
// schedule methods validate too, so this is a courtesy rather than a
// prerequisite.
func ValidateCron(expr string) error {
	if _, err := schedule.ParseCron(expr); err != nil {
		return fmt.Errorf("batchjobs: %w", err)
	}
	return nil
}

// convertSchedule turns the internal schedule into the exported one, for
// the same reason convert exists for jobs: the internal type carries the
// store's own concerns, and exporting it would make them public promises.
func convertSchedule(s *schedule.Schedule) *Schedule {
	if s == nil {
		return nil
	}
	return &Schedule{
		ID:         s.ID,
		Queue:      s.Queue,
		Cron:       s.CronExpr,
		Payload:    s.Payload,
		Priority:   Priority(s.Priority),
		MaxRetries: s.MaxRetries,
		Timeout:    s.Timeout,
		Enabled:    s.Enabled,
		NextRunAt:  s.NextRunAt,
		LastRunAt:  s.LastRunAt,
		CreatedAt:  s.CreatedAt,
		UpdatedAt:  s.UpdatedAt,
	}
}

// convertSchedules maps convertSchedule over a slice, preserving order.
func convertSchedules(schedules []*schedule.Schedule) []*Schedule {
	out := make([]*Schedule, len(schedules))
	for i, s := range schedules {
		out[i] = convertSchedule(s)
	}
	return out
}

// scheduleOptions resolves the per-job options that apply to a schedule's
// template, rejecting the ones that do not.
//
// Enqueue's options are reused deliberately: a schedule's job carries the
// same priority, retry budget and timeout as any other, and a caller
// should not have to learn a second vocabulary for them. The three that
// describe a single submission rather than a template -- when to run it,
// and which submission it is -- are rejected rather than ignored, since
// silently dropping WithIdempotencyKey would look like deduplication that
// is not happening.
func scheduleOptions(opts []Option) ([]schedule.Option, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	switch {
	case o.delay > 0:
		return nil, errors.New("batchjobs: WithDelay does not apply to a schedule; its cron expression decides when it runs")
	case !o.runAt.IsZero():
		return nil, errors.New("batchjobs: WithRunAt does not apply to a schedule; its cron expression decides when it runs")
	case o.idempotencyKey != "":
		return nil, errors.New("batchjobs: WithIdempotencyKey does not apply to a schedule; one key would collapse every firing into the first job")
	}

	return []schedule.Option{
		schedule.WithPriority(jobPriority(o.priority)),
		schedule.WithMaxRetries(o.maxRetries),
		schedule.WithTimeout(o.timeout),
	}, nil
}
