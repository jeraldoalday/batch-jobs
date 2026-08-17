package schedule

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// ErrNotFound is returned by Store lookups when no schedule with the given
// ID exists.
var ErrNotFound = errors.New("schedule: not found")

// Schedule is a recurring job template: a cron expression plus the fields
// needed to construct a new job.Job each time it fires. A scheduler loop
// (see internal/scheduler) advances NextRunAt and enqueues a fresh job.Job
// every time a Schedule comes due.
type Schedule struct {
	// ID uniquely identifies the schedule. It is assigned by New.
	ID string

	// Queue is the queue.Queue-level queue each produced job is
	// enqueued onto.
	Queue string

	// Payload is copied onto every job.Job this schedule produces.
	Payload []byte

	// Priority is applied to every job.Job this schedule produces.
	Priority job.Priority

	// MaxRetries is applied to every job.Job this schedule produces.
	MaxRetries int

	// Timeout is applied to every job.Job this schedule produces, bounding
	// how long a single execution attempt may run. Zero means no
	// per-attempt timeout, the same default job.New itself uses.
	Timeout time.Duration

	// CronExpr is the 5-field cron expression controlling when this
	// schedule fires. See ParseCron for the supported syntax.
	CronExpr string

	// Enabled controls whether the schedule is eligible to fire. A
	// disabled schedule is skipped by Store.Due until re-enabled.
	Enabled bool

	// NextRunAt is the next time this schedule is due to fire.
	NextRunAt time.Time

	// LastRunAt is the last time this schedule fired, or the zero time
	// if it has never fired.
	LastRunAt time.Time

	// CreatedAt is when the schedule was created.
	CreatedAt time.Time

	// UpdatedAt is when the schedule's state was last modified.
	UpdatedAt time.Time
}

// Option configures a Schedule at construction time.
type Option func(*Schedule)

// WithPriority sets the priority applied to every job the schedule
// produces.
func WithPriority(p job.Priority) Option {
	return func(s *Schedule) { s.Priority = p }
}

// WithMaxRetries sets the max retry count applied to every job the
// schedule produces.
func WithMaxRetries(n int) Option {
	return func(s *Schedule) { s.MaxRetries = n }
}

// WithTimeout sets the per-attempt execution timeout applied to every job
// the schedule produces.
func WithTimeout(d time.Duration) Option {
	return func(s *Schedule) { s.Timeout = d }
}

// New creates a Schedule that enqueues payload onto queueName each time
// cronExpr fires next. It returns an error if cronExpr cannot be parsed.
func New(queueName, cronExpr string, payload []byte, opts ...Option) (*Schedule, error) {
	c, err := ParseCron(cronExpr)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	next, ok := c.Next(now)
	if !ok {
		return nil, fmt.Errorf("schedule: cron expression %q never fires", cronExpr)
	}

	s := &Schedule{
		ID:        uuid.NewString(),
		Queue:     queueName,
		Payload:   payload,
		Priority:  job.PriorityNormal,
		CronExpr:  cronExpr,
		Enabled:   true,
		NextRunAt: next,
		CreatedAt: now,
		UpdatedAt: now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// NewJob builds the job.Job this Schedule produces for its current firing,
// applying the schedule's queue, payload, priority, and retry policy.
func (s *Schedule) NewJob() *job.Job {
	return job.New(s.Queue, s.Payload,
		job.WithPriority(s.Priority),
		job.WithMaxRetries(s.MaxRetries),
		job.WithTimeout(s.Timeout),
	)
}

// Advance parses the schedule's cron expression and returns the next time
// after now at which it fires. Callers use this after a firing to compute
// the value to persist via Store.MarkFired.
func (s *Schedule) Advance(now time.Time) (time.Time, error) {
	c, err := ParseCron(s.CronExpr)
	if err != nil {
		return time.Time{}, err
	}
	next, ok := c.Next(now)
	if !ok {
		return time.Time{}, fmt.Errorf("schedule: cron expression %q never fires again after %v", s.CronExpr, now)
	}
	return next, nil
}

// Store persists Schedules and lets a scheduler loop find the ones due to
// fire without scanning every row.
type Store interface {
	// Create persists a new schedule.
	Create(s *Schedule) error

	// Get returns the schedule with the given ID, or ErrNotFound.
	Get(id string) (*Schedule, error)

	// List returns every schedule, most recently created first.
	List() ([]*Schedule, error)

	// Due returns enabled schedules whose NextRunAt has passed as of
	// now, soonest first.
	Due(now time.Time) ([]*Schedule, error)

	// MarkFired records that the schedule with the given ID fired at
	// firedAt and is next due at next.
	MarkFired(id string, firedAt, next time.Time) error

	// SetEnabled toggles whether a schedule is eligible to fire.
	SetEnabled(id string, enabled bool) error

	// Delete removes a schedule permanently. It is not an error to
	// delete a schedule that does not exist.
	Delete(id string) error

	// Close releases any resources held by the store.
	Close() error
}
