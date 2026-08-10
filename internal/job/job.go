// Package job defines the core Job type shared by the queue, storage, and
// worker packages.
package job

import (
	"time"

	"github.com/google/uuid"
)

// Priority controls the order in which pending jobs are dequeued. Higher
// values are dequeued first.
type Priority int

const (
	PriorityLow    Priority = -1
	PriorityNormal Priority = 0
	PriorityHigh   Priority = 1
)

// Job is a single unit of work tracked by the queue.
type Job struct {
	// ID uniquely identifies the job. It is assigned by NewJob.
	ID string

	// Queue is the logical queue/topic the job belongs to (e.g. "emails",
	// "reports"). Workers subscribe to one or more queues.
	Queue string

	// Payload is the job's opaque, handler-specific data.
	Payload []byte

	// Status is the job's current lifecycle state.
	Status Status

	// Priority influences dequeue order relative to other pending jobs in
	// the same queue.
	Priority Priority

	// MaxRetries is the number of additional attempts allowed after the
	// first failure before the job is moved to the dead-letter queue.
	MaxRetries int

	// RetryCount is the number of attempts made so far, not counting the
	// first.
	RetryCount int

	// LastError holds the error message from the most recent failed
	// attempt, if any.
	LastError string

	// CreatedAt is when the job was first enqueued.
	CreatedAt time.Time

	// UpdatedAt is when the job's state was last modified.
	UpdatedAt time.Time

	// RunAt is the earliest time at which the job is eligible to be
	// dequeued. Jobs enqueued without a delay have RunAt equal to
	// CreatedAt.
	RunAt time.Time
}

// Option configures a Job at construction time.
type Option func(*Job)

// WithPriority sets the job's priority.
func WithPriority(p Priority) Option {
	return func(j *Job) { j.Priority = p }
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(n int) Option {
	return func(j *Job) { j.MaxRetries = n }
}

// WithDelay schedules the job to become eligible for execution after d has
// elapsed from creation, instead of immediately.
func WithDelay(d time.Duration) Option {
	return func(j *Job) { j.RunAt = j.CreatedAt.Add(d) }
}

// WithRunAt schedules the job to become eligible for execution at t.
func WithRunAt(t time.Time) Option {
	return func(j *Job) { j.RunAt = t }
}

// New creates a new Job for the given queue with the given payload, applying
// any options. The job starts in StatusPending with a zero retry count.
func New(queue string, payload []byte, opts ...Option) *Job {
	now := time.Now().UTC()
	j := &Job{
		ID:        uuid.NewString(),
		Queue:     queue,
		Payload:   payload,
		Status:    StatusPending,
		Priority:  PriorityNormal,
		CreatedAt: now,
		UpdatedAt: now,
		RunAt:     now,
	}
	for _, opt := range opts {
		opt(j)
	}
	return j
}

// Ready reports whether the job is eligible to be dequeued at time t, i.e.
// it is pending and its scheduled run time has passed.
func (j *Job) Ready(t time.Time) bool {
	return j.Status == StatusPending && !t.Before(j.RunAt)
}

// CanRetry reports whether the job has retry attempts remaining.
func (j *Job) CanRetry() bool {
	return j.RetryCount < j.MaxRetries
}

// MarkRunning transitions the job to StatusRunning.
func (j *Job) MarkRunning(now time.Time) {
	j.Status = StatusRunning
	j.UpdatedAt = now
}

// MarkSucceeded transitions the job to StatusSucceeded.
func (j *Job) MarkSucceeded(now time.Time) {
	j.Status = StatusSucceeded
	j.LastError = ""
	j.UpdatedAt = now
}

// MarkFailed records err as the job's last error and transitions it to
// StatusRetrying (with RunAt pushed out by backoff) if retries remain, or to
// StatusDead otherwise.
func (j *Job) MarkFailed(now time.Time, err error, backoff time.Duration) {
	j.LastError = err.Error()
	j.UpdatedAt = now

	if j.CanRetry() {
		j.RetryCount++
		j.Status = StatusRetrying
		j.RunAt = now.Add(backoff)
	} else {
		j.Status = StatusDead
	}
}
