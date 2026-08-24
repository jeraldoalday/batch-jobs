package batchjobs

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// Status is a job's lifecycle state.
type Status string

const (
	// StatusPending means the job is enqueued and waiting for a worker.
	StatusPending Status = Status(job.StatusPending)

	// StatusRunning means a worker has claimed the job and is running it.
	StatusRunning Status = Status(job.StatusRunning)

	// StatusSucceeded means the job's handler returned without error.
	StatusSucceeded Status = Status(job.StatusSucceeded)

	// StatusFailed means the handler returned an error it declared
	// permanent with Permanent, so no further attempt was made.
	StatusFailed Status = Status(job.StatusFailed)

	// StatusRetrying means the job failed and another attempt is
	// scheduled for its RunAt time.
	StatusRetrying Status = Status(job.StatusRetrying)

	// StatusDead means the job exhausted its retries and is parked for
	// inspection. Engine.RetryJob returns one to the queue.
	StatusDead Status = Status(job.StatusDead)

	// StatusCancelled means the job was cancelled before a worker claimed
	// it.
	StatusCancelled Status = Status(job.StatusCancelled)
)

// Statuses lists every status, in rough lifecycle order.
var Statuses = []Status{
	StatusPending,
	StatusRunning,
	StatusRetrying,
	StatusSucceeded,
	StatusFailed,
	StatusDead,
	StatusCancelled,
}

// Terminal reports whether the status is final: the job will not leave it
// on its own, though Engine.RetryJob can revive a dead or failed one.
func (s Status) Terminal() bool { return job.Status(s).Terminal() }

// Valid reports whether s is one of the known statuses.
func (s Status) Valid() bool { return job.Status(s).Valid() }

func (s Status) String() string { return string(s) }

// Priority orders ready jobs against each other. Higher runs first.
type Priority int

const (
	PriorityLow    Priority = Priority(job.PriorityLow)
	PriorityNormal Priority = Priority(job.PriorityNormal)
	PriorityHigh   Priority = Priority(job.PriorityHigh)
)

func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	default:
		return fmt.Sprintf("priority(%d)", int(p))
	}
}

// Job is a unit of work. Handlers receive one; Engine.Enqueue returns one.
//
// A handler is given its own copy, so mutating the struct is harmless and
// changes nothing about the job the engine is tracking. Report an outcome
// by returning an error (or nil) instead.
type Job struct {
	// ID uniquely identifies the job. It is what every lookup takes.
	ID string

	// Queue is the logical queue the job belongs to, and what decides
	// which handler runs it.
	Queue string

	// Payload is the job's opaque, handler-specific data.
	Payload []byte

	// Status is the job's lifecycle state.
	Status Status

	// Priority orders this job against other ready jobs.
	Priority Priority

	// MaxRetries is how many further attempts follow the first failure
	// before the job is dead-lettered.
	MaxRetries int

	// RetryCount is how many retries have happened so far.
	RetryCount int

	// LastError is the message from the most recent failed attempt.
	LastError string

	// CreatedAt is when the job was first enqueued.
	CreatedAt time.Time

	// UpdatedAt is when the job's state last changed.
	UpdatedAt time.Time

	// RunAt is the earliest time the job may run. For a retrying job it
	// is when the next attempt is due.
	RunAt time.Time

	// Timeout bounds a single attempt, after which the handler's context
	// is cancelled. Zero means no per-attempt timeout.
	Timeout time.Duration

	// IdempotencyKey is the key the job was enqueued under, if any.
	IdempotencyKey string
}

// Attempt is the 1-based number of the attempt currently being made: 1 the
// first time a handler sees the job, 2 on the first retry, and so on. It is
// the number to log, since RetryCount reads as one less than the attempt a
// handler is actually performing.
func (j *Job) Attempt() int { return j.RetryCount + 1 }

// AttemptsLeft is how many further attempts remain after this one, or 0
// once the job's retries are spent. A handler can use it to decide whether
// a failure is the last word -- to write a compensating record, say, or
// page someone -- rather than one of several tries.
func (j *Job) AttemptsLeft() int {
	if n := j.MaxRetries - j.RetryCount; n > 0 {
		return n
	}
	return 0
}

// Decode unmarshals the job's payload as JSON into v, the mirror of
// Engine.EnqueueJSON.
func (j *Job) Decode(v any) error {
	if len(j.Payload) == 0 {
		return fmt.Errorf("batchjobs: job %s has an empty payload", j.ID)
	}
	if err := json.Unmarshal(j.Payload, v); err != nil {
		return fmt.Errorf("batchjobs: decode job %s payload: %w", j.ID, err)
	}
	return nil
}

// convert turns the engine's internal job into the exported one. The two
// are deliberately separate types: the internal one carries lifecycle
// methods that only the engine may call, and exporting it would make the
// public API a promise about the engine's internals.
func convert(j *job.Job) *Job {
	if j == nil {
		return nil
	}
	return &Job{
		ID:             j.ID,
		Queue:          j.Queue,
		Payload:        j.Payload,
		Status:         Status(j.Status),
		Priority:       Priority(j.Priority),
		MaxRetries:     j.MaxRetries,
		RetryCount:     j.RetryCount,
		LastError:      j.LastError,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		RunAt:          j.RunAt,
		Timeout:        j.Timeout,
		IdempotencyKey: j.IdempotencyKey,
	}
}

// convertAll maps convert over a slice, preserving order.
func convertAll(jobs []*job.Job) []*Job {
	out := make([]*Job, len(jobs))
	for i, j := range jobs {
		out[i] = convert(j)
	}
	return out
}

// Option customizes a job at enqueue time.
type Option func(*options)

// options accumulates what the Option funcs set, so the public options do
// not have to name the internal job type.
type options struct {
	priority       Priority
	maxRetries     int
	runAt          time.Time
	delay          time.Duration
	timeout        time.Duration
	idempotencyKey string
}

// WithPriority sets the job's priority relative to other ready jobs.
func WithPriority(p Priority) Option {
	return func(o *options) { o.priority = p }
}

// WithMaxRetries allows n further attempts after the first failure, after
// which the job is dead-lettered.
func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = n }
}

// WithDelay makes the job ineligible to run until d has elapsed.
func WithDelay(d time.Duration) Option {
	return func(o *options) { o.delay = d }
}

// WithRunAt makes the job ineligible to run until t. A time already past
// leaves the job immediately eligible.
func WithRunAt(t time.Time) Option {
	return func(o *options) { o.runAt = t }
}

// WithTimeout bounds a single attempt to d, after which the handler's
// context is cancelled. A handler that ignores ctx.Done() is not
// interrupted by this -- the timeout can only cancel a context, not stop
// running code.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// WithIdempotencyKey makes an enqueue safe to repeat: a second enqueue with
// the same key for the same queue returns the job the first one created
// rather than creating another. Use it wherever a retried enqueue must not
// produce duplicate work -- charging a card, sending an email.
func WithIdempotencyKey(key string) Option {
	return func(o *options) { o.idempotencyKey = key }
}

// build applies opts and returns the internal job they describe.
func build(queue string, payload []byte, opts []Option) *job.Job {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	internal := []job.Option{
		job.WithPriority(job.Priority(o.priority)),
		job.WithMaxRetries(o.maxRetries),
	}
	// RunAt is applied after Delay so an explicit instant wins over a
	// relative one when a caller passes both, rather than the result
	// depending on the order the options happened to be listed in.
	if o.delay > 0 {
		internal = append(internal, job.WithDelay(o.delay))
	}
	if !o.runAt.IsZero() {
		internal = append(internal, job.WithRunAt(o.runAt))
	}
	if o.timeout > 0 {
		internal = append(internal, job.WithTimeout(o.timeout))
	}
	if o.idempotencyKey != "" {
		internal = append(internal, job.WithIdempotencyKey(o.idempotencyKey))
	}
	return job.New(queue, payload, internal...)
}
