package client

import (
	"encoding/json"
	"fmt"
	"time"
)

// Status is a job's lifecycle state, as reported by the server.
type Status string

const (
	// StatusPending means the job is enqueued and waiting for a worker.
	StatusPending Status = "pending"

	// StatusRunning means a worker has claimed the job and is executing it.
	StatusRunning Status = "running"

	// StatusSucceeded means the job's handler returned without error.
	StatusSucceeded Status = "succeeded"

	// StatusFailed means the job failed in a way the handler declared
	// permanent, so no further attempts were made.
	StatusFailed Status = "failed"

	// StatusRetrying means the job failed and is scheduled for another
	// attempt at its RunAt time.
	StatusRetrying Status = "retrying"

	// StatusDead means the job exhausted its retries and is parked for
	// inspection. Such a job can be revived with Client.Retry.
	StatusDead Status = "dead"

	// StatusCancelled means the job was cancelled before a worker claimed it.
	StatusCancelled Status = "cancelled"
)

// Statuses lists every status a job can be in, in rough lifecycle order. It
// is useful for building a complete stats breakdown, since the server omits
// statuses with no jobs.
var Statuses = []Status{
	StatusPending,
	StatusRunning,
	StatusRetrying,
	StatusSucceeded,
	StatusFailed,
	StatusDead,
	StatusCancelled,
}

// Terminal reports whether the status is final: the job will not move out
// of it on its own, though Client.Retry can revive a dead or failed one.
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

// Valid reports whether s is a status this client knows. A false result for
// a status the server returned means the server is newer than this client.
func (s Status) Valid() bool {
	for _, known := range Statuses {
		if s == known {
			return true
		}
	}
	return false
}

func (s Status) String() string { return string(s) }

// Priority controls dequeue order among ready jobs. Higher runs first.
type Priority int

const (
	PriorityLow    Priority = -1
	PriorityNormal Priority = 0
	PriorityHigh   Priority = 1
)

// Valid reports whether p is one of the three priorities the server accepts.
func (p Priority) Valid() bool {
	return p == PriorityLow || p == PriorityNormal || p == PriorityHigh
}

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

// Job is a job as the server reports it.
//
// It is decoded from the API's JSON representation, which carries the
// per-attempt timeout as whole seconds; this type exposes it as a
// time.Duration instead, so callers work in Go's own units on both sides of
// the wire.
type Job struct {
	// ID uniquely identifies the job, and is what every other call takes.
	ID string

	// Queue is the logical queue the job belongs to.
	Queue string

	// Payload is the job's opaque, handler-specific data.
	Payload []byte

	// Status is the job's lifecycle state at the time of the response.
	Status Status

	// Priority is the job's dequeue priority.
	Priority Priority

	// MaxRetries is how many additional attempts follow the first failure.
	MaxRetries int

	// RetryCount is how many retries have been made so far.
	RetryCount int

	// LastError is the message from the most recent failed attempt.
	LastError string

	// CreatedAt is when the job was first enqueued.
	CreatedAt time.Time

	// UpdatedAt is when the job's state last changed.
	UpdatedAt time.Time

	// RunAt is the earliest time the job is eligible to run. For a
	// retrying job it is when the next attempt is due.
	RunAt time.Time

	// Timeout bounds a single attempt. Zero means no per-attempt timeout.
	Timeout time.Duration

	// IdempotencyKey is the key this job was enqueued under, if any.
	IdempotencyKey string
}

// Attempt is the 1-based number of the attempt the job is on: 1 before any
// retry, 2 during the first retry, and so on.
func (j *Job) Attempt() int { return j.RetryCount + 1 }

// AttemptsLeft is how many further attempts the job has after the current
// one, or 0 once its retries are spent.
func (j *Job) AttemptsLeft() int {
	if n := j.MaxRetries - j.RetryCount; n > 0 {
		return n
	}
	return 0
}

// Decode unmarshals the job's payload as JSON into v. It is the mirror of
// EnqueueJSON, for callers whose payloads are JSON documents.
func (j *Job) Decode(v any) error {
	if len(j.Payload) == 0 {
		return fmt.Errorf("client: job %s has an empty payload", j.ID)
	}
	if err := json.Unmarshal(j.Payload, v); err != nil {
		return fmt.Errorf("client: decode job %s payload: %w", j.ID, err)
	}
	return nil
}

// jobDTO is the wire representation of a job, matching the server's JSON
// exactly. It is kept unexported so the timeout-in-seconds encoding stays
// an implementation detail of this package.
type jobDTO struct {
	ID             string    `json:"id"`
	Queue          string    `json:"queue"`
	Payload        []byte    `json:"payload,omitempty"`
	Status         string    `json:"status"`
	Priority       int       `json:"priority"`
	MaxRetries     int       `json:"max_retries"`
	RetryCount     int       `json:"retry_count"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	RunAt          time.Time `json:"run_at"`
	TimeoutSeconds int       `json:"timeout_seconds,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
}

// job converts the wire representation into the exported one.
func (d jobDTO) job() *Job {
	return &Job{
		ID:             d.ID,
		Queue:          d.Queue,
		Payload:        d.Payload,
		Status:         Status(d.Status),
		Priority:       Priority(d.Priority),
		MaxRetries:     d.MaxRetries,
		RetryCount:     d.RetryCount,
		LastError:      d.LastError,
		CreatedAt:      d.CreatedAt,
		UpdatedAt:      d.UpdatedAt,
		RunAt:          d.RunAt,
		Timeout:        time.Duration(d.TimeoutSeconds) * time.Second,
		IdempotencyKey: d.IdempotencyKey,
	}
}
