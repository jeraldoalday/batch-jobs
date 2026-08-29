package batchjobs

import (
	"errors"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
	"github.com/jeraldoalday/batch-jobs/internal/worker"
)

// The errors an Engine's callers branch on. Each is the same sentinel the
// engine itself uses, so errors.Is works through every layer without this
// package re-wrapping anything.
var (
	// ErrNotFound reports that no job with the given ID exists.
	ErrNotFound = queue.ErrNotFound

	// ErrNotCancelable reports that the job exists but has already
	// started or already finished. Losing that race is an outcome, not a
	// fault: by the time a cancel arrives, a worker may well have the job.
	ErrNotCancelable = queue.ErrNotCancelable

	// ErrNotRetryable reports that the job exists but is not dead or
	// failed, so there is nothing to revive.
	ErrNotRetryable = queue.ErrNotRetryable

	// ErrFull reports that a capacity-limited queue has as much newly
	// submitted work as it will hold. It is backpressure: the enqueue was
	// well-formed and will likely succeed once workers drain the backlog.
	ErrFull = queue.ErrFull

	// ErrClosed reports that the Engine's queue has been closed.
	ErrClosed = queue.ErrClosed

	// ErrNoHandler reports that a job's queue has no registered handler
	// and no fallback was set. A handler returning it is unusual; it
	// normally arrives as a job's LastError, naming the queue whose
	// registration is missing.
	ErrNoHandler = worker.ErrNoHandler

	// ErrScheduleNotFound reports that no schedule with the given ID
	// exists. It is deliberately distinct from ErrNotFound: a caller
	// holding both an ID for a job and an ID for a schedule should not
	// have one lookup's miss look like the other's.
	ErrScheduleNotFound = schedule.ErrNotFound

	// ErrSchedulingDisabled reports that a schedule method was called on
	// an Engine opened without Config.Schedules. It is a wiring mistake
	// rather than a runtime condition -- the answer is to turn the
	// feature on, not to retry -- so it is reported rather than the
	// Engine quietly opening a schedule database nobody asked for.
	ErrSchedulingDisabled = errors.New("batchjobs: scheduling is disabled; set Config.Schedules")
)

// Permanent marks an error from a Handler as one that retrying can never
// fix -- a payload that will not parse, a record that does not exist, a
// request the remote end rejected as invalid. The job is failed
// immediately, skipping both its remaining retries and the dead-letter
// step, and lands in StatusFailed rather than StatusDead.
//
// The distinction is what makes a dead-letter queue useful: StatusDead
// means "we tried and could not get through", worth retrying after
// whatever was broken is fixed, while StatusFailed means "this job is
// wrong", worth a human reading it.
//
// Permanent(nil) is nil, so wrapping a call's result unconditionally is
// safe:
//
//	return batchjobs.Permanent(json.Unmarshal(j.Payload, &msg))
func Permanent(err error) error { return worker.Permanent(err) }

// IsPermanent reports whether err was marked with Permanent, at any depth.
// Handlers that compose -- middleware that logs or retries around an inner
// handler -- use it to tell a permanent failure from a transient one
// without unwrapping by hand.
func IsPermanent(err error) bool { return worker.IsPermanent(err) }
