package batchjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// checkContext rejects a call whose context is already done, so a caller
// whose request was cancelled does not still write a job to the queue.
//
// It is a pre-flight check, not interruption: the queue's operations are
// synchronous local database calls that run to completion once started.
// Every method here takes a context anyway, for the caller's sake -- an
// engine method should not be the one call in a request path that cannot
// be passed the request's context.
func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("batchjobs: nil context")
	}
	return ctx.Err()
}

// Enqueue submits a job to the named queue and returns it as stored,
// including its assigned ID.
//
// The job is durable the moment Enqueue returns: it survives a restart
// whether or not this Engine is running workers, and whether or not any
// handler for the queue is registered yet.
//
// With WithIdempotencyKey, an enqueue whose key already exists for the
// queue returns the original job rather than creating another; compare the
// returned ID, or use EnqueueResult, to tell which happened.
func (e *Engine) Enqueue(ctx context.Context, name string, payload []byte, opts ...Option) (*Job, error) {
	j, _, err := e.EnqueueResult(ctx, name, payload, opts...)
	return j, err
}

// EnqueueResult is Enqueue, additionally reporting whether a new job was
// created (true) or an idempotency key matched one already on file (false).
func (e *Engine) EnqueueResult(ctx context.Context, name string, payload []byte, opts ...Option) (*Job, bool, error) {
	if name == "" {
		return nil, false, errors.New("batchjobs: queue name is required")
	}
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}

	j := build(name, payload, opts)
	submitted := j.ID

	if err := e.queue.Enqueue(j); err != nil {
		return nil, false, fmt.Errorf("batchjobs: enqueue on %q: %w", name, err)
	}

	// A queue that deduplicates rewrites the job in place with the
	// original's fields, ID included, so a changed ID is the signal that
	// nothing new was created.
	return convert(j), j.ID == submitted, nil
}

// EnqueueJSON marshals v as the job's payload and enqueues it, for the
// common case of a JSON document. Job.Decode is its mirror in the handler.
func (e *Engine) EnqueueJSON(ctx context.Context, name string, v any, opts ...Option) (*Job, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: encode payload for queue %q: %w", name, err)
	}
	return e.Enqueue(ctx, name, payload, opts...)
}

// Get returns the job with the given ID, or an error wrapping ErrNotFound.
func (e *Engine) Get(ctx context.Context, id string) (*Job, error) {
	if id == "" {
		return nil, errors.New("batchjobs: job ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	j, err := e.queue.Get(id)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: get job %s: %w", id, err)
	}
	return convert(j), nil
}

// List returns up to limit jobs in the given status, most recently updated
// first. It is how a program inspects its own dead letters -- listing
// StatusDead and deciding what to revive.
func (e *Engine) List(ctx context.Context, status Status, limit int) ([]*Job, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if !status.Valid() {
		return nil, fmt.Errorf("batchjobs: unknown status %q", status)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("batchjobs: limit %d must be > 0", limit)
	}

	jobs, err := e.queue.List(job.Status(status), limit)
	if err != nil {
		return nil, fmt.Errorf("batchjobs: list %s jobs: %w", status, err)
	}
	return convertAll(jobs), nil
}

// Cancel stops a job that has not started yet, returning an error wrapping
// ErrNotFound if the job is unknown or ErrNotCancelable if a worker already
// has it or it has already finished.
func (e *Engine) Cancel(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("batchjobs: job ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := e.queue.Cancel(id); err != nil {
		return fmt.Errorf("batchjobs: cancel job %s: %w", id, err)
	}
	return nil
}

// RetryJob returns a dead or failed job to the queue with its retry count
// and last error cleared, so it is attempted again from scratch. It returns
// an error wrapping ErrNotFound if the job is unknown, or ErrNotRetryable
// if it is in any other state.
//
// The name says "Job" because Retry alone would read like a configuration
// knob for the retry policy, which sits on Config.
func (e *Engine) RetryJob(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("batchjobs: job ID is required")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := e.queue.Retry(id); err != nil {
		return fmt.Errorf("batchjobs: retry job %s: %w", id, err)
	}
	return nil
}

// Stats is the number of jobs in each status. Statuses with no jobs are
// absent rather than zero, so read it with Get.
type Stats map[Status]int

// Get returns the number of jobs in the given status, or zero.
func (s Stats) Get(status Status) int { return s[status] }

// Total is how many jobs the queue holds across every status.
func (s Stats) Total() int {
	total := 0
	for _, n := range s {
		total += n
	}
	return total
}

// Active is how many jobs are still on their way to a terminal state:
// waiting, running, or scheduled for another attempt. It is what answers
// "is there anything left to do?", which Total does not -- a queue that has
// succeeded a million jobs has a large Total and nothing to do.
func (s Stats) Active() int {
	return s[StatusPending] + s[StatusRunning] + s[StatusRetrying]
}

// Backlog is how many jobs are waiting for a worker, excluding those
// already running.
func (s Stats) Backlog() int {
	return s[StatusPending] + s[StatusRetrying]
}

// Failing is how many jobs ended badly -- permanently failed or
// dead-lettered -- and will sit there until someone revives or discards
// them. It is the natural thing to alert on.
func (s Stats) Failing() int {
	return s[StatusFailed] + s[StatusDead]
}

// Stats returns the current per-status job counts.
func (e *Engine) Stats(ctx context.Context) (Stats, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	counts, err := e.queue.Stats()
	if err != nil {
		return nil, fmt.Errorf("batchjobs: stats: %w", err)
	}

	stats := make(Stats, len(counts))
	for status, n := range counts {
		stats[Status(status)] = n
	}
	return stats, nil
}

// Len is how many jobs are waiting to run: the backlog a worker pool would
// have to get through. It is a single cheap query, unlike Stats, which
// counts every status.
func (e *Engine) Len() int { return e.queue.Len() }

// Recovery summarizes one pass of reclaiming jobs whose worker died holding
// them.
type Recovery struct {
	// Requeued is how many orphaned jobs went back in the queue.
	Requeued int

	// DeadLettered is how many had no attempts left and were retired.
	DeadLettered int
}

// Total is how many orphaned jobs the pass dealt with, either way.
func (r Recovery) Total() int { return r.Requeued + r.DeadLettered }

// Recover reclaims every job whose lease has expired, without waiting for
// the Engine's next scheduled sweep. A running Engine does this on its own;
// call it directly when waiting is not acceptable -- an operator who has
// just confirmed a host is gone, or a deploy step reclaiming a retired
// instance's work.
func (e *Engine) Recover(ctx context.Context) (Recovery, error) {
	if err := checkContext(ctx); err != nil {
		return Recovery{}, err
	}

	result, err := e.queue.Recover()
	if err != nil {
		return Recovery{}, fmt.Errorf("batchjobs: recover orphaned jobs: %w", err)
	}
	return Recovery{Requeued: result.Requeued, DeadLettered: result.DeadLettered}, nil
}

// ExpiredLeases is how many claims have lapsed without having been
// reclaimed yet. It is normally zero on a running Engine; a number that
// stays high means jobs are being orphaned faster than the sweep clears
// them, which is worth alerting on.
func (e *Engine) ExpiredLeases() (int, error) {
	n, err := e.queue.ExpiredLeases()
	if err != nil {
		return 0, fmt.Errorf("batchjobs: count expired leases: %w", err)
	}
	return n, nil
}
