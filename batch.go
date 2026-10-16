package batchjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// Submission describes one job in a batch: the queue it belongs on, its
// payload, and any per-job options.
type Submission struct {
	// Queue is the queue name the job belongs on. Required.
	Queue string

	// Payload is the job's body, passed to the handler untouched. Use
	// JSON to fill it from a value.
	Payload []byte

	// Options are the same per-job options Enqueue accepts --
	// WithPriority, WithDelay, WithIdempotencyKey, and the rest.
	Options []Option
}

// JSON returns a Submission whose payload is v marshalled as JSON, the
// batch counterpart of EnqueueJSON.
func JSON(name string, v any, opts ...Option) (Submission, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return Submission{}, fmt.Errorf("batchjobs: encode payload for queue %q: %w", name, err)
	}
	return Submission{Queue: name, Payload: payload, Options: opts}, nil
}

// EnqueueBatch submits every job in one round trip and returns them as
// stored, in the order given.
//
// It exists because the cost of a durable enqueue is dominated by the
// transaction around it rather than by the row: submitting a hundred jobs
// one at a time costs about 83us each, and in a single batch about 28us.
// A fan-out, an import, or a schedule expansion should not pay the
// difference.
//
// The batch is atomic: either every job is stored or none is, so a caller
// that gets an error can retry the whole thing without worrying about
// which half went through. Idempotency keys work across the batch and
// within it, so a job whose key already exists comes back describing the
// original rather than creating a duplicate -- including when the
// duplicate is another entry in the same call.
//
// An empty batch is a no-op success.
func (e *Engine) EnqueueBatch(ctx context.Context, subs []Submission) ([]*Job, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if len(subs) == 0 {
		return nil, nil
	}

	jobs := make([]*job.Job, len(subs))
	for i, sub := range subs {
		if sub.Queue == "" {
			return nil, fmt.Errorf("batchjobs: enqueue batch: entry %d has no queue name", i)
		}
		jobs[i] = build(sub.Queue, sub.Payload, sub.Options)
	}

	// The error is wrapped rather than translated, so the sentinels a
	// caller already matches on after Enqueue -- ErrFull above all --
	// still match here.
	if err := queue.EnqueueBatch(e.queue, jobs); err != nil {
		return nil, fmt.Errorf("batchjobs: enqueue batch of %d: %w", len(subs), err)
	}

	return convertAll(jobs), nil
}

// EnqueueBatchJSON is EnqueueBatch for the common case of a slice of
// values that all go on the same queue with the same options.
//
// Options apply to every job, so WithIdempotencyKey is rejected rather than
// honored: one key shared across many jobs would collapse them all into
// the first rather than deduplicating anything. Give each value its own
// key with EnqueueBatch instead.
func (e *Engine) EnqueueBatchJSON(ctx context.Context, name string, values []any, opts ...Option) ([]*Job, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.idempotencyKey != "" {
		return nil, errors.New("batchjobs: EnqueueBatchJSON: WithIdempotencyKey applies to every value, which would collapse them all into the first; use EnqueueBatch to give each one its own key")
	}

	subs := make([]Submission, len(values))
	for i, v := range values {
		sub, err := JSON(name, v, opts...)
		if err != nil {
			return nil, err
		}
		subs[i] = sub
	}
	return e.EnqueueBatch(ctx, subs)
}
