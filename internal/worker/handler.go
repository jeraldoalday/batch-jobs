// Package worker implements a pool of goroutines that pull jobs from a
// queue.Queue and execute them concurrently.
package worker

import (
	"context"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// Handler processes a single job. Returning an error causes the job to be
// retried, subject to its MaxRetries, or moved to the pool's dead-letter
// queue once retries are exhausted. Wrapping the returned error with
// Permanent skips retrying and dead-lettering altogether, marking the job
// StatusFailed immediately — use this when the handler knows the error is
// not transient.
//
// If j.Timeout is non-zero, ctx is cancelled once that duration has
// elapsed; a well-behaved Handler should respect ctx.Done() so a stuck
// attempt is actually interrupted rather than merely reported late.
type Handler func(ctx context.Context, j *job.Job) error
