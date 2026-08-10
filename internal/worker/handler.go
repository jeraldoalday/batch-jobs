// Package worker implements a pool of goroutines that pull jobs from a
// queue.Queue and execute them concurrently.
package worker

import (
	"context"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// Handler processes a single job. Returning an error causes the job to be
// retried, subject to its MaxRetries, or moved to the pool's dead-letter
// queue once retries are exhausted.
type Handler func(ctx context.Context, j *job.Job) error
