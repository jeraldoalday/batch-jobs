package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// Pool runs a fixed number of goroutines ("workers") that pull jobs from a
// queue.Queue and execute them with a Handler. A Pool must not be copied
// after Start has been called.
type Pool struct {
	// Queue is where workers pull jobs from. Required.
	Queue queue.Queue

	// Handler processes each dequeued job. Required.
	Handler Handler

	// Concurrency is the number of worker goroutines to run. Defaults to
	// 1 if unset.
	Concurrency int

	// Backoff computes the delay before a failed job's next attempt.
	// Defaults to ExponentialBackoff(time.Second, time.Minute) if unset.
	Backoff BackoffFunc

	// DeadLetter, if set, receives jobs that have exhausted their
	// retries so they can be inspected or replayed later. If nil, such
	// jobs are logged and dropped.
	DeadLetter queue.Queue

	// Logger is used for operational logging. Defaults to slog.Default()
	// if unset.
	Logger *slog.Logger

	// Metrics, if set, records Prometheus-compatible counters and
	// duration histograms for each job processed. A nil Metrics (the
	// default) disables instrumentation.
	Metrics *metrics.Metrics

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// Start launches Concurrency worker goroutines that pull from Queue until
// ctx is cancelled or Stop is called. Start returns immediately; use Stop
// or wait on ctx to know when workers have actually exited.
func (p *Pool) Start(ctx context.Context) {
	if p.Concurrency <= 0 {
		p.Concurrency = 1
	}
	if p.Backoff == nil {
		p.Backoff = ExponentialBackoff(time.Second, time.Minute)
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.wg.Add(p.Concurrency)
	for i := 0; i < p.Concurrency; i++ {
		go func(id int) {
			defer p.wg.Done()
			p.loop(ctx, id)
		}(i)
	}

	p.Logger.Info("worker: pool started", "concurrency", p.Concurrency)
}

// Stop signals all workers to stop pulling new jobs and blocks until every
// in-flight job handler has returned.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	if p.Logger != nil {
		p.Logger.Info("worker: pool stopped")
	}
}

func (p *Pool) loop(ctx context.Context, id int) {
	for {
		j, err := p.Queue.Dequeue(ctx)
		if err != nil {
			return
		}
		p.process(ctx, j)
	}
}

func (p *Pool) process(ctx context.Context, j *job.Job) {
	start := time.Now()
	p.Metrics.IncJobsInFlight(j.Queue)
	attemptCtx, cancel := p.attemptContext(ctx, j)
	err := p.runHandler(attemptCtx, j)
	cancel()
	p.Metrics.DecJobsInFlight(j.Queue)
	if err == nil {
		j.MarkSucceeded(time.Now().UTC())
		duration := time.Since(start)
		p.Metrics.ObserveJob(j.Queue, j.Status, duration)
		p.Logger.Debug("worker: job succeeded", "job_id", j.ID, "queue", j.Queue, "duration", duration)
		return
	}

	if isPermanent(err) {
		j.MarkFailedPermanently(time.Now().UTC(), err)
		p.Metrics.ObserveJob(j.Queue, j.Status, time.Since(start))
		p.Logger.Warn("worker: job failed permanently", "job_id", j.ID, "queue", j.Queue, "error", err)
		return
	}

	backoff := p.Backoff(j.RetryCount + 1)
	j.MarkFailed(time.Now().UTC(), err, backoff)
	p.Metrics.ObserveJob(j.Queue, j.Status, time.Since(start))

	switch j.Status {
	case job.StatusRetrying:
		if reErr := p.Queue.Enqueue(j); reErr != nil {
			p.Logger.Error("worker: failed to reschedule job", "job_id", j.ID, "error", reErr)
		} else {
			p.Logger.Debug("worker: job scheduled for retry", "job_id", j.ID, "queue", j.Queue, "attempt", j.RetryCount, "run_at", j.RunAt)
		}
	case job.StatusDead:
		p.Logger.Warn("worker: job exhausted retries", "job_id", j.ID, "queue", j.Queue, "error", err)
		if p.DeadLetter != nil {
			j.RunAt = time.Now().UTC()
			if dlErr := p.DeadLetter.Enqueue(j); dlErr != nil {
				p.Logger.Error("worker: failed to enqueue dead letter", "job_id", j.ID, "error", dlErr)
			}
		}
	}
}

// attemptContext returns the context to run j's handler with, applying j's
// per-attempt Timeout on top of ctx when one is set, plus a cancel func the
// caller must invoke once the attempt finishes (whether or not the timeout
// was reached) to release the timer. When Timeout is unset, cancel is a
// no-op and ctx is returned unchanged.
func (p *Pool) attemptContext(ctx context.Context, j *job.Job) (context.Context, context.CancelFunc) {
	if j.Timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, j.Timeout)
}

// runHandler invokes the handler, converting a panic into an error so a
// single bad job cannot crash a worker goroutine.
func (p *Pool) runHandler(ctx context.Context, j *job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("worker: panic in handler: %v", r)
		}
	}()
	return p.Handler(ctx, j)
}
