package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// Pool runs a set of goroutines ("workers") that pull jobs from a
// queue.Queue and execute them with a Handler. The set is a fixed size by
// default; setting Scale.Max turns it into an autoscaling pool that tracks
// its own backlog. A Pool must not be copied after Start has been called.
type Pool struct {
	// Queue is where workers pull jobs from. Required.
	Queue queue.Queue

	// Handler processes each dequeued job. Required.
	Handler Handler

	// Name labels this pool in metrics, so a process running several
	// pools can tell their worker counts apart. Defaults to "default".
	Name string

	// Concurrency is the number of worker goroutines to run. Defaults to
	// 1 if unset. When autoscaling is enabled it is the pool's starting
	// size, clamped into the [Scale.Min, Scale.Max] range; otherwise it
	// is the pool's size for its whole lifetime.
	Concurrency int

	// Scale, if its Max is positive, enables autoscaling: a background
	// goroutine samples the pool's workload every ScaleInterval and
	// resizes the worker set to match, within [Min, Max]. The zero value
	// leaves the pool at a fixed Concurrency.
	Scale Policy

	// ScaleInterval is how often an autoscaling pool samples its workload.
	// Defaults to DefaultScaleInterval. Ignored unless Scale.Max is set.
	ScaleInterval time.Duration

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

	// mu guards the worker set below. It is held only for bookkeeping —
	// spawning and retiring — never while a handler runs.
	mu      sync.Mutex
	workers []*workerHandle
	nextID  int

	// busy counts workers currently inside a handler, as opposed to
	// blocked in Dequeue waiting for one. It is read by the autoscaler
	// from its own goroutine, so it is atomic rather than guarded by mu.
	busy atomic.Int64

	// poolCtx is the context Start was given, cancelled by Stop. Handlers
	// run under it, so cancelling it interrupts in-flight work; each
	// worker additionally gets its own child context used solely for
	// Dequeue, so retiring one worker cannot disturb another's job.
	poolCtx context.Context

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// workerHandle is the pool's record of one running worker goroutine.
type workerHandle struct {
	// id identifies the worker in log lines.
	id int

	// retire cancels the worker's Dequeue context, so it stops asking for
	// new jobs and exits. It deliberately does not touch the context the
	// worker's current handler is running under: a retired worker
	// finishes the job in its hands before going away.
	retire context.CancelFunc
}

// Start launches the pool's worker goroutines, which pull from Queue until
// ctx is cancelled or Stop is called, plus the autoscaler goroutine if
// Scale.Max is set. Start returns immediately; use Stop or wait on ctx to
// know when workers have actually exited.
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

	if p.ScaleInterval <= 0 {
		p.ScaleInterval = DefaultScaleInterval
	}
	if p.Name == "" {
		p.Name = DefaultPoolName
	}

	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.poolCtx = ctx

	initial := p.initialSize()
	p.scaleTo(initial)
	p.Metrics.SetPoolWorkers(p.Name, initial)

	if p.autoscaling() {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.autoscale(ctx)
		}()
		p.Logger.Info("worker: pool started", "concurrency", initial,
			"min", p.Scale.normalized().Min, "max", p.Scale.normalized().Max)
		return
	}

	p.Logger.Info("worker: pool started", "concurrency", initial)
}

// Stop signals all workers to stop pulling new jobs and blocks until every
// in-flight job handler has returned.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()

	p.mu.Lock()
	p.workers = nil
	p.mu.Unlock()
	p.Metrics.SetPoolWorkers(p.Name, 0)

	if p.Logger != nil {
		p.Logger.Info("worker: pool stopped")
	}
}

// loop is one worker goroutine. It pulls jobs using dequeueCtx, which is
// cancelled when this particular worker is retired or the pool stops, and
// runs each handler under ctx, the pool-wide context, so retiring a worker
// never cancels the job it is already running.
func (p *Pool) loop(ctx, dequeueCtx context.Context, id int) {
	for {
		j, err := p.Queue.Dequeue(dequeueCtx)
		if err != nil {
			return
		}
		p.busy.Add(1)
		p.process(ctx, j)
		p.busy.Add(-1)
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
