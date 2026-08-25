package sqlite

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/metrics"
)

// DefaultReapInterval is how often a Reaper sweeps for expired leases when
// Interval is unset. It is well under DefaultLeaseTTL so an orphaned job is
// typically reclaimed within a fraction of a lease past its expiry, rather
// than waiting out a full second TTL.
const DefaultReapInterval = 15 * time.Second

// Reaper periodically calls Recover on a Queue, so orphaned jobs come back
// on their own rather than only when a process happens to restart. A Reaper
// must not be copied after Start has been called.
//
// Running one is safe in every process that shares the database, and
// running none is safe too: Recover is idempotent and set-based, so
// duplicate reapers race harmlessly, and a deployment that would rather
// recover explicitly can call Recover itself.
type Reaper struct {
	// Queue is the queue whose expired leases are reclaimed. Required.
	Queue *Queue

	// Interval is how often to sweep. Defaults to DefaultReapInterval.
	Interval time.Duration

	// OptimizeInterval is how often the reaper refreshes the table
	// statistics the query planner uses to choose a dequeue index,
	// defaulting to DefaultOptimizeInterval. A negative value turns the
	// refresh off, leaving statistics to whatever Open seeded; see
	// Queue.Optimize for what that costs.
	OptimizeInterval time.Duration

	// Logger is used for operational logging. Defaults to slog.Default().
	Logger *slog.Logger

	// Metrics, if set, counts reclaimed jobs by outcome. A nil Metrics
	// (the default) disables instrumentation.
	Metrics *metrics.Metrics

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// Start launches the sweep loop, which runs until ctx is cancelled or Stop
// is called. Start returns immediately; use Stop or wait on ctx to know
// when the loop has actually exited.
//
// The first sweep happens immediately rather than one Interval later,
// because the most valuable sweep a process ever runs is the one at
// startup: if this process is replacing one that crashed, its jobs are
// sitting orphaned right now.
func (r *Reaper) Start(ctx context.Context) {
	if r.Interval <= 0 {
		r.Interval = DefaultReapInterval
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.loop(ctx)
	}()

	r.Logger.Info("sqlite: reaper started", "interval", r.Interval, "lease_ttl", r.Queue.LeaseTTL())
}

// Stop signals the sweep loop to stop and blocks until it exits, letting a
// sweep already in progress finish first.
func (r *Reaper) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	if r.Logger != nil {
		r.Logger.Info("sqlite: reaper stopped")
	}
}

func (r *Reaper) loop(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	// Statistics maintenance rides along on the reaper because it is the
	// one background loop every process running this queue already has,
	// and because both are the same kind of chore: cheap, idempotent, and
	// harmless to run from several processes at once. It gets its own
	// ticker rather than a counter of sweeps so that tuning the reap
	// interval does not silently retune it as well.
	optimize := newOptionalTicker(r.OptimizeInterval, DefaultOptimizeInterval)
	defer optimize.Stop()

	r.sweep()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweep()
		case <-optimize.C:
			if err := r.Queue.Optimize(); err != nil {
				r.Logger.Warn("sqlite: statistics refresh failed", "error", err)
			}
		}
	}
}

// optionalTicker is a time.Ticker whose channel may be nil, so a disabled
// periodic task costs a select case that never fires rather than a
// separate code path.
type optionalTicker struct {
	C      <-chan time.Time
	ticker *time.Ticker
}

// newOptionalTicker returns a ticker at interval, or at fallback when
// interval is zero, or a never-firing one when interval is negative.
func newOptionalTicker(interval, fallback time.Duration) optionalTicker {
	if interval < 0 {
		return optionalTicker{}
	}
	if interval == 0 {
		interval = fallback
	}
	t := time.NewTicker(interval)
	return optionalTicker{C: t.C, ticker: t}
}

// Stop releases the underlying ticker, if there is one.
func (t optionalTicker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
}

// sweep runs one recovery pass, logging and counting what it reclaimed.
// Errors are logged rather than returned: a reaper that gave up on the
// first transient failure would leave the very jobs it exists to rescue
// stranded until the next restart.
func (r *Reaper) sweep() {
	rec, err := r.Queue.Recover()
	if err != nil {
		r.Logger.Error("sqlite: lease recovery failed", "error", err)
		return
	}

	r.Metrics.ObserveRecovery(rec.Requeued, rec.DeadLettered)
	if rec.Total() == 0 {
		return
	}

	// Anything reclaimed means a worker died holding work, which is worth
	// a line in the log at warn: the queue heals itself, but the crash
	// that caused it does not show up anywhere else.
	r.Logger.Warn("sqlite: recovered orphaned jobs",
		"requeued", rec.Requeued, "dead_lettered", rec.DeadLettered)
}
