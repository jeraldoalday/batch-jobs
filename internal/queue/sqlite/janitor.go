package sqlite

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// DefaultPruneInterval is how often a Janitor prunes when Interval is
// unset.
//
// It is hours rather than seconds because retention is measured in days:
// sweeping more often would only delete the same rows a little sooner
// while paying for the scan every time. The interval bounds how far past
// its policy the history can drift, and an hour of drift on a day-long
// policy is not worth optimising.
const DefaultPruneInterval = time.Hour

// Janitor periodically prunes a Queue's finished jobs according to a
// retention policy, so history is bounded by the policy rather than by
// whether anyone remembered to run a cleanup. A Janitor must not be copied
// after Start has been called.
//
// Unlike the Reaper, which every process sharing a database can safely
// run, a Janitor is best run in one place. Several are still *correct* --
// pruning is idempotent and set-based, and two passes racing simply find
// each other's rows already gone -- but they duplicate the deletes and the
// write-lock pressure that goes with them for no benefit, since one pass
// already removes everything eligible.
type Janitor struct {
	// Queue is the queue whose finished jobs are pruned. Required.
	Queue *Queue

	// Retention is the policy applied on every sweep. Required: Start
	// refuses a policy that would delete nothing, since a janitor with
	// nothing to do is a configuration mistake rather than a valid
	// no-op.
	Retention queue.Retention

	// Interval is how often to prune. Defaults to DefaultPruneInterval.
	Interval time.Duration

	// Logger is used for operational logging. Defaults to slog.Default().
	Logger *slog.Logger

	// Metrics, if set, counts pruned jobs by status. A nil Metrics (the
	// default) disables instrumentation.
	Metrics *metrics.Metrics

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// Start launches the prune loop, which runs until ctx is cancelled or Stop
// is called. Start returns as soon as the loop is running; it returns an
// error without starting anything if the policy is invalid or would delete
// nothing.
//
// The first sweep happens one interval in, not immediately -- the opposite
// of the Reaper, and for the opposite reason. A reaper's most valuable
// sweep is the one at startup, because a process starting up may be
// replacing one that crashed with jobs still held. Nothing about deleting
// month-old rows is urgent, and startup is exactly when the process has
// better things to do with its single write lock than compete with the
// backlog it is trying to pick up.
func (jt *Janitor) Start(ctx context.Context) error {
	if err := jt.Retention.Validate(); err != nil {
		return err
	}
	if !jt.Retention.Enabled() {
		return queue.ErrNoRetention
	}

	if jt.Interval <= 0 {
		jt.Interval = DefaultPruneInterval
	}
	if jt.Logger == nil {
		jt.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(ctx)
	jt.cancel = cancel

	jt.wg.Add(1)
	go func() {
		defer jt.wg.Done()
		jt.loop(ctx)
	}()

	jt.Logger.Info("sqlite: janitor started", "interval", jt.Interval, "retention", jt.Retention.String())
	return nil
}

// Stop signals the prune loop to stop and blocks until it exits, letting a
// sweep already in progress finish first.
func (jt *Janitor) Stop() {
	if jt.cancel != nil {
		jt.cancel()
	}
	jt.wg.Wait()
	if jt.Logger != nil {
		jt.Logger.Info("sqlite: janitor stopped")
	}
}

func (jt *Janitor) loop(ctx context.Context) {
	ticker := time.NewTicker(jt.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !jt.sweep() {
				return
			}
		}
	}
}

// sweep runs one prune pass, reporting whether the loop should continue.
//
// Errors are logged rather than returned, for the same reason the reaper
// logs its own: a janitor that gave up on one transient failure would
// leave the history growing until someone noticed. The exception is a
// closed queue, which is not transient -- there is no pass that will ever
// succeed again, and retrying it hourly would only fill the log.
func (jt *Janitor) sweep() bool {
	pruned, err := jt.Queue.Prune(jt.Retention)
	if err != nil {
		if errors.Is(err, queue.ErrClosed) {
			jt.Logger.Debug("sqlite: janitor stopping; queue is closed")
			return false
		}
		jt.Logger.Error("sqlite: prune failed", "error", err, "pruned", pruned.String())
		// Even a failed pass usually deleted something before it
		// failed, and those deletions are real. Count them.
	}

	for _, status := range queue.TerminalStatuses {
		jt.Metrics.ObservePruned(status, pruned.Get(status))
	}

	if pruned.Total() > 0 {
		jt.Logger.Info("sqlite: pruned finished jobs", "total", pruned.Total(), "detail", pruned.String())
	}
	return true
}
