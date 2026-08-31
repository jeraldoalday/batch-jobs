package batchjobs

import (
	"context"
	"fmt"
	"time"
)

// Prune deletes finished jobs that Config.Retention has expired, and
// reports what it removed.
//
// With PruneInterval left at its default the Engine already does this on
// its own once started, so Prune is for the cases a timer does not cover:
// an admin endpoint, a maintenance command, a deployment that would rather
// prune from one place than from every process holding the queue (set
// PruneInterval negative and call this), or a test that needs the pass to
// have happened before it looks.
//
// It returns an error wrapping ErrNoRetention if no policy is configured.
// Deleting job history is not something to infer from a call with no
// policy behind it.
//
// A pass deletes in batches, giving up the database's write lock between
// them, so it slows the queue rather than stopping it. It is safe to call
// while workers are running and safe to call concurrently with the
// Engine's own sweeps -- it only ever touches jobs that have finished.
func (e *Engine) Prune(ctx context.Context) (Pruned, error) {
	if err := checkContext(ctx); err != nil {
		return Pruned{}, err
	}
	if !e.cfg.Retention.Enabled() {
		return Pruned{}, fmt.Errorf("batchjobs: prune: %w", ErrNoRetention)
	}

	pruned, err := e.queue.Prune(e.cfg.Retention.internal())
	if err != nil {
		// The counts are returned alongside the error rather than
		// discarded: a pass that failed on its third batch really did
		// delete the first two, and a caller reporting what happened
		// should say so.
		return fromPruned(pruned), fmt.Errorf("batchjobs: prune: %w", err)
	}
	return fromPruned(pruned), nil
}

// Retention returns the policy this Engine was opened with. It is the
// zero value -- keep everything -- unless Config.Retention was set.
func (e *Engine) Retention() Retention { return e.cfg.Retention }

// Retained reports how many finished jobs the queue is currently holding:
// the size of the history a retention policy governs, and the number that
// grows without bound when there is no policy.
//
// It is exposed as batchjobs_retained_jobs on the metrics handler as well,
// which is the better place to watch it from; this is for a program that
// wants the number itself -- a health check, an admin page, a decision
// about whether to prune now.
func (e *Engine) Retained() (int, error) {
	n, err := e.queue.Retained()
	if err != nil {
		return 0, fmt.Errorf("batchjobs: retained: %w", err)
	}
	return n, nil
}

// OldestRetained returns when the oldest finished job still held reached
// its terminal status, or the zero time if the queue is holding no
// finished jobs at all.
//
// It answers what the count cannot. A queue whose oldest succeeded job
// finished three weeks ago under a two-day policy is not being pruned,
// and the count will not say so if the queue has also gone quiet.
func (e *Engine) OldestRetained() (time.Time, error) {
	oldest, err := e.queue.OldestRetained()
	if err != nil {
		return time.Time{}, fmt.Errorf("batchjobs: oldest retained: %w", err)
	}
	return oldest, nil
}
