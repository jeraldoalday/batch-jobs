package worker

import (
	"context"
	"errors"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// DefaultHeartbeatDivisor sets the pool's heartbeat interval to a fraction
// of the queue's lease TTL when HeartbeatInterval is unset. Three means a
// worker can miss two consecutive heartbeats -- to a slow query, a stalled
// disk, a scheduling hiccup -- before its lease lapses and its job is
// handed to somebody else.
const DefaultHeartbeatDivisor = 3

// Heartbeater is implemented by queues that hand out time-bounded claims,
// such as queue/sqlite.Queue. A pool pulling from one keeps the claim on
// the job it is running alive for as long as it is actually running it, so
// the lease can be short enough to recover a crashed worker's jobs quickly
// without cutting a slow job short.
//
// A queue that does not implement this interface is used exactly as before:
// heartbeating is opt-in by capability, not configuration.
type Heartbeater interface {
	// Heartbeat extends the claim on the job with the given ID. It
	// returns an error matching queue.ErrLeaseLost if the caller no
	// longer holds the claim.
	Heartbeat(id string) error

	// LeaseTTL is how long a claim survives without a heartbeat, or 0 if
	// the queue does not expire claims.
	LeaseTTL() time.Duration
}

// heartbeat keeps j's claim alive while its handler runs.
//
// It returns a context to run the handler under and a stop function the
// caller must call when the attempt finishes. The returned context is
// cancelled if the lease is lost mid-flight: another worker may already be
// running this job, so the honest thing to do is interrupt the handler
// rather than let two attempts race to report an outcome. Handlers that
// ignore cancellation are unaffected, exactly as they are with job
// timeouts.
//
// When the queue does not lease, this is a no-op returning ctx unchanged.
func (p *Pool) heartbeat(ctx context.Context, j *job.Job) (context.Context, func()) {
	hb, ok := p.Queue.(Heartbeater)
	if !ok {
		return ctx, func() {}
	}
	interval := p.heartbeatInterval(hb)
	if interval <= 0 {
		return ctx, func() {}
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := hb.Heartbeat(j.ID); err != nil && p.heartbeatFatal(j, err) {
					cancel()
					return
				}
			}
		}
	}()

	return ctx, func() {
		cancel()
		<-done
	}
}

// heartbeatFatal reports whether a failed heartbeat means the job is no
// longer this worker's to run. Losing the lease -- or finding the job gone
// entirely -- is fatal to the attempt. Anything else, a busy database or a
// transient I/O error, is logged and the loop keeps trying: the lease has
// not necessarily lapsed yet, and abandoning a healthy job over one failed
// query would turn a blip into a lost attempt, while giving up on
// heartbeating would guarantee the lease lapses later.
func (p *Pool) heartbeatFatal(j *job.Job, err error) bool {
	if errors.Is(err, queue.ErrLeaseLost) || errors.Is(err, queue.ErrNotFound) {
		p.Logger.Warn("worker: lost lease on running job, interrupting",
			"job_id", j.ID, "queue", j.Queue, "error", err)
		return true
	}
	p.Logger.Error("worker: heartbeat failed", "job_id", j.ID, "queue", j.Queue, "error", err)
	return false
}

// heartbeatInterval is how often to extend a claim: the configured value if
// there is one, otherwise a fraction of the queue's own lease TTL, so the
// two cannot drift apart when the TTL is tuned.
func (p *Pool) heartbeatInterval(hb Heartbeater) time.Duration {
	if p.HeartbeatInterval > 0 {
		return p.HeartbeatInterval
	}
	ttl := hb.LeaseTTL()
	if ttl <= 0 {
		return 0
	}
	return ttl / DefaultHeartbeatDivisor
}
