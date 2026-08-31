package queue

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// DefaultPruneBatch is how many rows one prune statement removes when a
// Retention does not say. It is a compromise between the two things that
// go wrong at the extremes: a batch of one pays the cost of a transaction
// per row, while an unbounded delete holds the write lock for as long as
// it takes to remove the whole history, during which nothing can be
// enqueued or claimed.
const DefaultPruneBatch = 1000

// ErrNoRetention is returned by an operation that needs a retention policy
// when none is configured. Keeping everything forever is the default, so
// this is a request to prune arriving at a queue that has not been told
// what may be deleted -- a wiring gap, not a runtime condition.
var ErrNoRetention = errors.New("queue: no retention policy configured")

// TerminalStatuses lists the statuses a job can be pruned from, in the
// order a prune pass works through them.
//
// The order is not arbitrary. Succeeded jobs are almost always the bulk of
// a queue's history and the least interesting, so clearing them first
// reclaims the most space in the earliest batches -- which matters because
// a pass that is cut short (a closing queue, a cancelled context) should
// have done the most useful part of its work already. Dead jobs come last
// for the same reason inverted: they are the rows an operator is most
// likely to still want.
var TerminalStatuses = []job.Status{
	job.StatusSucceeded,
	job.StatusCancelled,
	job.StatusFailed,
	job.StatusDead,
}

// Retention says how long a job's record is kept after it reaches a
// terminal state. A durable queue does not otherwise forget anything: every
// job it has ever run is still a row, so a busy queue's database grows
// without bound until something is allowed to delete from it.
//
// A zero duration means "keep forever", which is the zero value of the
// whole struct and therefore the default. That default is deliberately the
// safe one: a queue that silently began deleting job history the day it was
// upgraded would be a much worse surprise than one that keeps growing at
// the rate it always has.
//
// The durations are per terminal status because they answer different
// questions. Succeeded jobs are an audit trail whose value decays in hours;
// dead jobs are a work list, and deleting one loses the only record that
// the work was ever meant to happen.
type Retention struct {
	// Succeeded is how long a job that ran cleanly is kept.
	Succeeded time.Duration

	// Failed is how long a job that failed permanently is kept. These are
	// jobs someone marked as wrong rather than unlucky, so they are worth
	// keeping longer than successes: the evidence of what was wrong is in
	// the row.
	Failed time.Duration

	// Dead is how long a job that exhausted its retries is kept. Leaving
	// it at zero -- forever -- is the recommendation, because a
	// dead-letter queue is a list of work that still needs doing and
	// pruning it discards the request itself.
	Dead time.Duration

	// Cancelled is how long a job cancelled before it ran is kept.
	Cancelled time.Duration

	// BatchSize bounds how many rows a single delete statement removes,
	// defaulting to DefaultPruneBatch. A pass keeps issuing batches until
	// there is nothing left to delete, so this tunes how long the write
	// lock is held at a time, not how much a pass removes in total.
	BatchSize int
}

// DefaultRetention is a policy suitable for a queue whose history is
// diagnostic rather than authoritative: successes and cancellations last a
// day, permanent failures a week, and dead-lettered jobs forever.
//
// It is offered as a starting point rather than applied by default,
// because only the deployment knows whether its job rows are a debugging
// convenience or a record something else depends on.
func DefaultRetention() Retention {
	return Retention{
		Succeeded: 24 * time.Hour,
		Cancelled: 24 * time.Hour,
		Failed:    7 * 24 * time.Hour,
	}
}

// TTL returns how long a job in the given status is kept, or zero for
// forever. A non-terminal status is always zero: a job that has not
// finished is not history, and no policy may delete it.
func (r Retention) TTL(status job.Status) time.Duration {
	switch status {
	case job.StatusSucceeded:
		return r.Succeeded
	case job.StatusFailed:
		return r.Failed
	case job.StatusDead:
		return r.Dead
	case job.StatusCancelled:
		return r.Cancelled
	default:
		return 0
	}
}

// Enabled reports whether the policy would delete anything at all. A
// disabled policy is not an error -- it is the default -- but it is worth
// distinguishing from a configured one so a caller can decline to start a
// janitor that has nothing to do.
func (r Retention) Enabled() bool {
	for _, status := range TerminalStatuses {
		if r.TTL(status) > 0 {
			return true
		}
	}
	return false
}

// Validate reports whether the policy is usable. A negative duration is
// rejected rather than clamped, because the two readings of it -- "keep
// forever" and "delete immediately" -- are opposites, and guessing wrong
// on a policy that deletes data is not a mistake worth making quietly.
func (r Retention) Validate() error {
	for _, status := range TerminalStatuses {
		if r.TTL(status) < 0 {
			return fmt.Errorf("queue: retention for %s is negative (%s); use zero to keep forever", status, r.TTL(status))
		}
	}
	if r.BatchSize < 0 {
		return fmt.Errorf("queue: retention batch size is negative (%d)", r.BatchSize)
	}
	return nil
}

// Batch is the effective batch size, substituting the default for zero.
func (r Retention) Batch() int {
	if r.BatchSize <= 0 {
		return DefaultPruneBatch
	}
	return r.BatchSize
}

// Cutoff pairs a terminal status with the instant before which its jobs
// may be deleted.
type Cutoff struct {
	// Status is the terminal status the cutoff applies to.
	Status job.Status

	// Before is the timestamp a job must have last been updated before to
	// be eligible for deletion.
	Before time.Time
}

// Cutoffs resolves the policy against a wall clock, returning one entry per
// status that has a finite retention, in TerminalStatuses order. Statuses
// kept forever are omitted, so an empty result means there is nothing to do.
//
// A pass computes these once, up front, rather than per batch. Doing it
// per batch would let the cutoff advance while the pass ran, so a long
// prune of a large history could keep finding newly eligible rows and
// never terminate; a fixed cutoff bounds the pass to the rows that
// qualified when it started.
func (r Retention) Cutoffs(now time.Time) []Cutoff {
	var cutoffs []Cutoff
	for _, status := range TerminalStatuses {
		ttl := r.TTL(status)
		if ttl <= 0 {
			continue
		}
		cutoffs = append(cutoffs, Cutoff{Status: status, Before: now.Add(-ttl).UTC()})
	}
	return cutoffs
}

// String renders the policy as the statuses it prunes and their ages,
// so it can go in a startup log line without the reader decoding a struct.
func (r Retention) String() string {
	parts := make([]string, 0, len(TerminalStatuses))
	for _, status := range TerminalStatuses {
		if ttl := r.TTL(status); ttl > 0 {
			parts = append(parts, string(status)+"="+ttl.String())
		}
	}
	if len(parts) == 0 {
		return "keep everything"
	}
	return strings.Join(parts, " ")
}

// Pruned counts what one prune pass deleted, per terminal status.
type Pruned struct {
	// Succeeded is how many completed jobs were deleted.
	Succeeded int

	// Failed is how many permanently failed jobs were deleted.
	Failed int

	// Dead is how many dead-lettered jobs were deleted.
	Dead int

	// Cancelled is how many cancelled jobs were deleted.
	Cancelled int
}

// Total is how many rows the pass removed altogether.
func (p Pruned) Total() int { return p.Succeeded + p.Failed + p.Dead + p.Cancelled }

// Get returns the count for one status, or zero for a status that is not
// terminal and so was never a candidate.
func (p Pruned) Get(status job.Status) int {
	switch status {
	case job.StatusSucceeded:
		return p.Succeeded
	case job.StatusFailed:
		return p.Failed
	case job.StatusDead:
		return p.Dead
	case job.StatusCancelled:
		return p.Cancelled
	default:
		return 0
	}
}

// Add records n deletions against a status. It is a no-op for a
// non-terminal status, which cannot be pruned.
func (p *Pruned) Add(status job.Status, n int) {
	switch status {
	case job.StatusSucceeded:
		p.Succeeded += n
	case job.StatusFailed:
		p.Failed += n
	case job.StatusDead:
		p.Dead += n
	case job.StatusCancelled:
		p.Cancelled += n
	}
}

// String renders the non-zero counts, for a log line summarizing a pass.
func (p Pruned) String() string {
	if p.Total() == 0 {
		return "nothing pruned"
	}
	parts := make([]string, 0, len(TerminalStatuses))
	for _, status := range TerminalStatuses {
		if n := p.Get(status); n > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", status, n))
		}
	}
	return strings.Join(parts, " ")
}

// Pruner is implemented by queues that keep terminal jobs and can
// therefore be asked to forget them. It is declared here, alongside
// Recoverer, so the janitor and the HTTP API can ask for the capability
// without importing a particular backend -- and so a backend that does not
// retain history (queue.Memory drops a job the moment it is claimed) is
// simply not a Pruner, rather than having to implement a no-op.
type Pruner interface {
	// Prune deletes terminal jobs older than r allows, returning what it
	// removed. It returns ErrNoRetention if r would delete nothing, since
	// a caller asking to prune under a policy that prunes nothing has
	// almost certainly failed to configure one.
	Prune(r Retention) (Pruned, error)

	// Retained reports how many terminal job rows the queue is currently
	// holding: the size of the history a policy governs.
	Retained() (int, error)
}
