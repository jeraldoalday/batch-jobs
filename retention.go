package batchjobs

import (
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// Retention says how long a job's record is kept after it finishes.
//
// An Engine's queue is a database file, and nothing in it expires on its
// own: every job the Engine has ever run is still a row. For a queue doing
// a few thousand jobs a day that is a file growing by a few megabytes a
// month, and for a busy one it is the reason the disk filled up.
//
// The zero value keeps everything, which is the default. Set the durations
// for the statuses whose history has a shelf life:
//
//	engine, err := batchjobs.Open(batchjobs.Config{
//	    DB: "jobs.db",
//	    Retention: batchjobs.Retention{
//	        Succeeded: 48 * time.Hour,
//	        Cancelled: 48 * time.Hour,
//	        Failed:    30 * 24 * time.Hour,
//	    },
//	})
//
// The durations are per status because the four mean different things. A
// succeeded job is an audit trail whose value decays in hours. A dead job
// is a work list: deleting one discards the only record that the work was
// ever meant to happen, which is why Dead is left at "forever" even in
// DefaultRetention.
type Retention struct {
	// Succeeded is how long a job that ran cleanly is kept. Zero keeps it
	// forever.
	Succeeded time.Duration

	// Failed is how long a permanently failed job is kept -- one whose
	// handler returned a Permanent error. These say something was wrong
	// with the job itself, and the evidence is in the row, so they are
	// worth keeping longer than successes.
	Failed time.Duration

	// Dead is how long a dead-lettered job is kept: one that exhausted
	// its retries. Leaving this at zero is the recommendation, since the
	// dead-letter queue is the list of work that still needs doing.
	Dead time.Duration

	// Cancelled is how long a job cancelled before it ran is kept.
	Cancelled time.Duration

	// BatchSize bounds how many rows one delete statement removes,
	// defaulting to 1000. It tunes how long the database's write lock is
	// held at a time, not how much a pass deletes in total: a pass keeps
	// issuing batches until nothing eligible is left.
	BatchSize int
}

// DefaultRetention is a reasonable starting policy for a queue whose job
// rows are a diagnostic convenience rather than a record something else
// depends on: successes and cancellations for a day, permanent failures
// for a week, and dead-lettered jobs forever.
func DefaultRetention() Retention {
	return Retention(queue.DefaultRetention())
}

// Enabled reports whether the policy would delete anything.
func (r Retention) Enabled() bool { return r.internal().Enabled() }

// String renders the statuses the policy prunes and their ages, for a
// startup log line.
func (r Retention) String() string { return r.internal().String() }

// internal converts to the queue package's equivalent. The two are the
// same shape deliberately -- a public type the engine's callers can name
// without importing an internal package -- so the conversion is a cast.
func (r Retention) internal() queue.Retention { return queue.Retention(r) }

// Pruned counts what one retention pass deleted, per terminal status.
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

// Total is how many job records the pass removed altogether.
func (p Pruned) Total() int { return p.Succeeded + p.Failed + p.Dead + p.Cancelled }

// Get returns the count for one status, or zero for a status that never
// finished and so was never a candidate.
func (p Pruned) Get(status Status) int { return p.internal().Get(job.Status(status)) }

// String renders the non-zero counts, for a log line summarizing a pass.
func (p Pruned) String() string { return p.internal().String() }

// internal converts to the queue package's equivalent.
func (p Pruned) internal() queue.Pruned { return queue.Pruned(p) }

// fromPruned converts back from the queue package's result.
func fromPruned(p queue.Pruned) Pruned { return Pruned(p) }
