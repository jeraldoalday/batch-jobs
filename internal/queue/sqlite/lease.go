package sqlite

import (
	"fmt"
	"os"
	"time"
)

// DefaultLeaseTTL is how long a claimed job stays claimed without a
// heartbeat before recovery may take it back. It is deliberately generous
// relative to the heartbeat interval a worker uses: reclaiming a job whose
// worker is merely slow to check in means running it twice, which is a far
// worse outcome than recovering a genuinely orphaned job a little late.
const DefaultLeaseTTL = 60 * time.Second

// LeaseLostError reports that a job's lease is no longer held by the caller
// -- recovery reclaimed it, or the job reached a terminal state some other
// way. A worker that sees this while running the job should stop: whatever
// it produces will be attributed to a claim it no longer owns, and another
// worker may already be running the same job.
type LeaseLostError struct {
	// JobID is the job whose lease was lost.
	JobID string

	// Status is the job's status at the time the loss was detected, or
	// empty if the job could not be read back.
	Status string
}

func (e *LeaseLostError) Error() string {
	if e.Status == "" {
		return fmt.Sprintf("sqlite: lease lost for job %s", e.JobID)
	}
	return fmt.Sprintf("sqlite: lease lost for job %s (now %s)", e.JobID, e.Status)
}

// defaultOwner identifies this process in the owner column, so an operator
// looking at a stalled job can tell which host and process was holding it.
// Hostname lookup failures degrade to the pid alone rather than failing:
// the value is diagnostic, never load-bearing.
func defaultOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return fmt.Sprintf("pid-%d", os.Getpid())
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

// leaseExpiry returns the absolute expiry to stamp on a claim made at now,
// or 0 when leases are disabled (a non-positive TTL), which leaves the job
// outside recovery's reach entirely.
func (q *Queue) leaseExpiry(now time.Time) int64 {
	if q.leaseTTL <= 0 {
		return 0
	}
	return now.Add(q.leaseTTL).UnixNano()
}
