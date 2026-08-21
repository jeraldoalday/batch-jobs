package queue

// Recovery summarizes one pass of reclaiming jobs whose worker died
// holding them: how many went back in the queue for another attempt, and
// how many had no attempts left and were retired to the dead-letter state
// instead.
type Recovery struct {
	// Requeued is the number of orphaned jobs returned to the queue.
	Requeued int

	// DeadLettered is the number of orphaned jobs that had no retries
	// left.
	DeadLettered int
}

// Total is how many orphaned jobs the pass dealt with, either way.
func (r Recovery) Total() int { return r.Requeued + r.DeadLettered }

// Recoverer is implemented by queues that lease claimed jobs and can
// therefore tell that a worker died holding one. It is the capability the
// HTTP API and the reaper look for, declared here so neither has to import
// a particular backend to ask for a recovery pass.
type Recoverer interface {
	// Recover reclaims every job whose lease has expired.
	Recover() (Recovery, error)

	// ExpiredLeases reports how many claims have lapsed without having
	// been recovered yet.
	ExpiredLeases() (int, error)
}
