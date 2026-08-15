package job

// Status represents the lifecycle state of a Job.
type Status string

const (
	// StatusPending means the job has been enqueued but not yet picked up
	// by a worker.
	StatusPending Status = "pending"

	// StatusRunning means a worker has claimed the job and is currently
	// executing it.
	StatusRunning Status = "running"

	// StatusSucceeded means the job's handler returned without error.
	StatusSucceeded Status = "succeeded"

	// StatusFailed means the job's handler returned an error and no
	// further retries remain.
	StatusFailed Status = "failed"

	// StatusRetrying means the job's handler returned an error and the
	// job has been rescheduled for another attempt.
	StatusRetrying Status = "retrying"

	// StatusDead means the job exhausted its retries and was moved to the
	// dead-letter queue for manual inspection.
	StatusDead Status = "dead"

	// StatusCancelled means the job was cancelled by a caller (e.g. via the
	// HTTP API) before a worker claimed it. Only pending or retrying jobs
	// can be cancelled; a job already running or in a terminal state is
	// not.
	StatusCancelled Status = "cancelled"
)

// Terminal reports whether the status represents a final state that a job
// will not transition out of on its own.
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

// Valid reports whether s is one of the known Status values.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusRetrying, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

func (s Status) String() string {
	return string(s)
}
