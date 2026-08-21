package worker

import (
	"errors"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// complete tells a durable queue how a job ended, for the outcomes that
// leave nothing to enqueue: succeeded, failed permanently, or dead.
//
// Retries are not completed -- they go back through Enqueue, which records
// the new state as part of rescheduling. Queues that do not implement
// queue.Completer (queue.Memory, which drops a job the moment it is
// claimed) are skipped, so this changes nothing for them.
//
// Failures are logged rather than returned: the handler has already run,
// and there is nothing useful the pool can do with the error beyond making
// it visible. Losing the lease is logged more loudly than a database
// error, because it means the job is about to run somewhere else.
func (p *Pool) complete(j *job.Job) {
	c, ok := p.Queue.(queue.Completer)
	if !ok {
		return
	}

	if err := c.Complete(j); err != nil {
		if errors.Is(err, queue.ErrLeaseLost) {
			p.Logger.Warn("worker: job outcome discarded, lease already lost",
				"job_id", j.ID, "queue", j.Queue, "status", j.Status, "error", err)
			return
		}
		p.Logger.Error("worker: failed to record job outcome",
			"job_id", j.ID, "queue", j.Queue, "status", j.Status, "error", err)
	}
}
