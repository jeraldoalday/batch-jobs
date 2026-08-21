package queue

import "github.com/jeraldoalday/batch-jobs/internal/job"

// Completer is implemented by queues that keep a job's record after it has
// been claimed -- durable backends, as opposed to queue.Memory, which drops
// a job the moment a worker takes it.
//
// A worker tells such a queue how the job ended. Without that, a claimed
// job's stored state is frozen at "running" forever: stats count work that
// finished hours ago as in flight, and a queue that leases claims will
// eventually decide the job was orphaned and run it again.
type Completer interface {
	// Complete records j's terminal state (succeeded, failed, or dead)
	// and releases any lease held on it. It returns an error matching
	// ErrLeaseLost if the job is no longer the caller's to complete --
	// typically because recovery already reclaimed it -- and ErrNotFound
	// if the job is unknown to the queue.
	Complete(j *job.Job) error
}

// Complete implements Completer by routing j to the queue that stores it,
// the same way Enqueue does, so a pool draining several queues through a
// Router reports outcomes to the right backend.
//
// Routes that do not keep completed jobs are a no-op success: a Router is
// free to mix a durable queue with an in-memory one, and a worker should
// not have to know which kind it just finished a job from.
func (r *Router) Complete(j *job.Job) error {
	rt, ok := r.routeFor(j.Queue)
	if !ok {
		return ErrNoRoute
	}

	c, ok := rt.queue.(Completer)
	if !ok {
		return nil
	}
	return c.Complete(j)
}
