package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

var _ queue.Queue = (*Bound)(nil)

// Bound is a view of a Queue restricted to a single logical queue name. It
// satisfies queue.Queue, so it can be handed to a worker.Pool or registered
// as a route on a queue.Router while every logical queue continues to share
// one database file, one schema, and one connection pool.
//
// That sharing is the point: routing several queues over one durable store
// otherwise means one database file per queue, which multiplies the
// migration surface and makes cross-queue stats a join across files.
type Bound struct {
	q    *Queue
	name string
}

// Bind returns a view of q restricted to the given logical queue name.
// Dequeue on the view only ever claims jobs whose Queue field matches, and
// Len only counts those jobs.
//
// Binding is cheap and requires no coordination: several Bound views (and
// the unrestricted Queue itself) may be used concurrently against the same
// Queue, since the claim is still a single atomic UPDATE.
func (q *Queue) Bind(name string) *Bound {
	return &Bound{q: q, name: name}
}

// Name returns the logical queue this view is bound to.
func (b *Bound) Name() string { return b.name }

// Enqueue implements queue.Queue. A job whose Queue field names a different
// logical queue is rejected rather than silently written somewhere this
// view could never dequeue it from.
func (b *Bound) Enqueue(j *job.Job) error {
	if j.Queue != b.name {
		return fmt.Errorf("sqlite: job %s is for queue %q, not %q: %w", j.ID, j.Queue, b.name, queue.ErrNoRoute)
	}
	return b.q.Enqueue(j)
}

// Dequeue implements queue.Queue, claiming only jobs on the bound queue.
func (b *Bound) Dequeue(ctx context.Context) (*job.Job, error) {
	return b.q.blockingDequeue(ctx, b.name)
}

// TryDequeue implements queue.Queue, claiming only jobs on the bound queue.
func (b *Bound) TryDequeue() (*job.Job, error) {
	return b.q.tryClaim(b.name)
}

// Len implements queue.Queue, counting only waiting jobs on the bound
// queue.
func (b *Bound) Len() int {
	var n int
	// As with Queue.Len, an error here is reported as "nothing waiting":
	// the queue.Queue interface gives Len no way to say otherwise.
	_ = b.q.db.QueryRow(
		`SELECT COUNT(*) FROM jobs WHERE queue = ? AND status IN ('pending', 'retrying')`,
		b.name,
	).Scan(&n)
	return n
}

// Heartbeat extends the lease on a running job, delegating to the
// underlying Queue. Leases are keyed by job ID, which is unique across
// every logical queue sharing the database, so a view needs no filter of
// its own here.
func (b *Bound) Heartbeat(id string) error { return b.q.Heartbeat(id) }

// LeaseTTL returns the underlying Queue's lease duration, so a worker
// handed a bound view can pick its heartbeat interval the same way it
// would from the Queue itself.
func (b *Bound) LeaseTTL() time.Duration { return b.q.LeaseTTL() }

// Close implements queue.Queue as a no-op. A view does not own the
// database it reads from — several views and the Queue itself share one
// connection pool — so closing one must not shut the others down. Close
// the underlying Queue instead.
func (b *Bound) Close() error { return nil }
