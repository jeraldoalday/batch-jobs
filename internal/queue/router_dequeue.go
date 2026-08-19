package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

var _ Queue = (*Router)(nil)

// TryDequeue implements Queue. It sweeps the routes in weighted
// round-robin order and returns the first job it can claim, or ErrEmpty if
// every route is empty.
//
// The sweep holds the router's lock across the backing queues' TryDequeue
// calls, so concurrent workers claim jobs one at a time rather than racing
// to spend the same route's credit. The critical section is one non-blocking
// claim on a single backing queue, which is what queue.Memory and
// queue/sqlite.Queue already serialize internally anyway.
func (r *Router) TryDequeue() (*job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, ErrClosed
	}
	return r.pickLocked()
}

// Dequeue implements Queue. It retries TryDequeue until a job turns up,
// ctx is cancelled, or the router is closed, waiting between sweeps on
// either an Enqueue notification or pollInterval — whichever comes first,
// since a backing queue can become ready without notifying the router (a
// delayed job's RunAt passing, or another process writing to a shared
// database file).
func (r *Router) Dequeue(ctx context.Context) (*job.Job, error) {
	for {
		j, err := r.TryDequeue()
		if err == nil {
			return j, nil
		}
		if !errors.Is(err, ErrEmpty) {
			return nil, err
		}

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, ErrClosed
		}
		waitCh := r.changed
		r.mu.Unlock()

		timer := time.NewTimer(r.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-waitCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// sweepOrderLocked returns the routes to consider for a dequeue: the
// registered routes in registration order, then the default route if one
// was configured. The default goes last so explicitly configured queues
// are always preferred over the catch-all. Callers must hold r.mu.
func (r *Router) sweepOrderLocked() []*route {
	if r.fallback == nil {
		return r.routes
	}
	return append(append(make([]*route, 0, len(r.routes)+1), r.routes...), r.fallback)
}

// startIndexLocked returns where in routes the next sweep should begin:
// at the route that served the previous job, so it can spend the rest of
// its credit before its siblings get a turn. Callers must hold r.mu.
func (r *Router) startIndexLocked(routes []*route) int {
	for i, rt := range routes {
		if rt == r.last {
			return i
		}
	}
	return 0
}

// refillLocked starts a new weighted round, restoring every route's credit
// to its configured weight and clearing the cursor.
//
// Clearing the cursor is the part that matters: it makes the new round
// begin at the first route in registration order rather than at whichever
// route happened to serve last. Without it, the route that closed out the
// previous round would immediately serve again on the fresh credit — three
// heavy jobs, one light one, then another heavy one out of turn — which is
// a full round of unfairness handed to whoever finished last.
//
// Callers must hold r.mu.
func (r *Router) refillLocked(routes []*route) {
	for _, rt := range routes {
		rt.credit = rt.weight
	}
	r.last = nil
}

// pickLocked runs the weighted round-robin sweep. A route may serve up to
// `weight` jobs per round before it is skipped in favour of its siblings;
// once every route has spent its credit, a new round begins. That is what
// keeps a hot queue from starving a quiet one while still honouring the
// configured shares. Callers must hold r.mu.
func (r *Router) pickLocked() (*job.Job, error) {
	routes := r.sweepOrderLocked()
	if len(routes) == 0 {
		return nil, ErrEmpty
	}

	// At most two sweeps: the first spends whatever credit is left in the
	// current round, and if that left some routes unprobed for lack of
	// credit, the second runs against a freshly refilled round.
	for sweep := 0; sweep < 2; sweep++ {
		start := r.startIndexLocked(routes)
		var probed, closed, skipped int

		for i := 0; i < len(routes); i++ {
			rt := routes[(start+i)%len(routes)]
			if rt.credit <= 0 {
				skipped++
				continue
			}
			probed++

			j, err := rt.queue.TryDequeue()
			switch {
			case err == nil:
				rt.credit--
				r.last = rt
				return j, nil
			case errors.Is(err, ErrClosed):
				closed++
			case errors.Is(err, ErrEmpty):
				// Nothing here right now; try the next route.
			default:
				return nil, err
			}
		}

		// The sweep found nothing, so start a fresh round: without this,
		// a router whose backing queues have all gone quiet could hold
		// some routes at zero credit forever and never probe them again.
		r.refillLocked(routes)

		if skipped == 0 {
			// Every route was probed. If they are all closed there is
			// nothing left to wait for, so say so rather than letting
			// Dequeue poll a dead router indefinitely.
			if probed > 0 && closed == probed {
				return nil, ErrClosed
			}
			return nil, ErrEmpty
		}
	}
	return nil, ErrEmpty
}
