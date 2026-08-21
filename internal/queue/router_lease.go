package queue

import (
	"errors"
	"time"
)

// leaser is the subset of a leasing Queue the Router needs to forward
// claim maintenance. It is declared here rather than imported so the queue
// package keeps no dependency on any particular backend, and matches the
// Heartbeater interface worker.Pool looks for.
type leaser interface {
	Heartbeat(id string) error
	LeaseTTL() time.Duration
}

// Heartbeat extends the claim on a running job, forwarding to whichever
// route holds it. Routes are asked in order and the first one that
// recognizes the job answers for it; a route that does not lease, or that
// has never seen the job, is skipped.
//
// Fanning out is necessary because a lease is keyed by job ID alone: by the
// time a worker is executing a job, the queue name that chose the route on
// the way in is no longer part of the conversation. In practice the sweep
// is short -- a router has a handful of routes, not thousands -- and it
// keeps the pool's heartbeating working unchanged when a router is
// substituted for a single queue.
//
// It returns ErrNotFound if no route knows the job, and nil (rather than an
// error) if no route leases at all, so a caller need not know whether the
// queues behind a router expire claims.
func (r *Router) Heartbeat(id string) error {
	leasing := 0
	for _, rt := range r.leasingRoutes() {
		leasing++
		err := rt.Heartbeat(id)
		if errors.Is(err, ErrNotFound) {
			// Not this route's job. Keep looking.
			continue
		}
		// Either the heartbeat landed or it failed for a reason that
		// belongs to the route holding the job (a lost lease, a
		// database error). Either way this is the answer.
		return err
	}

	if leasing == 0 {
		return nil
	}
	return ErrNotFound
}

// LeaseTTL returns the shortest lease TTL among the router's leasing
// routes, or 0 if none of them lease.
//
// The shortest is the only safe answer: a worker derives its heartbeat
// interval from this value, and an interval sized for the most generous
// route would let the strictest route's claims lapse while the job was
// still running.
func (r *Router) LeaseTTL() time.Duration {
	var shortest time.Duration
	for _, rt := range r.leasingRoutes() {
		ttl := rt.LeaseTTL()
		if ttl <= 0 {
			continue
		}
		if shortest == 0 || ttl < shortest {
			shortest = ttl
		}
	}
	return shortest
}

// leasingRoutes snapshots the routes that support leases, including the
// fallback. The snapshot is taken under the router's lock and the calls
// themselves are made outside it, since a heartbeat is a database round
// trip and must not block Enqueue.
func (r *Router) leasingRoutes() []leaser {
	r.mu.Lock()
	defer r.mu.Unlock()

	leasers := make([]leaser, 0, len(r.routes)+1)
	for _, rt := range r.routes {
		if l, ok := rt.queue.(leaser); ok {
			leasers = append(leasers, l)
		}
	}
	if r.fallback != nil {
		if l, ok := r.fallback.queue.(leaser); ok {
			leasers = append(leasers, l)
		}
	}
	return leasers
}
