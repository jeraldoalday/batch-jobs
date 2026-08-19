package queue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// ErrNoRoute is returned by Router.Enqueue when a job names a queue the
// router has no backing Queue for and no default was configured.
var ErrNoRoute = errors.New("queue: no route for queue name")

// DefaultRouterPollInterval bounds how long Router.Dequeue waits between
// sweeps of its routes when every one of them is empty. A backing queue
// can become ready without the router hearing about it — a delayed job's
// RunAt passing, or another process writing to a shared SQLite file — so
// the router cannot rely on Enqueue notifications alone.
const DefaultRouterPollInterval = 100 * time.Millisecond

// Route binds a logical queue name to the Queue that stores its jobs, plus
// the share of dequeue attempts that queue gets relative to its siblings.
type Route struct {
	// Name is the job.Queue value this route matches. Required, and
	// unique within a Router.
	Name string

	// Queue stores the jobs routed to Name. Required.
	Queue Queue

	// Weight is how many consecutive jobs this route may serve before the
	// router moves on to the next one, relative to the other routes'
	// weights. A route with weight 3 next to one with weight 1 gets three
	// jobs dequeued for every one of its neighbour's, as long as both
	// have work available. Values below 1 are treated as 1.
	Weight int
}

// Router is a Queue that fans out across several backing queues: Enqueue
// dispatches each job to the queue registered for its job.Queue name, and
// Dequeue pulls from all of them in weighted round-robin order.
//
// It exists so one worker.Pool can drain several logical queues with an
// explicit, tunable share of its attention — "spend three quarters of the
// pool on payments and the rest on emails" — rather than either running a
// separate pool per queue or letting whichever queue is busiest crowd out
// the others.
//
// A Router owns none of its backing queues: Close shuts down the router
// itself, leaving the caller to close the queues it registered.
type Router struct {
	mu     sync.Mutex
	routes []*route
	byName map[string]*route
	// fallback receives jobs whose queue name matches no route. Nil (the
	// default) makes such an Enqueue fail with ErrNoRoute.
	fallback *route
	// cursor is the index of the route the last successful dequeue came
	// from, so the next sweep resumes there and drains that route's
	// remaining credit before moving on.
	cursor int

	closed  bool
	changed chan struct{}

	pollInterval time.Duration
}

// route is a Router's internal, mutable view of a Route: the caller's
// configuration plus the credit bookkeeping that implements weighting.
type route struct {
	name   string
	queue  Queue
	weight int
	// credit is how many more jobs this route may serve in the current
	// round. It is refilled to weight when every route has run out.
	credit int
}

// RouterOption configures a Router at construction time.
type RouterOption func(*Router)

// WithRouterPollInterval overrides DefaultRouterPollInterval, the upper
// bound on how long a blocked Dequeue waits before re-sweeping its routes.
// A non-positive d is ignored.
func WithRouterPollInterval(d time.Duration) RouterOption {
	return func(r *Router) {
		if d > 0 {
			r.pollInterval = d
		}
	}
}

// WithDefaultRoute sends jobs whose queue name matches no registered route
// to q instead of failing with ErrNoRoute. It is the catch-all for
// producers that invent queue names at runtime.
func WithDefaultRoute(q Queue) RouterOption {
	return func(r *Router) {
		if q != nil {
			r.fallback = &route{name: "", queue: q, weight: 1, credit: 1}
		}
	}
}

// NewRouter returns a Router serving the given routes, in the order given.
// It returns an error if any route is invalid (empty name, nil queue) or
// two routes share a name.
func NewRouter(routes []Route, opts ...RouterOption) (*Router, error) {
	r := &Router{
		byName:       make(map[string]*route, len(routes)),
		changed:      make(chan struct{}),
		pollInterval: DefaultRouterPollInterval,
	}
	for _, opt := range opts {
		opt(r)
	}
	for _, rt := range routes {
		if err := r.Register(rt); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Register adds a route to the router. Routes may be added at any time,
// including while workers are dequeuing.
func (r *Router) Register(rt Route) error {
	if rt.Name == "" {
		return fmt.Errorf("queue: route name is required")
	}
	if rt.Queue == nil {
		return fmt.Errorf("queue: route %q has no queue", rt.Name)
	}
	if rt.Weight < 1 {
		rt.Weight = 1
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byName[rt.Name]; exists {
		return fmt.Errorf("queue: route %q already registered", rt.Name)
	}

	entry := &route{name: rt.Name, queue: rt.Queue, weight: rt.Weight, credit: rt.Weight}
	r.routes = append(r.routes, entry)
	r.byName[rt.Name] = entry

	// A new route may already hold ready jobs, so wake anyone waiting.
	r.broadcastLocked()
	return nil
}

// Names returns the registered route names, in registration order.
func (r *Router) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	names := make([]string, len(r.routes))
	for i, rt := range r.routes {
		names[i] = rt.name
	}
	return names
}

// Backing returns the Queue registered under name, reporting false if
// there is none. It is the escape hatch for operations the Queue interface
// doesn't cover, such as a per-queue depth gauge.
func (r *Router) Backing(name string) (Queue, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rt, ok := r.byName[name]
	if !ok {
		return nil, false
	}
	return rt.queue, true
}

// broadcastLocked wakes every goroutine blocked in Dequeue. Callers must
// hold r.mu.
func (r *Router) broadcastLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

// routeFor returns the route a job with the given queue name belongs to,
// falling back to the default route if one was configured.
func (r *Router) routeFor(name string) (*route, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if rt, ok := r.byName[name]; ok {
		return rt, true
	}
	if r.fallback != nil {
		return r.fallback, true
	}
	return nil, false
}

// Enqueue implements Queue by dispatching j to the queue registered under
// j.Queue, or to the default route if one was configured. Errors from the
// backing queue (ErrFull, ErrClosed) are returned unchanged, so a producer
// sees the same backpressure it would get talking to that queue directly.
func (r *Router) Enqueue(j *job.Job) error {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return ErrClosed
	}

	rt, ok := r.routeFor(j.Queue)
	if !ok {
		return fmt.Errorf("queue: %q: %w", j.Queue, ErrNoRoute)
	}

	if err := rt.queue.Enqueue(j); err != nil {
		return err
	}

	r.mu.Lock()
	r.broadcastLocked()
	r.mu.Unlock()
	return nil
}

// Len implements Queue, reporting the combined backlog of every route
// (plus the default route, if one was configured).
func (r *Router) Len() int {
	r.mu.Lock()
	routes := make([]*route, 0, len(r.routes)+1)
	routes = append(routes, r.routes...)
	if r.fallback != nil {
		routes = append(routes, r.fallback)
	}
	r.mu.Unlock()

	total := 0
	for _, rt := range routes {
		total += rt.queue.Len()
	}
	return total
}

// Close implements Queue. It shuts down the router — blocked Dequeue calls
// return ErrClosed and further Enqueues fail — but deliberately leaves the
// backing queues open, since the router did not create them and other
// routers, pools, or an HTTP API may still be using them.
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	r.broadcastLocked()
	return nil
}
