package queue

import (
	"errors"
	"testing"
	"time"
)

// fakeLeasedQueue is a Queue that leases claims for exactly one job ID,
// standing in for a durable backend without needing one.
type fakeLeasedQueue struct {
	Queue

	holds string
	ttl   time.Duration
	err   error
	beats int
}

func newFakeLeasedQueue(holds string, ttl time.Duration) *fakeLeasedQueue {
	return &fakeLeasedQueue{Queue: NewMemory(), holds: holds, ttl: ttl}
}

func (q *fakeLeasedQueue) LeaseTTL() time.Duration { return q.ttl }

func (q *fakeLeasedQueue) Heartbeat(id string) error {
	q.beats++
	if q.err != nil {
		return q.err
	}
	if id != q.holds {
		return ErrNotFound
	}
	return nil
}

// newLeasingRouter builds a router over the given routes, closed at the
// end of the test.
func newLeasingRouter(t *testing.T, routes ...Route) *Router {
	t.Helper()
	r, err := NewRouter(routes)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestRouterHeartbeatFindsTheHoldingRoute(t *testing.T) {
	emails := newFakeLeasedQueue("job-1", time.Minute)
	reports := newFakeLeasedQueue("job-2", time.Minute)
	r := newLeasingRouter(t,
		Route{Name: "emails", Queue: emails},
		Route{Name: "reports", Queue: reports},
	)

	if err := r.Heartbeat("job-2"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if reports.beats != 1 {
		t.Fatalf("holding route saw %d heartbeats, want 1", reports.beats)
	}
	if emails.beats != 1 {
		t.Fatalf("non-holding route saw %d heartbeats, want 1 (it is asked first and declines)", emails.beats)
	}
}

func TestRouterHeartbeatUnknownJob(t *testing.T) {
	r := newLeasingRouter(t,
		Route{Name: "emails", Queue: newFakeLeasedQueue("job-1", time.Minute)},
	)

	if err := r.Heartbeat("nobody-has-this"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Heartbeat for an unknown job = %v, want ErrNotFound", err)
	}
}

func TestRouterHeartbeatSurfacesLeaseLoss(t *testing.T) {
	emails := newFakeLeasedQueue("job-1", time.Minute)
	emails.err = ErrLeaseLost
	r := newLeasingRouter(t, Route{Name: "emails", Queue: emails})

	if err := r.Heartbeat("job-1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Heartbeat = %v, want ErrLeaseLost forwarded from the route", err)
	}
}

func TestRouterHeartbeatWithoutLeasingRoutes(t *testing.T) {
	// Plain in-memory queues do not lease; heartbeating a router over
	// them must be a successful no-op rather than an error, so a caller
	// need not know what is behind the router.
	r := newLeasingRouter(t,
		Route{Name: "emails", Queue: NewMemory()},
		Route{Name: "reports", Queue: NewMemory()},
	)

	if err := r.Heartbeat("job-1"); err != nil {
		t.Fatalf("Heartbeat over non-leasing routes = %v, want nil", err)
	}
	if got := r.LeaseTTL(); got != 0 {
		t.Fatalf("LeaseTTL over non-leasing routes = %v, want 0", got)
	}
}

func TestRouterHeartbeatIncludesDefaultRoute(t *testing.T) {
	fallback := newFakeLeasedQueue("job-9", time.Minute)
	r, err := NewRouter(
		[]Route{{Name: "emails", Queue: NewMemory()}},
		WithDefaultRoute(fallback),
	)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { r.Close() })

	if err := r.Heartbeat("job-9"); err != nil {
		t.Fatalf("Heartbeat against the default route: %v", err)
	}
	if fallback.beats != 1 {
		t.Fatalf("default route saw %d heartbeats, want 1", fallback.beats)
	}
}

func TestRouterLeaseTTLTakesTheShortest(t *testing.T) {
	r := newLeasingRouter(t,
		Route{Name: "emails", Queue: newFakeLeasedQueue("a", time.Minute)},
		Route{Name: "reports", Queue: newFakeLeasedQueue("b", 10*time.Second)},
		Route{Name: "audit", Queue: NewMemory()},
	)

	// A heartbeat interval derived from the most generous route would let
	// the strictest route's claims lapse mid-job.
	if got, want := r.LeaseTTL(), 10*time.Second; got != want {
		t.Fatalf("LeaseTTL = %v, want %v (the shortest leasing route)", got, want)
	}
}
