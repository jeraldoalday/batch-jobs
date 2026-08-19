package queue

import (
	"errors"
	"strings"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// newTestRouter builds a router over fresh in-memory queues, one per name,
// and returns both the router and the backing queues by name.
func newTestRouter(t *testing.T, names ...string) (*Router, map[string]*Memory) {
	t.Helper()

	backing := make(map[string]*Memory, len(names))
	routes := make([]Route, len(names))
	for i, name := range names {
		q := NewMemory()
		backing[name] = q
		routes[i] = Route{Name: name, Queue: q}
	}

	r, err := NewRouter(routes)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r, backing
}

func TestNewRouterRejectsInvalidRoutes(t *testing.T) {
	tests := []struct {
		name   string
		routes []Route
	}{
		{"empty name", []Route{{Name: "", Queue: NewMemory()}}},
		{"nil queue", []Route{{Name: "emails", Queue: nil}}},
		{"duplicate name", []Route{
			{Name: "emails", Queue: NewMemory()},
			{Name: "emails", Queue: NewMemory()},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRouter(tt.routes); err == nil {
				t.Fatal("NewRouter: err = nil, want an error")
			}
		})
	}
}

func TestRouterNamesPreservesRegistrationOrder(t *testing.T) {
	r, _ := newTestRouter(t, "payments", "emails", "reports")

	got := r.Names()
	want := []string{"payments", "emails", "reports"}
	if len(got) != len(want) {
		t.Fatalf("Names() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Names() = %v, want %v", got, want)
		}
	}
}

func TestRouterEnqueueRoutesByQueueName(t *testing.T) {
	r, backing := newTestRouter(t, "emails", "payments")

	if err := r.Enqueue(job.New("payments", []byte("charge"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := backing["payments"].Len(); got != 1 {
		t.Errorf("payments queue Len() = %d, want 1", got)
	}
	if got := backing["emails"].Len(); got != 0 {
		t.Errorf("emails queue Len() = %d, want 0", got)
	}
}

func TestRouterEnqueueUnknownQueueIsErrNoRoute(t *testing.T) {
	r, _ := newTestRouter(t, "emails")

	err := r.Enqueue(job.New("nope", nil))
	if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("Enqueue to an unregistered queue: err = %v, want ErrNoRoute", err)
	}
	// The message should name the queue that could not be routed, since
	// that is the only actionable part of the failure.
	if err != nil && !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %q does not mention the unroutable queue name", err)
	}
}

func TestRouterDefaultRouteCatchesUnknownNames(t *testing.T) {
	fallback := NewMemory()
	known := NewMemory()
	r, err := NewRouter([]Route{{Name: "emails", Queue: known}}, WithDefaultRoute(fallback))
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	if err := r.Enqueue(job.New("unregistered", nil)); err != nil {
		t.Fatalf("Enqueue with a default route: %v", err)
	}
	if got := fallback.Len(); got != 1 {
		t.Errorf("default queue Len() = %d, want 1", got)
	}

	// The default must not shadow an explicitly registered route.
	if err := r.Enqueue(job.New("emails", nil)); err != nil {
		t.Fatalf("Enqueue to a registered queue: %v", err)
	}
	if got := known.Len(); got != 1 {
		t.Errorf("emails queue Len() = %d, want 1", got)
	}
	if got := fallback.Len(); got != 1 {
		t.Errorf("default queue Len() = %d, want it unchanged at 1", got)
	}
}

// Jobs sent to the default route must still come back out of Dequeue;
// otherwise the catch-all is a black hole.
func TestRouterDefaultRouteIsDequeued(t *testing.T) {
	fallback := NewMemory()
	r, err := NewRouter([]Route{{Name: "emails", Queue: NewMemory()}}, WithDefaultRoute(fallback))
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	j := job.New("unregistered", nil)
	if err := r.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := r.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("dequeued %q, want the default-routed job %q", got.ID, j.ID)
	}
}

func TestRouterEnqueuePropagatesBackingErrors(t *testing.T) {
	bounded := NewMemory(WithCapacity(1))
	r, err := NewRouter([]Route{{Name: "emails", Queue: bounded}})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	if err := r.Enqueue(job.New("emails", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := r.Enqueue(job.New("emails", nil)); !errors.Is(err, ErrFull) {
		t.Fatalf("Enqueue to a full route: err = %v, want ErrFull", err)
	}
}

func TestRouterLenSumsRoutes(t *testing.T) {
	r, backing := newTestRouter(t, "a", "b")

	for i := 0; i < 3; i++ {
		if err := backing["a"].Enqueue(job.New("a", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := backing["b"].Enqueue(job.New("b", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := r.Len(); got != 4 {
		t.Errorf("Len() = %d, want 4", got)
	}
}

func TestRouterLenIncludesDefaultRoute(t *testing.T) {
	fallback := NewMemory()
	r, err := NewRouter([]Route{{Name: "a", Queue: NewMemory()}}, WithDefaultRoute(fallback))
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	if err := r.Enqueue(job.New("elsewhere", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := r.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestRouterBacking(t *testing.T) {
	r, backing := newTestRouter(t, "emails")

	q, ok := r.Backing("emails")
	if !ok {
		t.Fatal("Backing(\"emails\"): ok = false, want true")
	}
	if q != Queue(backing["emails"]) {
		t.Error("Backing returned a different queue than the one registered")
	}
	if _, ok := r.Backing("missing"); ok {
		t.Error("Backing(\"missing\"): ok = true, want false")
	}
}

func TestRouterRegisterAfterConstruction(t *testing.T) {
	r, _ := newTestRouter(t, "emails")

	late := NewMemory()
	if err := r.Register(Route{Name: "reports", Queue: late}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Register(Route{Name: "reports", Queue: NewMemory()}); err == nil {
		t.Fatal("re-registering a name: err = nil, want an error")
	}

	j := job.New("reports", nil)
	if err := r.Enqueue(j); err != nil {
		t.Fatalf("Enqueue to a late-registered route: %v", err)
	}
	if got := late.Len(); got != 1 {
		t.Errorf("late route Len() = %d, want 1", got)
	}
}

func TestRouterCloseLeavesBackingQueuesOpen(t *testing.T) {
	r, backing := newTestRouter(t, "emails")

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil (idempotent)", err)
	}

	if err := r.Enqueue(job.New("emails", nil)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Close: err = %v, want ErrClosed", err)
	}
	if _, err := r.TryDequeue(); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryDequeue after Close: err = %v, want ErrClosed", err)
	}

	// The router does not own the queues it was handed, so they must
	// still be usable by whoever does.
	if err := backing["emails"].Enqueue(job.New("emails", nil)); err != nil {
		t.Errorf("backing queue after router Close: err = %v, want it still open", err)
	}
}
