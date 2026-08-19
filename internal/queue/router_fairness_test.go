package queue

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// fill enqueues n jobs onto the router's route named name.
func fill(t *testing.T, r *Router, name string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := r.Enqueue(job.New(name, nil)); err != nil {
			t.Fatalf("Enqueue to %s: %v", name, err)
		}
	}
}

// drain dequeues n jobs and returns the route name each came from, which is
// the order the weighting produced.
func drain(t *testing.T, r *Router, n int) string {
	t.Helper()
	var got strings.Builder
	for i := 0; i < n; i++ {
		j, err := r.TryDequeue()
		if err != nil {
			t.Fatalf("TryDequeue %d: %v", i, err)
		}
		got.WriteString(j.Queue)
	}
	return got.String()
}

func TestRouterWeightedOrder(t *testing.T) {
	heavy, light := NewMemory(), NewMemory()
	r, err := NewRouter([]Route{
		{Name: "a", Queue: heavy, Weight: 3},
		{Name: "b", Queue: light, Weight: 1},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	fill(t, r, "a", 6)
	fill(t, r, "b", 2)

	// Three from a, then one from b, twice over: the 3:1 weighting.
	if got, want := drain(t, r, 8), "aaabaaab"; got != want {
		t.Errorf("dequeue order = %q, want %q", got, want)
	}
}

func TestRouterEqualWeightsRoundRobin(t *testing.T) {
	r, _ := newTestRouter(t, "a", "b", "c")

	fill(t, r, "a", 2)
	fill(t, r, "b", 2)
	fill(t, r, "c", 2)

	if got, want := drain(t, r, 6), "abcabc"; got != want {
		t.Errorf("dequeue order = %q, want %q", got, want)
	}
}

// A weight is a share, not a cap: an idle sibling must not stop a busy
// route from using the whole pool.
func TestRouterIdleSiblingDoesNotCapWeight(t *testing.T) {
	small, big := NewMemory(), NewMemory()
	r, err := NewRouter([]Route{
		{Name: "small", Queue: small, Weight: 1},
		{Name: "big", Queue: big, Weight: 10},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	fill(t, r, "small", 5)

	if got, want := drain(t, r, 5), strings.Repeat("small", 5); got != want {
		t.Errorf("dequeue order = %q, want %q", got, want)
	}
}

// The property the weighting exists for: a permanently saturated queue must
// never prevent a quiet one from being served.
func TestRouterHotQueueDoesNotStarveQuietOne(t *testing.T) {
	hot, quiet := NewMemory(), NewMemory()
	r, err := NewRouter([]Route{
		{Name: "hot", Queue: hot, Weight: 5},
		{Name: "quiet", Queue: quiet, Weight: 1},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	fill(t, r, "hot", 100)
	fill(t, r, "quiet", 1)

	for i := 0; i < 20; i++ {
		j, err := r.TryDequeue()
		if err != nil {
			t.Fatalf("TryDequeue %d: %v", i, err)
		}
		if j.Queue == "quiet" {
			return
		}
	}
	t.Error("the quiet route was never served in 20 dequeues from a saturated router")
}

// Credit left over from an earlier round must not leave a route unprobed
// once work reappears somewhere else.
func TestRouterServesAfterIdlePeriod(t *testing.T) {
	r, _ := newTestRouter(t, "a", "b")

	fill(t, r, "a", 1)
	if got := drain(t, r, 1); got != "a" {
		t.Fatalf("dequeue order = %q, want %q", got, "a")
	}

	// Router is now empty; sweeps in between must not wedge the cursor.
	for i := 0; i < 5; i++ {
		if _, err := r.TryDequeue(); !errors.Is(err, ErrEmpty) {
			t.Fatalf("TryDequeue on an empty router: err = %v, want ErrEmpty", err)
		}
	}

	fill(t, r, "b", 1)
	if got := drain(t, r, 1); got != "b" {
		t.Errorf("dequeue order = %q, want %q", got, "b")
	}
}

func TestRouterDequeueBlocksUntilEnqueue(t *testing.T) {
	r, _ := newTestRouter(t, "a")

	done := make(chan *job.Job, 1)
	go func() {
		j, err := r.Dequeue(context.Background())
		if err != nil {
			t.Errorf("Dequeue: %v", err)
			close(done)
			return
		}
		done <- j
	}()

	select {
	case j := <-done:
		t.Fatalf("Dequeue returned %v before anything was enqueued", j)
	case <-time.After(20 * time.Millisecond):
	}

	want := job.New("a", nil)
	if err := r.Enqueue(want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case got := <-done:
		if got == nil || got.ID != want.ID {
			t.Errorf("Dequeue returned %v, want job %q", got, want.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after an Enqueue")
	}
}

// A delayed job arrives with no Enqueue notification when its RunAt
// passes, so the router's poll interval is what has to notice it.
func TestRouterDequeuePollsForDelayedJobs(t *testing.T) {
	backing := NewMemory()
	r, err := NewRouter([]Route{{Name: "a", Queue: backing}}, WithRouterPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	want := job.New("a", nil, job.WithDelay(20*time.Millisecond))
	if err := r.Enqueue(want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := r.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("Dequeue returned %q, want %q", got.ID, want.ID)
	}
}

func TestRouterDequeueRespectsContextCancellation(t *testing.T) {
	r, _ := newTestRouter(t, "a")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := r.Dequeue(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dequeue: err = %v, want context.DeadlineExceeded", err)
	}
}

func TestRouterDequeueUnblocksOnClose(t *testing.T) {
	r, _ := newTestRouter(t, "a")

	errCh := make(chan error, 1)
	go func() {
		_, err := r.Dequeue(context.Background())
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Dequeue after Close: err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after the router was closed")
	}
}

// When every backing queue has shut down there is nothing left to wait
// for, so Dequeue must report ErrClosed instead of polling forever.
func TestRouterDequeueReportsAllBackingQueuesClosed(t *testing.T) {
	r, backing := newTestRouter(t, "a", "b")
	for _, q := range backing {
		if err := q.Close(); err != nil {
			t.Fatalf("closing backing queue: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := r.Dequeue(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Dequeue with all routes closed: err = %v, want ErrClosed", err)
	}
}

// A closed route alongside a live one must not stop the live one being
// served.
func TestRouterSkipsClosedRoutes(t *testing.T) {
	r, backing := newTestRouter(t, "dead", "live")
	if err := backing["dead"].Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fill(t, r, "live", 2)
	if got, want := drain(t, r, 2), "livelive"; got != want {
		t.Errorf("dequeue order = %q, want %q", got, want)
	}
}

// Concurrent workers sharing a router must each get a distinct job: the
// weighted sweep mutates shared credit state, so it has to be serialized.
func TestRouterConcurrentDequeueDeliversEachJobOnce(t *testing.T) {
	const routes = 4
	const perRoute = 25
	const workers = 8

	names := []string{"a", "b", "c", "d"}
	r, _ := newTestRouter(t, names...)
	for _, name := range names {
		fill(t, r, name, perRoute)
	}

	var mu sync.Mutex
	seen := make(map[string]int, routes*perRoute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				j, err := r.Dequeue(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				seen[j.ID]++
				done := len(seen) == routes*perRoute
				mu.Unlock()
				if done {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != routes*perRoute {
		t.Fatalf("delivered %d distinct jobs, want %d", len(seen), routes*perRoute)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("job %s delivered %d times, want exactly once", id, count)
		}
	}
	if got := r.Len(); got != 0 {
		t.Errorf("Len() = %d after draining, want 0", got)
	}
}
