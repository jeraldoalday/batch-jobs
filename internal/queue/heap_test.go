package queue

import (
	"container/heap"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// TestDelayedHeapOrdersByRunAt pins delayedHeap's ordering directly: with
// more than one job waiting, container/heap only calls Less to compare
// siblings, so a queue test that ever has at most one delayed job in flight
// (as every other test in this package does) never exercises it at all.
func TestDelayedHeapOrdersByRunAt(t *testing.T) {
	base := time.Now()
	soon := &job.Job{ID: "soon", RunAt: base.Add(10 * time.Millisecond)}
	soonest := &job.Job{ID: "soonest", RunAt: base.Add(1 * time.Millisecond)}
	later := &job.Job{ID: "later", RunAt: base.Add(time.Hour)}

	var h delayedHeap
	// Pushed out of RunAt order to prove the heap reorders them rather than
	// merely preserving insertion order.
	heap.Push(&h, later)
	heap.Push(&h, soon)
	heap.Push(&h, soonest)

	wantOrder := []*job.Job{soonest, soon, later}
	for i, want := range wantOrder {
		got := heap.Pop(&h).(*job.Job)
		if got.ID != want.ID {
			t.Errorf("Pop[%d] = job %q, want %q (earliest RunAt first)", i, got.ID, want.ID)
		}
	}
}
