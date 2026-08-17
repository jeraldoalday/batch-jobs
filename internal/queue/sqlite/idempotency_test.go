package sqlite

import (
	"sync"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestSQLiteEnqueueIdempotencyKeyDeduplicates(t *testing.T) {
	q := openTestQueue(t)

	first := job.New("orders", []byte("v1"), job.WithIdempotencyKey("order-42"))
	if err := q.Enqueue(first); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}

	second := job.New("orders", []byte("v2"), job.WithIdempotencyKey("order-42"))
	if err := q.Enqueue(second); err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	if second.ID != first.ID {
		t.Errorf("second.ID = %q, want %q (deduped to the original job)", second.ID, first.ID)
	}
	if string(second.Payload) != "v1" {
		t.Errorf("second.Payload = %q, want %q (original payload, not overwritten)", second.Payload, "v1")
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (no duplicate row)", got)
	}
}

func TestSQLiteEnqueueIdempotencyKeyScopedPerQueue(t *testing.T) {
	q := openTestQueue(t)

	a := job.New("emails", nil, job.WithIdempotencyKey("shared-key"))
	b := job.New("reports", nil, job.WithIdempotencyKey("shared-key"))

	if err := q.Enqueue(a); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := q.Enqueue(b); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}

	if a.ID == b.ID {
		t.Error("expected distinct jobs for the same key in different queues")
	}
	if got := q.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 (same key, different queues, no dedupe)", got)
	}
}

func TestSQLiteEnqueueEmptyIdempotencyKeyNeverDeduplicates(t *testing.T) {
	q := openTestQueue(t)

	a := job.New("q", nil)
	b := job.New("q", nil)
	if err := q.Enqueue(a); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := q.Enqueue(b); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}

	if got := q.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 (empty idempotency keys must not collide)", got)
	}
}

func TestSQLiteEnqueueIdempotencyKeyConcurrentRace(t *testing.T) {
	q := openTestQueue(t)

	const n = 20
	jobs := make([]*job.Job, n)
	for i := range jobs {
		jobs[i] = job.New("orders", nil, job.WithIdempotencyKey("order-race"))
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = q.Enqueue(jobs[i])
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Enqueue[%d]: %v", i, err)
		}
	}

	if got := q.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (all concurrent Enqueues for the same key should collapse to one job)", got)
	}

	winner := jobs[0].ID
	for i, j := range jobs {
		if j.ID != winner {
			t.Errorf("jobs[%d].ID = %q, want %q (every caller should observe the same winning job)", i, j.ID, winner)
		}
	}
}
