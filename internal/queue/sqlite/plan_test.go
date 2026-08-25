package sqlite

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// queryPlan returns the EXPLAIN QUERY PLAN lines for a statement, joined
// into one string for substring matching.
func queryPlan(t *testing.T, q *Queue, query string, args ...any) string {
	t.Helper()

	rows, err := q.db.Query("EXPLAIN QUERY PLAN "+strings.TrimSuffix(strings.TrimSpace(query), ";"), args...)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		lines = append(lines, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}
	return strings.Join(lines, "\n")
}

// planQueue opens a queue holding enough rows that the planner has real
// statistics rather than guessing about an empty table.
func planQueue(t *testing.T) *Queue {
	t.Helper()

	q, err := Open(filepath.Join(t.TempDir(), "plan.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	batch := make([]*job.Job, 500)
	for i := range batch {
		j := job.New("plan", nil)
		if i%2 == 0 {
			j.IdempotencyKey = string(rune('a'+i%26)) + string(rune('a'+i/26%26))
		}
		batch[i] = j
	}
	if err := q.EnqueueBatch(batch); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	return q
}

// TestIdempotencyLookupUsesItsIndex is a regression test for a query that
// read the whole table on every idempotent enqueue.
//
// idx_jobs_idempotency is a partial index (WHERE idempotency_key != ”),
// and SQLite only uses a partial index for a query whose WHERE clause
// provably implies the index's. "idempotency_key = ?" against a bound
// parameter does not imply it, since the parameter could be the empty
// string, so the planner fell back to a full scan -- one that got slower
// as the queue grew. The query now restates the predicate.
func TestIdempotencyLookupUsesItsIndex(t *testing.T) {
	q := planQueue(t)

	plan := queryPlan(t, q, idempotentMatchQuery, "plan", "aa")
	if !strings.Contains(plan, "idx_jobs_idempotency") {
		t.Errorf("the idempotency lookup does not use idx_jobs_idempotency:\n%s", plan)
	}
	if strings.Contains(plan, "SCAN jobs") {
		t.Errorf("the idempotency lookup scans the whole table:\n%s", plan)
	}
}

// TestIdempotencyLookupStillMatches checks the added predicate did not
// change what the query finds, only how it finds it.
func TestIdempotencyLookupStillMatches(t *testing.T) {
	q := planQueue(t)

	original := job.New("plan", []byte("original"))
	original.IdempotencyKey = "unique-key"
	if err := q.Enqueue(original); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	repeat := job.New("plan", []byte("repeat"))
	repeat.IdempotencyKey = "unique-key"
	if err := q.Enqueue(repeat); err != nil {
		t.Fatalf("Enqueue repeat: %v", err)
	}
	if repeat.ID != original.ID {
		t.Errorf("repeat has ID %s, want the original's %s", repeat.ID, original.ID)
	}

	// A job with no key must still be admitted rather than matching one
	// of the many rows whose key is also empty.
	plain := job.New("plan", []byte("plain"))
	before := q.Len()
	if err := q.Enqueue(plain); err != nil {
		t.Fatalf("Enqueue plain: %v", err)
	}
	if got := q.Len(); got != before+1 {
		t.Errorf("Len() = %d after enqueuing a keyless job, want %d", got, before+1)
	}
}
