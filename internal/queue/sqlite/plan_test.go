package sqlite

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// TestClaimUsesTheOrderedIndex is the regression test for a dequeue whose
// cost was proportional to the entire backlog.
//
// The claim's ORDER BY could not be served from idx_jobs_dequeue, because
// run_at sits between the status equality and the sort columns, so SQLite
// materialised every ready job into a temporary B-tree and sorted it to
// take the one row LIMIT 1 asked for -- on every claim, from every worker.
// Splitting the status IN into one branch per status lets
// idx_jobs_dequeue_order hand back rows already in order.
func TestClaimUsesTheOrderedIndex(t *testing.T) {
	q := planQueue(t)
	if err := q.Optimize(); err != nil {
		t.Fatalf("Optimize: %v", err)
	}

	plan := queryPlan(t, q, claimQuery, time.Now().UnixNano(), "", "owner", int64(0))

	if got := strings.Count(plan, "idx_jobs_dequeue_order"); got != 2 {
		t.Errorf("the claim uses idx_jobs_dequeue_order for %d of its 2 status branches:\n%s", got, plan)
	}
	// One temp B-tree survives, on the two-row union that picks between
	// the branches' winners. Sorting two rows is free; sorting the
	// backlog is what this guards against.
	if got := strings.Count(plan, "USE TEMP B-TREE FOR ORDER BY"); got > 1 {
		t.Errorf("the claim sorts inside its status branches (%d temp B-trees):\n%s", got, plan)
	}
}

// TestOptimizeSeedsStatistics guards the pragma's flags rather than its
// effect, because the effect is invisible except as speed. Without the
// 0x10000 bit, PRAGMA optimize considers only tables the current
// connection has touched -- and with a pooled connection that is almost
// never the one that wrote the jobs, so the table would never be analyzed
// and the fast plan would never be chosen.
func TestOptimizeSeedsStatistics(t *testing.T) {
	q := planQueue(t)

	if _, err := q.db.Exec("DELETE FROM sqlite_stat1"); err != nil {
		// The table only exists once something has been analyzed; if
		// nothing has, there is nothing to clear.
		t.Logf("clearing statistics: %v", err)
	}
	if err := q.Optimize(); err != nil {
		t.Fatalf("Optimize: %v", err)
	}

	var rows int
	if err := q.db.QueryRow(`SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = 'jobs'`).Scan(&rows); err != nil {
		t.Fatalf("count statistics: %v", err)
	}
	if rows == 0 {
		t.Error("Optimize recorded no statistics for the jobs table")
	}
}

func TestOptimizeOnClosedQueueIsANoOp(t *testing.T) {
	q, err := Open(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Optimize(); err != nil {
		t.Errorf("Optimize on a closed queue = %v, want nil", err)
	}
}

// TestOpenSeedsStatisticsForAPopulatedQueue covers the open-time seeding.
// A process restarting against an existing queue faces the largest
// backlog that queue ever has, which is the worst possible moment to be
// running on generic planner guesses.
func TestOpenSeedsStatisticsForAPopulatedQueue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")

	first, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	batch := make([]*job.Job, 200)
	for i := range batch {
		batch[i] = job.New("reopen", nil)
	}
	if err := first.EnqueueBatch(batch); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if _, err := first.db.Exec("DELETE FROM sqlite_stat1"); err != nil {
		t.Logf("clearing statistics: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()

	var rows int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = 'jobs'`).Scan(&rows); err != nil {
		t.Fatalf("count statistics: %v", err)
	}
	if rows == 0 {
		t.Error("reopening a populated queue left it without statistics")
	}
}

// TestOpenOfAnEmptyQueueSkipsAnalysis checks the guard that keeps the
// common case -- a fresh database with nothing in it -- from paying for an
// analysis of nothing.
func TestOpenOfAnEmptyQueueSkipsAnalysis(t *testing.T) {
	q, err := Open(filepath.Join(t.TempDir(), "empty.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()

	var tables int
	if err := q.db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_stat1'`,
	).Scan(&tables); err != nil {
		t.Fatalf("look for sqlite_stat1: %v", err)
	}
	if tables != 0 {
		t.Error("opening an empty queue ran an analysis it had no reason to run")
	}
}
