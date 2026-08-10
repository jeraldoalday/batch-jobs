package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestSQLiteJobsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	q1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// TryDequeue claims the oldest job first, so the one enqueued first
	// ends up "claimed" (status=running) and the second stays pending.
	claimed := job.New("q", []byte("claimed"))
	if err := q1.Enqueue(claimed); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	pending := job.New("q", []byte("pending"))
	if err := q1.Enqueue(pending); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q1.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.ID != claimed.ID {
		t.Fatalf("TryDequeue claimed job %q, want %q", got.ID, claimed.ID)
	}

	if err := q1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a process restart: open a brand new Queue against the same
	// file. Nothing carries over from q1 except what was written to disk.
	q2, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { q2.Close() })

	if got := q2.Len(); got != 1 {
		t.Fatalf("Len after restart = %d, want 1 (the still-pending job)", got)
	}

	stats, err := q2.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats[job.StatusRunning] != 1 {
		t.Errorf("Stats[running] = %d, want 1 (the job claimed before restart)", stats[job.StatusRunning])
	}
	if stats[job.StatusPending] != 1 {
		t.Errorf("Stats[pending] = %d, want 1", stats[job.StatusPending])
	}

	afterRestart, err := q2.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue after restart: %v", err)
	}
	if afterRestart.ID != pending.ID {
		t.Errorf("got job %q, want %q", afterRestart.ID, pending.ID)
	}
	if string(afterRestart.Payload) != "pending" {
		t.Errorf("Payload = %q, want %q", afterRestart.Payload, "pending")
	}
}

func TestSQLiteTerminalJobsPersistForInspection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	q1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	dead := job.New("q", []byte("exhausted"))
	dead.MarkFailed(dead.CreatedAt, errBoom, 0)
	dead.Status = job.StatusDead
	if err := q1.Enqueue(dead); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q2, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { q2.Close() })

	// A dead job must never be handed back out by Dequeue/TryDequeue...
	if _, err := q2.TryDequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("TryDequeue: err = %v, want ErrEmpty (dead jobs are not re-delivered)", err)
	}

	// ...but it must still be inspectable, unlike queue.Memory which drops it.
	deadJobs, err := q2.List(job.StatusDead, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(deadJobs) != 1 || deadJobs[0].ID != dead.ID {
		t.Fatalf("List(StatusDead) = %v, want [%s]", deadJobs, dead.ID)
	}
}

func TestSQLiteReopenAppliesNoDuplicateMigrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	q1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := q1.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer q2.Close()

	if got := q2.Len(); got != 1 {
		t.Fatalf("Len after reopen = %d, want 1", got)
	}
}
