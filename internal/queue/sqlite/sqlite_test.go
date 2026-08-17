package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// openTestQueue returns a Queue backed by a private, in-memory SQLite
// database that is closed automatically at the end of the test.
func openTestQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func TestSQLiteEnqueueTryDequeueFIFO(t *testing.T) {
	q := openTestQueue(t)

	first := job.New("q", []byte("1"))
	second := job.New("q", []byte("2"))
	if err := q.Enqueue(first); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Enqueue(second); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got1, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got1.ID != first.ID {
		t.Errorf("got job %q, want %q (FIFO order)", got1.ID, first.ID)
	}

	got2, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got2.ID != second.ID {
		t.Errorf("got job %q, want %q (FIFO order)", got2.ID, second.ID)
	}
}

func TestSQLiteTryDequeueEmpty(t *testing.T) {
	q := openTestQueue(t)
	if _, err := q.TryDequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("TryDequeue on empty queue: err = %v, want ErrEmpty", err)
	}
}

func TestSQLiteTryDequeueMarksRunning(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusRunning)
	}
}

func TestSQLiteEnqueuePreservesFields(t *testing.T) {
	q := openTestQueue(t)
	want := job.New("reports", []byte("payload"),
		job.WithPriority(job.PriorityHigh),
		job.WithMaxRetries(3),
		job.WithTimeout(30*time.Second),
	)
	if err := q.Enqueue(want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Queue != want.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, want.Queue)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, want.Payload)
	}
	if got.Priority != want.Priority {
		t.Errorf("Priority = %v, want %v", got.Priority, want.Priority)
	}
	if got.MaxRetries != want.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", got.MaxRetries, want.MaxRetries)
	}
	if got.Timeout != want.Timeout {
		t.Errorf("Timeout = %v, want %v", got.Timeout, want.Timeout)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
}

func TestSQLiteEnqueueAfterCloseFails(t *testing.T) {
	q := openTestQueue(t)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Enqueue(job.New("q", nil)); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Enqueue after Close: err = %v, want ErrClosed", err)
	}
}

func TestSQLiteReenqueueUpdatesExistingRow(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil, job.WithMaxRetries(1))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	claimed.MarkFailed(claimed.UpdatedAt, errBoom, 0) // RunAt reset to now, eligible immediately
	if err := q.Enqueue(claimed); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}

	if got := q.Len(); got != 1 {
		t.Fatalf("Len after re-enqueue = %d, want 1 (upsert, not a duplicate row)", got)
	}

	retried, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue after retry: %v", err)
	}
	if retried.ID != j.ID {
		t.Errorf("got job %q, want %q", retried.ID, j.ID)
	}
	if retried.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", retried.RetryCount)
	}
}

var errBoom = errors.New("boom")
