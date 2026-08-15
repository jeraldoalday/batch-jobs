package sqlite

import (
	"errors"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestCancelPendingJob(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := q.Cancel(j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	got, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusCancelled)
	}
}

func TestCancelRetryingJob(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil, job.WithMaxRetries(1))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	claimed.MarkFailed(claimed.UpdatedAt, errBoom, 0)
	if err := q.Enqueue(claimed); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}

	if err := q.Cancel(j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	got, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusCancelled)
	}
}

func TestCancelExcludesJobFromDequeue(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if _, err := q.TryDequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("TryDequeue after Cancel: err = %v, want ErrEmpty", err)
	}
}

func TestCancelRunningJobFails(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	if err := q.Cancel(j.ID); !errors.Is(err, queue.ErrNotCancelable) {
		t.Fatalf("Cancel running job: err = %v, want ErrNotCancelable", err)
	}
}

func TestCancelTerminalJobFails(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	claimed.MarkSucceeded(claimed.UpdatedAt)
	if err := q.Enqueue(claimed); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}

	if err := q.Cancel(j.ID); !errors.Is(err, queue.ErrNotCancelable) {
		t.Fatalf("Cancel succeeded job: err = %v, want ErrNotCancelable", err)
	}
}

func TestCancelUnknownIDReturnsErrNotFound(t *testing.T) {
	q := openTestQueue(t)
	if err := q.Cancel("does-not-exist"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("Cancel: err = %v, want ErrNotFound", err)
	}
}

func TestCancelAfterCloseFails(t *testing.T) {
	q := openTestQueue(t)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Cancel("anything"); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Cancel after Close: err = %v, want ErrClosed", err)
	}
}
