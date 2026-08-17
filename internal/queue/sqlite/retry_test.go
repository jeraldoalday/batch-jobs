package sqlite

import (
	"errors"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestRetryDeadJob(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil, job.WithMaxRetries(0))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	claimed.MarkFailed(claimed.UpdatedAt, errBoom, 0)
	if claimed.Status != job.StatusDead {
		t.Fatalf("precondition: Status = %q, want %q", claimed.Status, job.StatusDead)
	}
	if err := q.Enqueue(claimed); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}

	if err := q.Retry(j.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	got, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusPending)
	}
	if got.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", got.RetryCount)
	}
	if got.LastError != "" {
		t.Errorf("LastError = %q, want empty", got.LastError)
	}
}

func TestRetryFailedJob(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	claimed.Status = job.StatusFailed
	claimed.LastError = errBoom.Error()
	if err := q.Enqueue(claimed); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}

	if err := q.Retry(j.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	got, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusPending)
	}
}

func TestRetryMakesJobDequeueable(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil, job.WithMaxRetries(0))
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

	if err := q.Retry(j.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	redequeued, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue after Retry: %v", err)
	}
	if redequeued.ID != j.ID {
		t.Errorf("TryDequeue returned %q, want %q", redequeued.ID, j.ID)
	}
}

func TestRetryPendingJobFails(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := q.Retry(j.ID); !errors.Is(err, queue.ErrNotRetryable) {
		t.Fatalf("Retry pending job: err = %v, want ErrNotRetryable", err)
	}
}

func TestRetrySucceededJobFails(t *testing.T) {
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

	if err := q.Retry(j.ID); !errors.Is(err, queue.ErrNotRetryable) {
		t.Fatalf("Retry succeeded job: err = %v, want ErrNotRetryable", err)
	}
}

func TestRetryUnknownIDReturnsErrNotFound(t *testing.T) {
	q := openTestQueue(t)
	if err := q.Retry("does-not-exist"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("Retry: err = %v, want ErrNotFound", err)
	}
}

func TestRetryAfterCloseFails(t *testing.T) {
	q := openTestQueue(t)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Retry("anything"); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Retry after Close: err = %v, want ErrClosed", err)
	}
}
