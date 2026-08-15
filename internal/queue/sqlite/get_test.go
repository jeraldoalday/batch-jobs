package sqlite

import (
	"errors"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestGetReturnsEnqueuedJob(t *testing.T) {
	q := openTestQueue(t)
	want := job.New("reports", []byte("payload"), job.WithPriority(job.PriorityHigh))
	if err := q.Enqueue(want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.Get(want.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Status != job.StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusPending)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, want.Payload)
	}
}

func TestGetReflectsCurrentState(t *testing.T) {
	q := openTestQueue(t)
	j := job.New("q", nil)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	got, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Errorf("Status = %q, want %q (Get should see post-dequeue state)", got.Status, job.StatusRunning)
	}
}

func TestGetUnknownIDReturnsErrNotFound(t *testing.T) {
	q := openTestQueue(t)
	if _, err := q.Get("does-not-exist"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("Get: err = %v, want ErrNotFound", err)
	}
}
