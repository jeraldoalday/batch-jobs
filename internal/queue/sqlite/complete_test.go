package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// claimOne enqueues a job and claims it, returning the claimed copy: the
// state a worker holds while it is running the job.
func claimOne(t *testing.T, q *Queue, opts ...job.Option) *job.Job {
	t.Helper()

	j := job.New("q", []byte("work"), opts...)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	return claimed
}

func TestCompleteRecordsSuccess(t *testing.T) {
	q := openLeasedQueue(t, Options{})
	claimed := claimOne(t, q, job.WithMaxRetries(2))

	claimed.MarkSucceeded(time.Now().UTC())
	if err := q.Complete(claimed); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got, err := q.Get(claimed.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusSucceeded {
		t.Fatalf("stored status = %q, want succeeded", got.Status)
	}

	stats, err := q.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats[job.StatusRunning] != 0 {
		t.Fatalf("%d job(s) still counted as running after completion, want 0", stats[job.StatusRunning])
	}
}

func TestCompleteRecordsFailure(t *testing.T) {
	q := openLeasedQueue(t, Options{})
	claimed := claimOne(t, q, job.WithMaxRetries(0))

	claimed.MarkFailedPermanently(time.Now().UTC(), errors.New("bad payload"))
	if err := q.Complete(claimed); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got, err := q.Get(claimed.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusFailed {
		t.Fatalf("stored status = %q, want failed", got.Status)
	}
	if got.LastError != "bad payload" {
		t.Fatalf("stored last error = %q, want %q", got.LastError, "bad payload")
	}
}

func TestCompleteReleasesTheLease(t *testing.T) {
	// A lease that has already lapsed: without Complete clearing it, the
	// next recovery pass would treat this finished job as an orphan and
	// run it a second time.
	q := openLeasedQueue(t, Options{LeaseTTL: time.Nanosecond})
	claimed := claimOne(t, q, job.WithMaxRetries(3))

	claimed.MarkSucceeded(time.Now().UTC())
	if err := q.Complete(claimed); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	rec, err := q.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Total() != 0 {
		t.Fatalf("Recover reclaimed %d completed job(s), want 0", rec.Total())
	}
}

func TestCompleteRefusesNonTerminalStatus(t *testing.T) {
	q := openLeasedQueue(t, Options{})
	claimed := claimOne(t, q, job.WithMaxRetries(2))

	// A retrying job goes back through Enqueue; Complete is only for
	// outcomes with nothing left to run.
	claimed.MarkFailed(time.Now().UTC(), errors.New("boom"), time.Millisecond)
	if err := q.Complete(claimed); err == nil {
		t.Fatal("Complete with a retrying job: want an error")
	}
}

func TestCompleteAfterRecoveryReportsLeaseLoss(t *testing.T) {
	q := openLeasedQueue(t, Options{LeaseTTL: time.Nanosecond})
	claimed := claimOne(t, q, job.WithMaxRetries(3))

	// Recovery reclaims the job while the (very slow) worker is still
	// running it. The worker's eventual outcome must not overwrite the
	// rescheduled job.
	if _, err := q.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	claimed.MarkSucceeded(time.Now().UTC())
	if err := q.Complete(claimed); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("Complete after recovery = %v, want ErrLeaseLost", err)
	}

	got, err := q.Get(claimed.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusPending {
		t.Fatalf("stored status = %q, want pending (the recovered state must survive)", got.Status)
	}
}

func TestCompleteUnknownJob(t *testing.T) {
	q := openLeasedQueue(t, Options{})

	j := job.New("q", nil)
	j.MarkSucceeded(time.Now().UTC())
	if err := q.Complete(j); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("Complete on an unknown job = %v, want queue.ErrNotFound", err)
	}
}

func TestBoundCompleteDelegates(t *testing.T) {
	q := openLeasedQueue(t, Options{})
	b := q.Bind("q")

	claimed := claimOne(t, q, job.WithMaxRetries(1))
	claimed.MarkSucceeded(time.Now().UTC())
	if err := b.Complete(claimed); err != nil {
		t.Fatalf("Bound.Complete: %v", err)
	}

	got, err := q.Get(claimed.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusSucceeded {
		t.Fatalf("stored status = %q, want succeeded", got.Status)
	}
}
