package sqlite

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// crashedQueue opens a queue at path with leases that expire immediately,
// claims one job, and then closes the handle without ever reporting an
// outcome -- the database state a SIGKILLed worker leaves behind.
func crashedQueue(t *testing.T, path string, opts ...job.Option) string {
	t.Helper()

	q, err := Open(path, Options{LeaseTTL: time.Nanosecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()

	j := job.New("q", []byte("work"), opts...)
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if claimed.Status != job.StatusRunning {
		t.Fatalf("claimed job status = %q, want running", claimed.Status)
	}
	return claimed.ID
}

func TestRecoverRequeuesOrphanedJob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	id := crashedQueue(t, path, job.WithMaxRetries(3))

	// A fresh process starts up against the same file.
	q, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	if got := q.Len(); got != 0 {
		t.Fatalf("Len before recovery = %d, want 0 (the orphan is invisible to the queue)", got)
	}

	rec, err := q.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Requeued != 1 || rec.DeadLettered != 0 {
		t.Fatalf("Recover = %+v, want 1 requeued, 0 dead-lettered", rec)
	}

	got, err := q.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusPending {
		t.Fatalf("recovered job status = %q, want pending", got.Status)
	}
	if got.RetryCount != 1 {
		t.Fatalf("recovered job retry count = %d, want 1 (recovery spends an attempt)", got.RetryCount)
	}
	if got.LastError == "" {
		t.Fatal("recovered job has no LastError explaining the recovery")
	}
	if q.Len() != 1 {
		t.Fatalf("Len after recovery = %d, want 1 (the job is queued again)", q.Len())
	}
}

func TestRecoverDeadLettersWhenRetriesExhausted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	// No retries allowed: the crash used up the job's only attempt.
	id := crashedQueue(t, path)

	q, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	rec, err := q.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Requeued != 0 || rec.DeadLettered != 1 {
		t.Fatalf("Recover = %+v, want 0 requeued, 1 dead-lettered", rec)
	}
	if rec.Total() != 1 {
		t.Fatalf("Recovery.Total = %d, want 1", rec.Total())
	}

	got, err := q.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusDead {
		t.Fatalf("job status = %q, want dead", got.Status)
	}
}

func TestRecoverSpendsOneAttemptPerCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	q, err := Open(path, Options{LeaseTTL: time.Nanosecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	j := job.New("q", []byte("poison"), job.WithMaxRetries(2))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A job that kills whichever worker picks it up: claimed, orphaned,
	// recovered, over and over. Recovery must not loop forever.
	for i := 0; i < 3; i++ {
		if _, err := q.TryDequeue(); err != nil {
			t.Fatalf("TryDequeue %d: %v", i, err)
		}
		if _, err := q.Recover(); err != nil {
			t.Fatalf("Recover %d: %v", i, err)
		}
	}

	got, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusDead {
		t.Fatalf("after 3 crash-recover cycles with MaxRetries=2, status = %q, want dead", got.Status)
	}
	if _, err := q.TryDequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("TryDequeue after the job died = %v, want ErrEmpty", err)
	}
}

func TestRecoverLeavesLiveClaimsAlone(t *testing.T) {
	q, err := Open(":memory:", Options{LeaseTTL: time.Minute})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	j := job.New("q", []byte("work"), job.WithMaxRetries(3))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	rec, err := q.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Total() != 0 {
		t.Fatalf("Recover reclaimed %d job(s) with an unexpired lease, want 0", rec.Total())
	}

	got, err := q.Get(j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Fatalf("live job status = %q, want running", got.Status)
	}
}

func TestRecoverIgnoresUnleasedJobs(t *testing.T) {
	// A negative TTL disables leasing, standing in for both an opted-out
	// deployment and rows written before migration 0003.
	q, err := Open(":memory:", Options{LeaseTTL: -1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	if q.LeaseTTL() != 0 {
		t.Fatalf("LeaseTTL with leasing disabled = %v, want 0", q.LeaseTTL())
	}

	j := job.New("q", []byte("work"), job.WithMaxRetries(3))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	rec, err := q.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec.Total() != 0 {
		t.Fatalf("Recover reclaimed %d unleased job(s), want 0", rec.Total())
	}
}

func TestExpiredLeasesCountsOrphansUntilRecovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	crashedQueue(t, path, job.WithMaxRetries(1))

	q, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	n, err := q.ExpiredLeases()
	if err != nil {
		t.Fatalf("ExpiredLeases: %v", err)
	}
	if n != 1 {
		t.Fatalf("ExpiredLeases before recovery = %d, want 1", n)
	}

	if _, err := q.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n, err = q.ExpiredLeases(); err != nil || n != 0 {
		t.Fatalf("ExpiredLeases after recovery = %d (err %v), want 0", n, err)
	}
}

func TestHeartbeatKeepsLeaseAlive(t *testing.T) {
	q, err := Open(":memory:", Options{LeaseTTL: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	j := job.New("q", []byte("slow"), job.WithMaxRetries(3))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}

	// Outlive the original lease by heartbeating across it.
	for i := 0; i < 4; i++ {
		time.Sleep(20 * time.Millisecond)
		if err := q.Heartbeat(j.ID); err != nil {
			t.Fatalf("Heartbeat %d: %v", i, err)
		}
		rec, err := q.Recover()
		if err != nil {
			t.Fatalf("Recover %d: %v", i, err)
		}
		if rec.Total() != 0 {
			t.Fatalf("Recover reclaimed a heartbeating job on iteration %d", i)
		}
	}
}

func TestHeartbeatReportsLostLease(t *testing.T) {
	q, err := Open(":memory:", Options{LeaseTTL: time.Nanosecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	j := job.New("q", []byte("work"), job.WithMaxRetries(3))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.TryDequeue(); err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if _, err := q.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	err = q.Heartbeat(j.ID)
	var lost *LeaseLostError
	if !errors.As(err, &lost) {
		t.Fatalf("Heartbeat after recovery = %v, want *LeaseLostError", err)
	}
	if lost.JobID != j.ID {
		t.Fatalf("LeaseLostError.JobID = %q, want %q", lost.JobID, j.ID)
	}
	if lost.Status != string(job.StatusPending) {
		t.Fatalf("LeaseLostError.Status = %q, want pending", lost.Status)
	}
	if lost.Error() == "" {
		t.Fatal("LeaseLostError has an empty message")
	}
}

func TestHeartbeatUnknownJob(t *testing.T) {
	q, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	if err := q.Heartbeat("nope"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("Heartbeat on an unknown job = %v, want queue.ErrNotFound", err)
	}
}

func TestHeartbeatIsNoOpWithoutLeases(t *testing.T) {
	q, err := Open(":memory:", Options{LeaseTTL: -1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	// No job, no lease, no error: callers should not have to check
	// whether the queue leases before heartbeating.
	if err := q.Heartbeat("anything"); err != nil {
		t.Fatalf("Heartbeat with leasing disabled = %v, want nil", err)
	}
}

func TestRecoverOnClosedQueue(t *testing.T) {
	q, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := q.Recover(); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Recover on a closed queue = %v, want queue.ErrClosed", err)
	}
	if err := q.Heartbeat("x"); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Heartbeat on a closed queue = %v, want queue.ErrClosed", err)
	}
}
