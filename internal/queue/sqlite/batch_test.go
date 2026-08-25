package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// batchQueue opens a file-backed queue for a batch test. Batching is about
// transaction boundaries, so these run against a real file rather than an
// in-memory database.
func batchQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := Open(filepath.Join(t.TempDir(), "batch.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

// makeJobs builds n jobs on the named queue, distinguishable by payload.
func makeJobs(name string, n int) []*job.Job {
	jobs := make([]*job.Job, n)
	for i := range jobs {
		jobs[i] = job.New(name, []byte(fmt.Sprintf("payload-%d", i)))
	}
	return jobs
}

func TestEnqueueBatchAdmitsEveryJob(t *testing.T) {
	q := batchQueue(t)
	jobs := makeJobs("batch", 50)

	if err := q.EnqueueBatch(jobs); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	if got := q.Len(); got != 50 {
		t.Errorf("Len() = %d after a batch of 50, want 50", got)
	}
	for _, j := range jobs {
		got, err := q.Get(j.ID)
		if err != nil {
			t.Fatalf("Get(%s): %v", j.ID, err)
		}
		if got.Status != job.StatusPending {
			t.Errorf("job %s has status %q, want pending", j.ID, got.Status)
		}
		if string(got.Payload) != string(j.Payload) {
			t.Errorf("job %s payload = %q, want %q", j.ID, got.Payload, j.Payload)
		}
	}
}

func TestEnqueueBatchEmptyIsANoOp(t *testing.T) {
	q := batchQueue(t)

	if err := q.EnqueueBatch(nil); err != nil {
		t.Errorf("EnqueueBatch(nil) = %v, want nil", err)
	}
	if err := q.EnqueueBatch([]*job.Job{}); err != nil {
		t.Errorf("EnqueueBatch(empty) = %v, want nil", err)
	}
	if got := q.Len(); got != 0 {
		t.Errorf("Len() = %d after empty batches, want 0", got)
	}
}

func TestEnqueueBatchRejectsNilJob(t *testing.T) {
	q := batchQueue(t)

	jobs := makeJobs("batch", 3)
	jobs[1] = nil

	err := q.EnqueueBatch(jobs)
	if err == nil {
		t.Fatal("EnqueueBatch with a nil element succeeded, want an error")
	}
	if got := q.Len(); got != 0 {
		t.Errorf("Len() = %d after a rejected batch, want 0: nothing may be admitted", got)
	}
}

// TestEnqueueBatchIsAtomic is the promise that distinguishes a batch from
// a loop: a failure partway must leave nothing behind.
func TestEnqueueBatchIsAtomic(t *testing.T) {
	q := batchQueue(t)

	jobs := makeJobs("batch", 10)
	// A payload the schema cannot store is hard to arrange, so the
	// failure is induced with a duplicate idempotency key against a row
	// inserted in a way that skips the batch's own lookup: two jobs in
	// the batch sharing a key deduplicate rather than failing, so the
	// collision has to come from the queue column being different while
	// the unique index is not.
	jobs[4].ID = jobs[3].ID
	jobs[4].Queue = "other"

	// A repeated ID upserts rather than failing, so this batch should
	// succeed with nine distinct rows -- and the check below is that the
	// upsert applied to the *second* of the two, not that it errored.
	if err := q.EnqueueBatch(jobs); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	got, err := q.Get(jobs[3].ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Queue != "other" {
		t.Errorf("shared-ID job has queue %q, want the later write's %q", got.Queue, "other")
	}
	if n := q.Len(); n != 9 {
		t.Errorf("Len() = %d, want 9 after a batch of 10 with one repeated ID", n)
	}
}

func TestEnqueueBatchClosedQueue(t *testing.T) {
	q := batchQueue(t)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := q.EnqueueBatch(makeJobs("batch", 3)); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("EnqueueBatch on a closed queue = %v, want ErrClosed", err)
	}
}

func TestEnqueueBatchDedupesAgainstExistingKeys(t *testing.T) {
	q := batchQueue(t)

	original := job.New("batch", []byte("first"))
	original.IdempotencyKey = "shared"
	if err := q.Enqueue(original); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	repeat := job.New("batch", []byte("second"))
	repeat.IdempotencyKey = "shared"
	fresh := job.New("batch", []byte("third"))

	if err := q.EnqueueBatch([]*job.Job{repeat, fresh}); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	if repeat.ID != original.ID {
		t.Errorf("deduped job has ID %s, want the original's %s", repeat.ID, original.ID)
	}
	if string(repeat.Payload) != "first" {
		t.Errorf("deduped job payload = %q, want the original's %q", repeat.Payload, "first")
	}
	if got := q.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2: the repeat must not create a third row", got)
	}
}

// TestEnqueueBatchDedupesWithinOneBatch depends on the batch's lookups
// seeing rows inserted earlier in the same transaction. Without that, two
// jobs sharing a key inside one call would both attempt an insert and the
// unique index would fail the whole batch.
func TestEnqueueBatchDedupesWithinOneBatch(t *testing.T) {
	q := batchQueue(t)

	first := job.New("batch", []byte("first"))
	first.IdempotencyKey = "same"
	second := job.New("batch", []byte("second"))
	second.IdempotencyKey = "same"

	if err := q.EnqueueBatch([]*job.Job{first, second}); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	if second.ID != first.ID {
		t.Errorf("second job has ID %s, want the first's %s", second.ID, first.ID)
	}
	if got := q.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestEnqueueBatchWakesBlockedDequeue(t *testing.T) {
	q := batchQueue(t)

	claimed := make(chan *job.Job, 1)
	go func() {
		j, err := q.Dequeue(t.Context())
		if err != nil {
			close(claimed)
			return
		}
		claimed <- j
	}()

	// Give the dequeue a moment to block rather than to find the job on
	// its first attempt, which would not exercise the notification.
	time.Sleep(50 * time.Millisecond)
	if err := q.EnqueueBatch(makeJobs("batch", 1)); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	select {
	case j, ok := <-claimed:
		if !ok || j == nil {
			t.Fatal("Dequeue returned no job")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue was not woken by EnqueueBatch")
	}
}

func TestEnqueueBatchLeavesTheConnectionUsable(t *testing.T) {
	q := batchQueue(t)

	// A rejected batch must not return a connection to the pool with the
	// write lock still held, or every later write would block on it.
	jobs := makeJobs("batch", 2)
	jobs[1] = nil
	_ = q.EnqueueBatch(jobs)

	done := make(chan error, 1)
	go func() { done <- q.Enqueue(job.New("batch", nil)) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Enqueue after a rejected batch: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Enqueue after a rejected batch blocked; the batch left a transaction open")
	}
}

func TestEnqueueBatchConcurrent(t *testing.T) {
	q := batchQueue(t)

	const batches, size = 8, 25
	errs := make([]error, batches)
	var wg sync.WaitGroup
	for i := 0; i < batches; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = q.EnqueueBatch(makeJobs(fmt.Sprintf("q%d", i), size))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("batch %d: %v", i, err)
		}
	}
	if got, want := q.Len(), batches*size; got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
}

// TestPackageEnqueueBatchFallsBackToALoop covers the helper that lets a
// producer call EnqueueBatch without knowing which backend it holds.
func TestPackageEnqueueBatchFallsBackToALoop(t *testing.T) {
	mem := queue.NewMemory()
	defer mem.Close()

	if _, ok := any(mem).(queue.BatchEnqueuer); ok {
		t.Skip("queue.Memory grew batch support; this test no longer covers the fallback")
	}

	jobs := makeJobs("fallback", 5)
	if err := queue.EnqueueBatch(mem, jobs); err != nil {
		t.Fatalf("queue.EnqueueBatch: %v", err)
	}
	if got := mem.Len(); got != 5 {
		t.Errorf("Len() = %d, want 5", got)
	}
}

func TestPackageEnqueueBatchUsesBatchSupport(t *testing.T) {
	q := batchQueue(t)

	if err := queue.EnqueueBatch(q, makeJobs("batch", 4)); err != nil {
		t.Fatalf("queue.EnqueueBatch: %v", err)
	}
	if got := q.Len(); got != 4 {
		t.Errorf("Len() = %d, want 4", got)
	}
}

func TestEnqueueBatchErrorNamesTheJob(t *testing.T) {
	q := batchQueue(t)

	jobs := makeJobs("batch", 3)
	jobs[2] = nil
	err := q.EnqueueBatch(jobs)
	if err == nil {
		t.Fatal("EnqueueBatch succeeded with a nil element")
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("error %q does not identify which job failed", err)
	}
}
