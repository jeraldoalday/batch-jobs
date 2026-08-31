package sqlite

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// insertFinished writes a job already in a terminal status, with
// updated_at set to age ago. Retention keys on when a job finished, and
// finishing one for real would stamp it "now", so the tests reach past
// the lifecycle to place rows at a chosen age.
func insertFinished(t *testing.T, q *Queue, status job.Status, age time.Duration) *job.Job {
	t.Helper()

	j := job.New("q", []byte("work"))
	j.Status = status
	finished := time.Now().Add(-age).UTC()
	j.CreatedAt = finished
	j.UpdatedAt = finished
	j.RunAt = finished

	if _, err := q.db.Exec(
		`INSERT INTO jobs (`+jobColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		insertArgs(j)...,
	); err != nil {
		t.Fatalf("insert %s job: %v", status, err)
	}
	return j
}

// countStatus is the number of rows left in a status.
func countStatus(t *testing.T, q *Queue, status job.Status) int {
	t.Helper()

	var n int
	if err := q.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE status = ?`, string(status)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", status, err)
	}
	return n
}

func TestPruneDeletesOnlyExpiredJobs(t *testing.T) {
	q := openTestQueue(t)

	old := insertFinished(t, q, job.StatusSucceeded, 2*time.Hour)
	recent := insertFinished(t, q, job.StatusSucceeded, 10*time.Minute)

	pruned, err := q.Prune(queue.Retention{Succeeded: time.Hour})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Succeeded != 1 || pruned.Total() != 1 {
		t.Fatalf("pruned = %v, want exactly one succeeded job", pruned)
	}

	if _, err := q.Get(old.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("Get(old) = %v, want ErrNotFound: it was two hours past a one-hour policy", err)
	}
	if _, err := q.Get(recent.ID); err != nil {
		t.Errorf("Get(recent) = %v, want the job still present", err)
	}
}

// A status the policy leaves at zero is kept forever, however old its
// jobs are. This is the property that makes the zero-value policy safe.
func TestPruneKeepsStatusesWithNoTTL(t *testing.T) {
	q := openTestQueue(t)

	dead := insertFinished(t, q, job.StatusDead, 365*24*time.Hour)
	failed := insertFinished(t, q, job.StatusFailed, 365*24*time.Hour)
	insertFinished(t, q, job.StatusSucceeded, 365*24*time.Hour)

	pruned, err := q.Prune(queue.Retention{Succeeded: time.Hour})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Dead != 0 || pruned.Failed != 0 {
		t.Errorf("pruned = %v, want nothing outside the configured status", pruned)
	}

	for _, j := range []*job.Job{dead, failed} {
		if _, err := q.Get(j.ID); err != nil {
			t.Errorf("Get(%s) = %v, want the job kept", j.Status, err)
		}
	}
}

// Pruning must never touch work that has not finished, no matter how long
// it has been sitting there: an old pending job is a backlog, not history.
func TestPruneNeverTouchesUnfinishedJobs(t *testing.T) {
	q := openTestQueue(t)

	pending := insertFinished(t, q, job.StatusPending, 30*24*time.Hour)
	running := insertFinished(t, q, job.StatusRunning, 30*24*time.Hour)
	retrying := insertFinished(t, q, job.StatusRetrying, 30*24*time.Hour)

	r := queue.Retention{
		Succeeded: time.Second,
		Failed:    time.Second,
		Dead:      time.Second,
		Cancelled: time.Second,
	}
	pruned, err := q.Prune(r)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Total() != 0 {
		t.Errorf("pruned %v, want nothing: none of these jobs have finished", pruned)
	}

	for _, j := range []*job.Job{pending, running, retrying} {
		if _, err := q.Get(j.ID); err != nil {
			t.Errorf("Get(%s) = %v, want the job kept", j.Status, err)
		}
	}
}

func TestPruneCountsEachStatusSeparately(t *testing.T) {
	q := openTestQueue(t)

	for i := 0; i < 3; i++ {
		insertFinished(t, q, job.StatusSucceeded, time.Hour)
	}
	for i := 0; i < 2; i++ {
		insertFinished(t, q, job.StatusCancelled, time.Hour)
	}
	insertFinished(t, q, job.StatusDead, time.Hour)

	pruned, err := q.Prune(queue.Retention{
		Succeeded: time.Minute,
		Cancelled: time.Minute,
		Dead:      time.Minute,
	})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Succeeded != 3 || pruned.Cancelled != 2 || pruned.Dead != 1 {
		t.Errorf("pruned = %v, want succeeded=3 cancelled=2 dead=1", pruned)
	}
	if pruned.Total() != 6 {
		t.Errorf("Total() = %d, want 6", pruned.Total())
	}
}

// The batch loop must keep going until the status is clear, not stop
// after the first short-of-a-full-batch round or run forever on a full one.
func TestPruneBatchesUntilExhausted(t *testing.T) {
	q := openTestQueue(t)

	const total = 25
	for i := 0; i < total; i++ {
		insertFinished(t, q, job.StatusSucceeded, time.Hour)
	}

	pruned, err := q.Prune(queue.Retention{Succeeded: time.Minute, BatchSize: 4})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Succeeded != total {
		t.Errorf("pruned %d jobs in batches of 4, want %d", pruned.Succeeded, total)
	}
	if n := countStatus(t, q, job.StatusSucceeded); n != 0 {
		t.Errorf("%d succeeded jobs left, want 0", n)
	}
}

// A batch size that exactly divides the eligible rows makes the final
// round a full batch, which is the case a "stop when the batch is short"
// loop gets wrong if it checks before deleting.
func TestPruneExactMultipleOfBatchSize(t *testing.T) {
	q := openTestQueue(t)

	for i := 0; i < 10; i++ {
		insertFinished(t, q, job.StatusSucceeded, time.Hour)
	}

	pruned, err := q.Prune(queue.Retention{Succeeded: time.Minute, BatchSize: 5})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Succeeded != 10 {
		t.Errorf("pruned = %d, want 10", pruned.Succeeded)
	}
}

// Each batch takes the oldest rows first, so a pass interrupted partway
// has removed the least interesting history rather than an arbitrary
// slice of it. Prune itself never stops early, so the ordering is
// observed by running a single batch of the underlying statement.
func TestPruneBatchTakesOldestFirst(t *testing.T) {
	q := openTestQueue(t)

	oldest := insertFinished(t, q, job.StatusSucceeded, 5*time.Hour)
	middle := insertFinished(t, q, job.StatusSucceeded, 4*time.Hour)
	newest := insertFinished(t, q, job.StatusSucceeded, 3*time.Hour)

	cutoff := time.Now().Add(-time.Hour).UnixNano()
	if _, err := q.db.Exec(pruneBatchQuery, string(job.StatusSucceeded), cutoff, 1); err != nil {
		t.Fatalf("prune batch: %v", err)
	}

	if _, err := q.Get(oldest.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("Get(oldest) = %v, want ErrNotFound: a batch of one must take the oldest row", err)
	}
	for _, j := range []*job.Job{middle, newest} {
		if _, err := q.Get(j.ID); err != nil {
			t.Errorf("Get(%s) = %v, want the job still present", j.ID, err)
		}
	}
}

// The cutoff bounds the pass regardless of batching: rows on the recent
// side of it are never eligible, however many rounds the loop runs.
func TestPruneStopsAtTheCutoff(t *testing.T) {
	q := openTestQueue(t)

	expired := insertFinished(t, q, job.StatusSucceeded, 5*time.Hour)
	fresh := insertFinished(t, q, job.StatusSucceeded, 3*time.Hour)

	pruned, err := q.Prune(queue.Retention{Succeeded: 4 * time.Hour, BatchSize: 1})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Succeeded != 1 {
		t.Errorf("pruned %d jobs, want 1", pruned.Succeeded)
	}
	if _, err := q.Get(expired.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("Get(expired) = %v, want ErrNotFound", err)
	}
	if _, err := q.Get(fresh.ID); err != nil {
		t.Errorf("Get(fresh) = %v, want the job kept", err)
	}
}

func TestPruneRejectsPolicyThatPrunesNothing(t *testing.T) {
	q := openTestQueue(t)

	if _, err := q.Prune(queue.Retention{}); !errors.Is(err, queue.ErrNoRetention) {
		t.Fatalf("Prune(zero policy) = %v, want ErrNoRetention", err)
	}
}

func TestPruneRejectsInvalidPolicy(t *testing.T) {
	q := openTestQueue(t)

	if _, err := q.Prune(queue.Retention{Succeeded: -time.Hour}); err == nil {
		t.Fatal("Prune accepted a negative retention")
	}
}

func TestPruneOnClosedQueue(t *testing.T) {
	q, err := Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	q.Close()

	if _, err := q.Prune(queue.Retention{Succeeded: time.Hour}); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Prune on closed queue = %v, want ErrClosed", err)
	}
}

// Pruning an empty history is a no-op success, not an error: a janitor
// runs on a timer and most of its passes have nothing to do.
func TestPruneEmptyIsNoOp(t *testing.T) {
	q := openTestQueue(t)

	pruned, err := q.Prune(queue.Retention{Succeeded: time.Hour})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Total() != 0 {
		t.Errorf("pruned = %v on an empty queue, want nothing", pruned)
	}
}

// Pruning must leave the live queue alone: jobs waiting to run are still
// dequeued in the same order afterwards.
func TestPruneLeavesTheQueueWorking(t *testing.T) {
	q := openTestQueue(t)

	for i := 0; i < 5; i++ {
		insertFinished(t, q, job.StatusSucceeded, time.Hour)
	}
	first := job.New("q", []byte("first"))
	if err := q.Enqueue(first); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, err := q.Prune(queue.Retention{Succeeded: time.Minute}); err != nil {
		t.Fatalf("Prune: %v", err)
	}

	if got := q.Len(); got != 1 {
		t.Errorf("Len() = %d after pruning, want 1", got)
	}
	claimed, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue after prune: %v", err)
	}
	if claimed.ID != first.ID {
		t.Errorf("claimed %q, want the job that was never eligible for pruning", claimed.ID)
	}
}

func TestRetainedCountsOnlyFinishedJobs(t *testing.T) {
	q := openTestQueue(t)

	insertFinished(t, q, job.StatusSucceeded, time.Hour)
	insertFinished(t, q, job.StatusDead, time.Hour)
	insertFinished(t, q, job.StatusPending, time.Hour)
	insertFinished(t, q, job.StatusRunning, time.Hour)

	got, err := q.Retained()
	if err != nil {
		t.Fatalf("Retained: %v", err)
	}
	if got != 2 {
		t.Errorf("Retained() = %d, want 2 (the pending and running jobs are not history)", got)
	}
}

func TestRetainedFallsAfterPruning(t *testing.T) {
	q := openTestQueue(t)

	for i := 0; i < 4; i++ {
		insertFinished(t, q, job.StatusSucceeded, time.Hour)
	}
	before, err := q.Retained()
	if err != nil {
		t.Fatalf("Retained: %v", err)
	}

	if _, err := q.Prune(queue.Retention{Succeeded: time.Minute}); err != nil {
		t.Fatalf("Prune: %v", err)
	}

	after, err := q.Retained()
	if err != nil {
		t.Fatalf("Retained: %v", err)
	}
	if before != 4 || after != 0 {
		t.Errorf("Retained went %d -> %d, want 4 -> 0", before, after)
	}
}

func TestOldestRetainedEmptyQueue(t *testing.T) {
	q := openTestQueue(t)

	got, err := q.OldestRetained()
	if err != nil {
		t.Fatalf("OldestRetained: %v", err)
	}
	if !got.IsZero() {
		t.Errorf("OldestRetained() = %s on an empty queue, want the zero time", got)
	}
}

func TestOldestRetainedIgnoresUnfinishedJobs(t *testing.T) {
	q := openTestQueue(t)

	insertFinished(t, q, job.StatusPending, 30*24*time.Hour)
	insertFinished(t, q, job.StatusSucceeded, time.Hour)

	got, err := q.OldestRetained()
	if err != nil {
		t.Fatalf("OldestRetained: %v", err)
	}
	if age := time.Since(got); age > 2*time.Hour {
		t.Errorf("OldestRetained() is %s old, want ~1h: the month-old pending job is not history", age)
	}
}

// The gauge exists to reveal a history that has aged past its policy, so
// it must report the oldest row rather than the newest or an average.
func TestOldestRetainedReportsTheOldest(t *testing.T) {
	q := openTestQueue(t)

	insertFinished(t, q, job.StatusSucceeded, time.Hour)
	insertFinished(t, q, job.StatusDead, 6*time.Hour)
	insertFinished(t, q, job.StatusSucceeded, 2*time.Hour)

	got, err := q.OldestRetained()
	if err != nil {
		t.Fatalf("OldestRetained: %v", err)
	}
	age := time.Since(got)
	if age < 5*time.Hour || age > 7*time.Hour {
		t.Errorf("OldestRetained() is %s old, want ~6h", age)
	}
}

// A large-ish history exercised through the real batch default, as a
// check that nothing about the loop is quadratic or off by a batch.
func TestPruneLargeHistory(t *testing.T) {
	q := openTestQueue(t)

	const total = 500
	for i := 0; i < total; i++ {
		status := job.StatusSucceeded
		if i%5 == 0 {
			status = job.StatusCancelled
		}
		insertFinished(t, q, status, time.Duration(i+1)*time.Minute)
	}

	pruned, err := q.Prune(queue.Retention{
		Succeeded: time.Second,
		Cancelled: time.Second,
		BatchSize: 64,
	})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned.Total() != total {
		t.Errorf("pruned %d of %d jobs", pruned.Total(), total)
	}

	retained, err := q.Retained()
	if err != nil {
		t.Fatalf("Retained: %v", err)
	}
	if retained != 0 {
		t.Errorf("Retained() = %d after pruning everything, want 0", retained)
	}
}

// The prune batch must be answered from idx_jobs_retention. Without it,
// each round scans every row in the status -- the whole history, once per
// batch -- which is the cost the migration exists to remove.
func TestPruneBatchUsesTheRetentionIndex(t *testing.T) {
	q := planQueue(t)

	plan := queryPlan(t, q, pruneBatchQuery, string(job.StatusSucceeded), time.Now().UnixNano(), 100)
	if !strings.Contains(plan, "idx_jobs_retention") {
		t.Errorf("prune batch plan does not use idx_jobs_retention:\n%s", plan)
	}
	if strings.Contains(plan, "SCAN jobs") {
		t.Errorf("prune batch plan scans the table:\n%s", plan)
	}
}

// The listing query gains from the same index: it can now walk it
// backwards instead of sorting every job in the status to return a page.
func TestListNoLongerSortsEveryJobInTheStatus(t *testing.T) {
	q := planQueue(t)

	plan := queryPlan(t, q,
		`SELECT `+jobColumns+` FROM jobs WHERE status = ? ORDER BY updated_at DESC LIMIT ?`,
		string(job.StatusPending), 50)
	if strings.Contains(plan, "TEMP B-TREE") {
		t.Errorf("list plan still materialises a sort:\n%s", plan)
	}
	if !strings.Contains(plan, "idx_jobs_retention") {
		t.Errorf("list plan does not use idx_jobs_retention:\n%s", plan)
	}
}
