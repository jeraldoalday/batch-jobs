package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
	sqlitestore "github.com/jeraldoalday/batch-jobs/internal/schedule/sqlite"
)

// openTestStore returns a schedule.Store backed by a private, in-memory
// SQLite database that is closed automatically at the end of the test.
func openTestStore(t *testing.T) schedule.Store {
	t.Helper()
	s, err := sqlitestore.Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// mustDueSchedule creates and persists a schedule whose NextRunAt has
// already passed, so it is immediately eligible to fire.
func mustDueSchedule(t *testing.T, store schedule.Store, opts ...schedule.Option) *schedule.Schedule {
	t.Helper()
	sch, err := schedule.New("q", "0 0 1 1 *", []byte("payload"), opts...)
	if err != nil {
		t.Fatalf("schedule.New: %v", err)
	}
	sch.NextRunAt = time.Now().UTC().Add(-time.Minute)
	if err := store.Create(sch); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return sch
}

func TestSchedulerEnqueuesDueSchedule(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	sch := mustDueSchedule(t, store)

	s := &Scheduler{Store: store, Queue: q, PollInterval: 5 * time.Millisecond}
	s.Start(context.Background())
	defer s.Stop()

	waitFor(t, func() bool { return q.Len() == 1 })

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.Queue != sch.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, sch.Queue)
	}
	if string(got.Payload) != string(sch.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, sch.Payload)
	}
}

func TestSchedulerSkipsDisabledSchedule(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	sch := mustDueSchedule(t, store)
	if err := store.SetEnabled(sch.ID, false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}

	s := &Scheduler{Store: store, Queue: q, PollInterval: 5 * time.Millisecond}
	s.Start(context.Background())

	// Give the scheduler a few poll cycles to (not) act, then confirm
	// nothing was enqueued.
	time.Sleep(50 * time.Millisecond)
	s.Stop()

	if got := q.Len(); got != 0 {
		t.Fatalf("Queue.Len() = %d, want 0 (disabled schedule should never fire)", got)
	}
}

func TestSchedulerAdvancesNextRunAtAndLastRunAt(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	sch := mustDueSchedule(t, store)

	s := &Scheduler{Store: store, Queue: q, PollInterval: 5 * time.Millisecond}
	s.Start(context.Background())
	defer s.Stop()

	waitFor(t, func() bool { return q.Len() == 1 })

	var got *schedule.Schedule
	waitFor(t, func() bool {
		var err error
		got, err = store.Get(sch.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return !got.LastRunAt.IsZero()
	})

	if !got.NextRunAt.After(time.Now().UTC()) {
		t.Errorf("NextRunAt = %v, want a time in the future after firing", got.NextRunAt)
	}
}

// errBoom is a sentinel error injected by the wrapper stores and queue
// below, to exercise fire's remaining error paths without a fake that
// duplicates a real Store or Queue's own behavior.
var errBoom = errors.New("boom")

// TestSchedulerDisablesScheduleWhenAdvanceFails covers the same Advance
// failure as TestSchedulerRecordsMetricsForFireError (reusing its
// corruptingStore), but asserts fire's actual effect on the schedule and
// queue rather than the metric it also emits.
func TestSchedulerDisablesScheduleWhenAdvanceFails(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	sch := mustDueSchedule(t, store)

	s := &Scheduler{Store: &corruptingStore{Store: store}, Queue: q, PollInterval: 5 * time.Millisecond}
	s.Start(context.Background())
	defer s.Stop()

	waitFor(t, func() bool {
		got, err := store.Get(sch.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return !got.Enabled
	})

	if got := q.Len(); got != 0 {
		t.Errorf("Queue.Len() = %d, want 0 (a schedule that cannot advance should not enqueue a job)", got)
	}
}

// failingEnqueueQueue wraps a queue.Queue and makes every Enqueue fail,
// so a test can exercise fire's enqueue-failure path with a real queue
// behind it.
type failingEnqueueQueue struct {
	queue.Queue
}

func (f *failingEnqueueQueue) Enqueue(j *job.Job) error { return errBoom }

func TestSchedulerLeavesScheduleDueWhenEnqueueFails(t *testing.T) {
	store := openTestStore(t)
	sch := mustDueSchedule(t, store)
	originalNext := sch.NextRunAt

	s := &Scheduler{Store: store, Queue: &failingEnqueueQueue{Queue: queue.NewMemory()}, PollInterval: 5 * time.Millisecond}
	s.Start(context.Background())

	// Give the scheduler a few poll cycles to try and fail, then confirm
	// the schedule was neither disabled nor advanced: a transient queue
	// failure should leave it due again on the next tick, not drop it.
	time.Sleep(50 * time.Millisecond)
	s.Stop()

	got, err := store.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Enabled {
		t.Error("Enabled = false, want true (an enqueue failure should not disable the schedule)")
	}
	if !got.NextRunAt.Equal(originalNext) {
		t.Errorf("NextRunAt = %v, want unchanged %v (a failed fire should not advance it)", got.NextRunAt, originalNext)
	}
}

// failingMarkFiredStore wraps a schedule.Store and makes every MarkFired
// call fail, so a test can exercise fire's mark-fired-failure path
// against a real store.
type failingMarkFiredStore struct {
	schedule.Store
}

func (f *failingMarkFiredStore) MarkFired(id string, firedAt, next time.Time) error {
	return errBoom
}

func TestSchedulerLeavesScheduleDueWhenMarkFiredFails(t *testing.T) {
	store := openTestStore(t)
	q := queue.NewMemory()
	sch := mustDueSchedule(t, store)
	originalNext := sch.NextRunAt

	s := &Scheduler{Store: &failingMarkFiredStore{Store: store}, Queue: q, PollInterval: 5 * time.Millisecond}
	s.Start(context.Background())
	defer s.Stop()

	// The job still gets enqueued -- MarkFired is a separate step after
	// Queue.Enqueue -- but a store that cannot record the fire must not
	// silently advance the schedule in memory either.
	waitFor(t, func() bool { return q.Len() == 1 })

	got, err := store.Get(sch.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.NextRunAt.Equal(originalNext) {
		t.Errorf("NextRunAt = %v, want unchanged %v (a failed MarkFired should leave the stored schedule due)", got.NextRunAt, originalNext)
	}
}

// blockingDueStore wraps a schedule.Store and makes its first Due call
// signal started and then block on release, letting a test control
// exactly when a tick's work happens relative to Stop.
type blockingDueStore struct {
	schedule.Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingDueStore) Due(now time.Time) ([]*schedule.Schedule, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return b.Store.Due(now)
}

func TestSchedulerStopWaitsForInFlightTick(t *testing.T) {
	store := &blockingDueStore{
		Store:   openTestStore(t),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	q := queue.NewMemory()

	s := &Scheduler{Store: store, Queue: q, PollInterval: 5 * time.Millisecond}
	s.Start(context.Background())

	<-store.started

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned before the in-flight tick finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after the in-flight tick finished")
	}
}

// waitFor polls cond until it returns true or fails the test after a
// timeout, to avoid flaky sleeps in concurrency tests.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
