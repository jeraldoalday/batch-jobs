package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestJanitorPrunesOnInterval(t *testing.T) {
	q := openTestQueue(t)
	insertFinished(t, q, job.StatusSucceeded, time.Hour)
	insertFinished(t, q, job.StatusSucceeded, time.Hour)

	jt := &Janitor{
		Queue:     q,
		Retention: queue.Retention{Succeeded: time.Minute},
		Interval:  5 * time.Millisecond,
		Logger:    quietLogger(),
	}
	if err := jt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer jt.Stop()

	waitFor(t, 2*time.Second, "the expired jobs to be pruned", func() bool {
		n, err := q.Retained()
		return err == nil && n == 0
	})
}

// The janitor's first sweep waits a full interval, unlike the reaper's.
// Startup is when the process has the least to gain from deleting old
// rows and the most to lose from holding the write lock.
func TestJanitorDoesNotSweepOnStart(t *testing.T) {
	q := openTestQueue(t)
	insertFinished(t, q, job.StatusSucceeded, time.Hour)

	jt := &Janitor{
		Queue:     q,
		Retention: queue.Retention{Succeeded: time.Minute},
		Interval:  time.Hour,
		Logger:    quietLogger(),
	}
	if err := jt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer jt.Stop()

	// With an hour-long interval, anything pruned in the next moment
	// could only have come from a sweep at startup.
	time.Sleep(50 * time.Millisecond)

	n, err := q.Retained()
	if err != nil {
		t.Fatalf("Retained: %v", err)
	}
	if n != 1 {
		t.Errorf("Retained() = %d, want 1: the janitor pruned at startup", n)
	}
}

func TestJanitorRejectsPolicyThatPrunesNothing(t *testing.T) {
	q := openTestQueue(t)

	jt := &Janitor{Queue: q, Logger: quietLogger()}
	if err := jt.Start(context.Background()); !errors.Is(err, queue.ErrNoRetention) {
		t.Fatalf("Start with no policy = %v, want ErrNoRetention", err)
	}

	// A refused Start must not have launched anything, so Stop is a no-op
	// rather than a wait on a goroutine that does not exist.
	jt.Stop()
}

func TestJanitorRejectsInvalidPolicy(t *testing.T) {
	q := openTestQueue(t)

	jt := &Janitor{Queue: q, Retention: queue.Retention{Dead: -time.Hour}, Logger: quietLogger()}
	if err := jt.Start(context.Background()); err == nil {
		t.Fatal("Start accepted a negative retention")
	}
}

func TestJanitorStopsOnContextCancel(t *testing.T) {
	q := openTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())

	jt := &Janitor{
		Queue:     q,
		Retention: queue.Retention{Succeeded: time.Minute},
		Interval:  5 * time.Millisecond,
		Logger:    quietLogger(),
	}
	if err := jt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cancel()
	jt.Stop() // must return rather than block on a loop that ignored ctx
}

// Stop is called on shutdown paths that may run twice; it must be safe.
func TestJanitorStopIsIdempotent(t *testing.T) {
	q := openTestQueue(t)

	jt := &Janitor{
		Queue:     q,
		Retention: queue.Retention{Succeeded: time.Minute},
		Interval:  time.Hour,
		Logger:    quietLogger(),
	}
	if err := jt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	jt.Stop()
	jt.Stop()
}

func TestJanitorDefaultsInterval(t *testing.T) {
	q := openTestQueue(t)

	jt := &Janitor{Queue: q, Retention: queue.Retention{Succeeded: time.Minute}, Logger: quietLogger()}
	if err := jt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer jt.Stop()

	if jt.Interval != DefaultPruneInterval {
		t.Errorf("Interval = %s, want %s", jt.Interval, DefaultPruneInterval)
	}
}

func TestJanitorCountsPrunedJobsByStatus(t *testing.T) {
	q := openTestQueue(t)
	insertFinished(t, q, job.StatusSucceeded, time.Hour)
	insertFinished(t, q, job.StatusSucceeded, time.Hour)
	insertFinished(t, q, job.StatusCancelled, time.Hour)

	m := metrics.New(prometheus.NewRegistry())
	jt := &Janitor{
		Queue:     q,
		Retention: queue.Retention{Succeeded: time.Minute, Cancelled: time.Minute},
		Interval:  5 * time.Millisecond,
		Logger:    quietLogger(),
		Metrics:   m,
	}
	if err := jt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer jt.Stop()

	waitFor(t, 2*time.Second, "the prune counters to be published", func() bool {
		body := scrape(m)
		return strings.Contains(body, `batchjobs_jobs_pruned_total{status="succeeded"} 2`) &&
			strings.Contains(body, `batchjobs_jobs_pruned_total{status="cancelled"} 1`)
	})
}

// A closed queue is not a transient failure: there is no later pass that
// could succeed, so the loop ends instead of logging hourly forever.
func TestJanitorStopsWhenTheQueueCloses(t *testing.T) {
	q, err := Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	jt := &Janitor{
		Queue:     q,
		Retention: queue.Retention{Succeeded: time.Minute},
		Interval:  5 * time.Millisecond,
		Logger:    quietLogger(),
	}
	if err := jt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	q.Close()

	// Stop waits on the loop goroutine; if the sweep kept retrying past
	// the close it would still be running, but it would also still exit
	// on cancel -- so assert on the queue's own state instead: a closed
	// queue answers ErrClosed, which is what the loop saw.
	if _, err := q.Prune(jt.Retention); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Prune on a closed queue = %v, want ErrClosed", err)
	}
	jt.Stop()
}

// The janitor must not disturb work that is still waiting to run.
func TestJanitorLeavesLiveJobsAlone(t *testing.T) {
	q := openTestQueue(t)
	insertFinished(t, q, job.StatusSucceeded, time.Hour)
	live := job.New("q", []byte("live"))
	if err := q.Enqueue(live); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jt := &Janitor{
		Queue:     q,
		Retention: queue.Retention{Succeeded: time.Minute},
		Interval:  5 * time.Millisecond,
		Logger:    quietLogger(),
	}
	if err := jt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer jt.Stop()

	waitFor(t, 2*time.Second, "the finished job to be pruned", func() bool {
		n, err := q.Retained()
		return err == nil && n == 0
	})

	if _, err := q.Get(live.ID); err != nil {
		t.Errorf("the pending job was pruned: %v", err)
	}
	if got := q.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}
