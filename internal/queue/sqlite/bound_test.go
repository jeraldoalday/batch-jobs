package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestBoundDequeueOnlyClaimsItsOwnQueue(t *testing.T) {
	q := openTestQueue(t)

	emails := q.Bind("emails")
	reports := q.Bind("reports")

	report := job.New("reports", []byte("weekly"))
	email := job.New("emails", []byte("welcome"))
	for _, j := range []*job.Job{report, email} {
		if err := q.Enqueue(j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// The report was enqueued first, so an unrestricted TryDequeue would
	// return it; the bound view must skip past it to the email.
	got, err := emails.TryDequeue()
	if err != nil {
		t.Fatalf("emails.TryDequeue: %v", err)
	}
	if got.ID != email.ID {
		t.Errorf("emails view claimed %q, want the email %q", got.ID, email.ID)
	}

	got, err = reports.TryDequeue()
	if err != nil {
		t.Fatalf("reports.TryDequeue: %v", err)
	}
	if got.ID != report.ID {
		t.Errorf("reports view claimed %q, want the report %q", got.ID, report.ID)
	}
}

func TestBoundTryDequeueEmptyWhenOnlyOtherQueuesHaveWork(t *testing.T) {
	q := openTestQueue(t)

	if err := q.Enqueue(job.New("reports", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, err := q.Bind("emails").TryDequeue(); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("emails.TryDequeue: err = %v, want ErrEmpty", err)
	}
}

func TestBoundRespectsPriorityWithinItsQueue(t *testing.T) {
	q := openTestQueue(t)

	low := job.New("emails", []byte("low"), job.WithPriority(job.PriorityLow))
	high := job.New("emails", []byte("high"), job.WithPriority(job.PriorityHigh))
	for _, j := range []*job.Job{low, high} {
		if err := q.Enqueue(j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	got, err := q.Bind("emails").TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.ID != high.ID {
		t.Errorf("claimed %q, want the high-priority job %q", got.ID, high.ID)
	}
}

func TestBoundName(t *testing.T) {
	q := openTestQueue(t)
	if got := q.Bind("emails").Name(); got != "emails" {
		t.Errorf("Name() = %q, want %q", got, "emails")
	}
}

func TestBoundLenCountsOnlyItsOwnQueue(t *testing.T) {
	q := openTestQueue(t)

	for i := 0; i < 3; i++ {
		if err := q.Enqueue(job.New("emails", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := q.Enqueue(job.New("reports", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := q.Bind("emails").Len(); got != 3 {
		t.Errorf("emails Len() = %d, want 3", got)
	}
	if got := q.Bind("reports").Len(); got != 1 {
		t.Errorf("reports Len() = %d, want 1", got)
	}
	if got := q.Len(); got != 4 {
		t.Errorf("unrestricted Len() = %d, want 4", got)
	}
}

func TestBoundEnqueueRejectsForeignJobs(t *testing.T) {
	q := openTestQueue(t)
	emails := q.Bind("emails")

	err := emails.Enqueue(job.New("reports", nil))
	if !errors.Is(err, queue.ErrNoRoute) {
		t.Fatalf("Enqueue of a foreign job: err = %v, want ErrNoRoute", err)
	}
	if got := q.Len(); got != 0 {
		t.Errorf("Len() = %d after a rejected Enqueue, want 0", got)
	}

	if err := emails.Enqueue(job.New("emails", nil)); err != nil {
		t.Fatalf("Enqueue of a matching job: %v", err)
	}
	if got := emails.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

// A blocked Dequeue on one view must not be woken by traffic on another,
// and must return as soon as its own queue gets a job.
func TestBoundDequeueBlocksUntilItsOwnQueueHasWork(t *testing.T) {
	q := openTestQueue(t)
	emails := q.Bind("emails")

	type result struct {
		j   *job.Job
		err error
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		j, err := emails.Dequeue(ctx)
		done <- result{j, err}
	}()

	if err := q.Enqueue(job.New("reports", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case r := <-done:
		t.Fatalf("emails.Dequeue returned %v/%v on another queue's job", r.j, r.err)
	case <-time.After(50 * time.Millisecond):
	}

	want := job.New("emails", nil)
	if err := q.Enqueue(want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("emails.Dequeue: %v", r.err)
		}
		if r.j.ID != want.ID {
			t.Errorf("claimed %q, want %q", r.j.ID, want.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("emails.Dequeue did not return after its own queue got a job")
	}
}

// Closing a view must not close the shared database out from under the
// Queue or any sibling view.
func TestBoundCloseDoesNotCloseTheQueue(t *testing.T) {
	q := openTestQueue(t)
	emails := q.Bind("emails")

	if err := emails.Close(); err != nil {
		t.Fatalf("Bound.Close: %v", err)
	}
	if err := q.Enqueue(job.New("emails", nil)); err != nil {
		t.Errorf("Enqueue after closing a view: %v", err)
	}
	if _, err := emails.TryDequeue(); err != nil {
		t.Errorf("TryDequeue after closing the view: %v", err)
	}
}

// Once the underlying Queue is closed, its views must report it rather
// than failing obscurely against a closed pool.
func TestBoundReportsClosedQueue(t *testing.T) {
	q := openTestQueue(t)
	emails := q.Bind("emails")

	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := emails.TryDequeue(); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("TryDequeue on a closed queue: err = %v, want ErrClosed", err)
	}
	if err := emails.Enqueue(job.New("emails", nil)); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("Enqueue on a closed queue: err = %v, want ErrClosed", err)
	}
}

// The end this was built for: a Router weighting several bound views over
// a single database file.
func TestBoundRoutedThroughRouter(t *testing.T) {
	q := openTestQueue(t)

	r, err := queue.NewRouter([]queue.Route{
		{Name: "payments", Queue: q.Bind("payments"), Weight: 3},
		{Name: "emails", Queue: q.Bind("emails"), Weight: 1},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	defer r.Close()

	for i := 0; i < 6; i++ {
		if err := r.Enqueue(job.New("payments", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := r.Enqueue(job.New("emails", nil)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if got := r.Len(); got != 8 {
		t.Fatalf("Len() = %d, want 8", got)
	}

	var order string
	for i := 0; i < 8; i++ {
		j, err := r.TryDequeue()
		if err != nil {
			t.Fatalf("TryDequeue %d: %v", i, err)
		}
		order += string(j.Queue[0])
	}
	if want := "pppepppe"; order != want {
		t.Errorf("dequeue order = %q, want %q (3:1 weighting)", order, want)
	}
}
