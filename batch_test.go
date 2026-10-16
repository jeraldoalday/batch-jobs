package batchjobs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	batchjobs "github.com/jeraldoalday/batch-jobs"
)

// batchEngine opens an ephemeral engine for a batch test.
func batchEngine(t *testing.T) *batchjobs.Engine {
	t.Helper()
	e, err := batchjobs.Open(batchjobs.Config{DB: batchjobs.MemoryDB})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestEngineEnqueueBatch(t *testing.T) {
	e := batchEngine(t)

	subs := make([]batchjobs.Submission, 25)
	for i := range subs {
		subs[i] = batchjobs.Submission{Queue: "emails", Payload: []byte{byte(i)}}
	}

	jobs, err := e.EnqueueBatch(t.Context(), subs)
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if len(jobs) != len(subs) {
		t.Fatalf("EnqueueBatch returned %d jobs, want %d", len(jobs), len(subs))
	}

	seen := make(map[string]bool, len(jobs))
	for i, j := range jobs {
		if j.ID == "" {
			t.Errorf("job %d has no ID", i)
		}
		if seen[j.ID] {
			t.Errorf("job %d repeats ID %s", i, j.ID)
		}
		seen[j.ID] = true
		if j.Queue != "emails" {
			t.Errorf("job %d is on queue %q, want %q", i, j.Queue, "emails")
		}
		if j.Status != batchjobs.StatusPending {
			t.Errorf("job %d has status %q, want pending", i, j.Status)
		}
		if len(j.Payload) != 1 || j.Payload[0] != byte(i) {
			t.Errorf("job %d has payload %v, want the one submitted", i, j.Payload)
		}
	}

	if got := e.Len(); got != len(subs) {
		t.Errorf("Len() = %d, want %d", got, len(subs))
	}
}

func TestEngineEnqueueBatchPreservesOrder(t *testing.T) {
	e := batchEngine(t)

	subs := []batchjobs.Submission{
		{Queue: "a", Payload: []byte("first")},
		{Queue: "b", Payload: []byte("second")},
		{Queue: "c", Payload: []byte("third")},
	}
	jobs, err := e.EnqueueBatch(t.Context(), subs)
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	for i, want := range []string{"a", "b", "c"} {
		if jobs[i].Queue != want {
			t.Errorf("jobs[%d] is on queue %q, want %q: order must match the submissions", i, jobs[i].Queue, want)
		}
	}
}

func TestEngineEnqueueBatchAppliesOptions(t *testing.T) {
	e := batchEngine(t)

	jobs, err := e.EnqueueBatch(t.Context(), []batchjobs.Submission{{
		Queue:   "reports",
		Payload: []byte("x"),
		Options: []batchjobs.Option{
			batchjobs.WithPriority(batchjobs.PriorityHigh),
			batchjobs.WithMaxRetries(7),
			batchjobs.WithTimeout(30 * time.Second),
		},
	}})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	j := jobs[0]
	if j.Priority != batchjobs.PriorityHigh {
		t.Errorf("Priority = %v, want high", j.Priority)
	}
	if j.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7", j.MaxRetries)
	}
	if j.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", j.Timeout)
	}
}

func TestEngineEnqueueBatchEmpty(t *testing.T) {
	e := batchEngine(t)

	jobs, err := e.EnqueueBatch(t.Context(), nil)
	if err != nil {
		t.Errorf("EnqueueBatch(nil) = %v, want nil", err)
	}
	if len(jobs) != 0 {
		t.Errorf("EnqueueBatch(nil) returned %d jobs, want 0", len(jobs))
	}
}

func TestEngineEnqueueBatchRejectsMissingQueueName(t *testing.T) {
	e := batchEngine(t)

	_, err := e.EnqueueBatch(t.Context(), []batchjobs.Submission{
		{Queue: "ok", Payload: nil},
		{Queue: "", Payload: nil},
	})
	if err == nil {
		t.Fatal("EnqueueBatch with an unnamed queue succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "1") {
		t.Errorf("error %q does not identify which entry is at fault", err)
	}
	if got := e.Len(); got != 0 {
		t.Errorf("Len() = %d after a rejected batch, want 0", got)
	}
}

func TestEngineEnqueueBatchRejectsCancelledContext(t *testing.T) {
	e := batchEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := e.EnqueueBatch(ctx, []batchjobs.Submission{{Queue: "q"}}); !errors.Is(err, context.Canceled) {
		t.Errorf("EnqueueBatch with a cancelled context = %v, want context.Canceled", err)
	}
	if got := e.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0: a cancelled call must not write", got)
	}
}

func TestEngineEnqueueBatchDedupesOnIdempotencyKey(t *testing.T) {
	e := batchEngine(t)

	original, err := e.Enqueue(t.Context(), "emails", []byte("first"),
		batchjobs.WithIdempotencyKey("welcome-42"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := e.EnqueueBatch(t.Context(), []batchjobs.Submission{
		{Queue: "emails", Payload: []byte("second"), Options: []batchjobs.Option{batchjobs.WithIdempotencyKey("welcome-42")}},
		{Queue: "emails", Payload: []byte("third")},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	if jobs[0].ID != original.ID {
		t.Errorf("deduped job has ID %s, want the original's %s", jobs[0].ID, original.ID)
	}
	if got := e.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2: the repeat must not create a third job", got)
	}
}

func TestEngineEnqueueBatchJSON(t *testing.T) {
	e := batchEngine(t)

	type message struct {
		To string `json:"to"`
	}
	values := []any{message{To: "a@example.com"}, message{To: "b@example.com"}}

	jobs, err := e.EnqueueBatchJSON(t.Context(), "emails", values)
	if err != nil {
		t.Fatalf("EnqueueBatchJSON: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("EnqueueBatchJSON returned %d jobs, want 2", len(jobs))
	}

	var got message
	if err := jobs[1].Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.To != "b@example.com" {
		t.Errorf("second job decodes to %q, want %q", got.To, "b@example.com")
	}
}

func TestEngineEnqueueBatchJSONRejectsIdempotencyKey(t *testing.T) {
	e := batchEngine(t)

	values := []any{"a", "b"}
	_, err := e.EnqueueBatchJSON(t.Context(), "q", values, batchjobs.WithIdempotencyKey("shared"))
	if err == nil {
		t.Fatal("EnqueueBatchJSON with a shared idempotency key succeeded, want an error")
	}
	if got := e.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0: a rejected batch must not enqueue any of it", got)
	}
}

func TestEngineEnqueueBatchJSONRejectsUnmarshalableValue(t *testing.T) {
	e := batchEngine(t)

	if _, err := e.EnqueueBatchJSON(t.Context(), "q", []any{make(chan int)}); err == nil {
		t.Fatal("EnqueueBatchJSON with an unmarshalable value succeeded, want an error")
	}
	if got := e.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
}

// TestEngineEnqueueBatchJobsRun is the end-to-end check: batched jobs are
// ordinary jobs, and a worker pool picks them up like any other.
func TestEngineEnqueueBatchJobsRun(t *testing.T) {
	e := batchEngine(t)

	const n = 20
	done := make(chan struct{}, n)
	e.Handle("work", func(context.Context, *batchjobs.Job) error {
		done <- struct{}{}
		return nil
	})
	if err := e.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	subs := make([]batchjobs.Submission, n)
	for i := range subs {
		subs[i] = batchjobs.Submission{Queue: "work"}
	}
	if _, err := e.EnqueueBatch(t.Context(), subs); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	deadline := time.After(15 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("only %d of %d batched jobs ran", i, n)
		}
	}
}
