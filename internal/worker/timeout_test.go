package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestPoolCancelsContextOnJobTimeout(t *testing.T) {
	q := queue.NewMemory()
	dlq := queue.NewMemory()
	cancelled := make(chan struct{})

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		},
		Concurrency: 1,
		Backoff:     ConstantBackoff(time.Millisecond),
		DeadLetter:  dlq,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	j := job.New("q", nil, job.WithTimeout(20*time.Millisecond), job.WithMaxRetries(0))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler's context was never cancelled")
	}

	waitFor(t, func() bool { return dlq.Len() == 1 })

	got, err := dlq.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue from dead letter queue: %v", err)
	}
	if got.LastError != context.DeadlineExceeded.Error() {
		t.Errorf("LastError = %q, want %q", got.LastError, context.DeadlineExceeded.Error())
	}
}

func TestPoolDoesNotCancelWithoutTimeout(t *testing.T) {
	q := queue.NewMemory()
	done := make(chan error, 1)

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			select {
			case <-ctx.Done():
				done <- ctx.Err()
			case <-time.After(50 * time.Millisecond):
				done <- nil
			}
			return nil
		},
		Concurrency: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	if err := q.Enqueue(job.New("q", nil)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler's context was cancelled unexpectedly: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never observed its 50ms window")
	}
}

func TestPoolPermanentErrorSkipsRetryAndDeadLetter(t *testing.T) {
	q := queue.NewMemory()
	dlq := queue.NewMemory()
	var attempts atomic.Int32

	p := &Pool{
		Queue: q,
		Handler: func(ctx context.Context, j *job.Job) error {
			attempts.Add(1)
			return Permanent(errors.New("malformed payload"))
		},
		Concurrency: 1,
		Backoff:     ConstantBackoff(time.Millisecond),
		DeadLetter:  dlq,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	defer func() {
		cancel()
		p.Stop()
	}()

	j := job.New("q", nil, job.WithMaxRetries(5))
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, func() bool { return attempts.Load() == 1 })

	// Give the pool a little longer: if it were (incorrectly) retrying,
	// a second attempt would show up here.
	time.Sleep(50 * time.Millisecond)
	if n := attempts.Load(); n != 1 {
		t.Errorf("attempts = %d, want 1 (no retries for a permanent error)", n)
	}
	if dlq.Len() != 0 {
		t.Errorf("dead-letter queue length = %d, want 0", dlq.Len())
	}
}

func TestIsPermanentUnwraps(t *testing.T) {
	base := errors.New("boom")
	wrapped := Permanent(base)
	if !isPermanent(wrapped) {
		t.Error("isPermanent(Permanent(err)) = false, want true")
	}
	if isPermanent(base) {
		t.Error("isPermanent(err) = true, want false for an unwrapped error")
	}
	if isPermanent(nil) {
		t.Error("isPermanent(nil) = true, want false")
	}
}

func TestPermanentNilReturnsNil(t *testing.T) {
	if err := Permanent(nil); err != nil {
		t.Errorf("Permanent(nil) = %v, want nil", err)
	}
}
