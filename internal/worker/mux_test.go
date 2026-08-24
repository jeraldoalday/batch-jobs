package worker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestMuxDispatchesByQueue(t *testing.T) {
	mux := NewMux()

	var got []string
	var mu sync.Mutex
	record := func(name string) Handler {
		return func(ctx context.Context, j *job.Job) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, name+":"+j.ID)
			return nil
		}
	}

	mux.Handle("emails", record("emails"))
	mux.Handle("reports", record("reports"))

	emailJob := job.New("emails", nil)
	emailJob.ID = "e1"
	reportJob := job.New("reports", nil)
	reportJob.ID = "r1"

	for _, j := range []*job.Job{emailJob, reportJob, emailJob} {
		if err := mux.Serve(context.Background(), j); err != nil {
			t.Fatalf("Serve(%s) = %v, want nil", j.Queue, err)
		}
	}

	want := []string{"emails:e1", "reports:r1", "emails:e1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dispatched %v, want %v", got, want)
	}
}

func TestMuxUnregisteredQueueIsPermanent(t *testing.T) {
	mux := NewMux()
	mux.Handle("emails", func(context.Context, *job.Job) error { return nil })

	err := mux.Serve(context.Background(), job.New("reports", nil))
	if err == nil {
		t.Fatal("Serve for an unregistered queue returned nil, want an error")
	}
	if !errors.Is(err, ErrNoHandler) {
		t.Errorf("Serve error = %v, want it to wrap ErrNoHandler", err)
	}
	// A job nothing knows how to run must not be retried: the pool has to
	// see this as permanent, or the job burns its attempts on nothing.
	if !isPermanent(err) {
		t.Errorf("Serve error = %v, want a Permanent error", err)
	}
	// The queue name belongs in the message; without it an operator
	// reading a dead job's last_error cannot tell which registration is
	// missing.
	if !contains(err.Error(), "reports") {
		t.Errorf("Serve error = %q, want it to name the queue", err)
	}
}

func TestMuxFallbackCatchesUnregistered(t *testing.T) {
	mux := NewMux()
	mux.Handle("emails", func(context.Context, *job.Job) error {
		return errors.New("emails handler should not have run")
	})

	var caught string
	mux.Fallback(func(_ context.Context, j *job.Job) error {
		caught = j.Queue
		return nil
	})

	if err := mux.Serve(context.Background(), job.New("unknown", nil)); err != nil {
		t.Fatalf("Serve with a fallback = %v, want nil", err)
	}
	if caught != "unknown" {
		t.Errorf("fallback saw queue %q, want %q", caught, "unknown")
	}
}

func TestMuxFallbackDoesNotShadowRegistered(t *testing.T) {
	mux := NewMux()

	var ran string
	mux.Handle("emails", func(context.Context, *job.Job) error {
		ran = "registered"
		return nil
	})
	mux.Fallback(func(context.Context, *job.Job) error {
		ran = "fallback"
		return nil
	})

	if err := mux.Serve(context.Background(), job.New("emails", nil)); err != nil {
		t.Fatalf("Serve = %v, want nil", err)
	}
	if ran != "registered" {
		t.Errorf("ran %q handler, want the registered one to win", ran)
	}
}

func TestMuxFallbackNilRestoresDefault(t *testing.T) {
	mux := NewMux()
	mux.Fallback(func(context.Context, *job.Job) error { return nil })

	if err := mux.Serve(context.Background(), job.New("unknown", nil)); err != nil {
		t.Fatalf("Serve with a fallback = %v, want nil", err)
	}

	mux.Fallback(nil)
	if err := mux.Serve(context.Background(), job.New("unknown", nil)); !errors.Is(err, ErrNoHandler) {
		t.Errorf("after clearing the fallback, Serve = %v, want ErrNoHandler", err)
	}
}

func TestMuxPropagatesHandlerError(t *testing.T) {
	want := errors.New("boom")
	mux := NewMux()
	mux.Handle("emails", func(context.Context, *job.Job) error { return want })

	if err := mux.Serve(context.Background(), job.New("emails", nil)); !errors.Is(err, want) {
		t.Errorf("Serve = %v, want the handler's own error %v", err, want)
	}
}

func TestMuxPassesContextThrough(t *testing.T) {
	type ctxKey struct{}

	mux := NewMux()
	var seen any
	mux.Handle("emails", func(ctx context.Context, _ *job.Job) error {
		seen = ctx.Value(ctxKey{})
		return nil
	})

	ctx := context.WithValue(context.Background(), ctxKey{}, "value")
	if err := mux.Serve(ctx, job.New("emails", nil)); err != nil {
		t.Fatalf("Serve = %v, want nil", err)
	}
	if seen != "value" {
		t.Errorf("handler saw context value %v, want %q", seen, "value")
	}
}

func TestMuxHandleFunc(t *testing.T) {
	mux := NewMux()
	called := false
	mux.HandleFunc("emails", func(context.Context, *job.Job) error {
		called = true
		return nil
	})

	if err := mux.Serve(context.Background(), job.New("emails", nil)); err != nil {
		t.Fatalf("Serve = %v, want nil", err)
	}
	if !called {
		t.Error("handler registered with HandleFunc did not run")
	}
}

func TestMuxQueues(t *testing.T) {
	mux := NewMux()
	if got := mux.Queues(); len(got) != 0 {
		t.Errorf("empty Mux.Queues() = %v, want none", got)
	}

	noop := func(context.Context, *job.Job) error { return nil }
	mux.Handle("reports", noop)
	mux.Handle("emails", noop)
	mux.Handle("webhooks", noop)
	mux.Fallback(noop)

	// Sorted, and the unnamed fallback is not one of them.
	want := []string{"emails", "reports", "webhooks"}
	if got := mux.Queues(); !reflect.DeepEqual(got, want) {
		t.Errorf("Queues() = %v, want %v", got, want)
	}
}

func TestMuxZeroValueUsable(t *testing.T) {
	var mux Mux
	mux.Handle("emails", func(context.Context, *job.Job) error { return nil })

	if err := mux.Serve(context.Background(), job.New("emails", nil)); err != nil {
		t.Fatalf("zero-value Mux Serve = %v, want nil", err)
	}
}

func TestMuxHandlePanics(t *testing.T) {
	noop := Handler(func(context.Context, *job.Job) error { return nil })

	tests := []struct {
		name  string
		setup func(m *Mux)
	}{
		{"empty name", func(m *Mux) { m.Handle("", noop) }},
		{"nil handler", func(m *Mux) { m.Handle("emails", nil) }},
		{"duplicate registration", func(m *Mux) {
			m.Handle("emails", noop)
			m.Handle("emails", noop)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic, got none")
				}
			}()
			tt.setup(NewMux())
		})
	}
}

func TestMuxConcurrentRegistrationAndDispatch(t *testing.T) {
	mux := NewMux()

	const queues = 16
	var wg sync.WaitGroup
	for i := range queues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("queue-%d", i)
			mux.Handle(name, func(context.Context, *job.Job) error { return nil })
		}()
	}
	// Dispatch concurrently with registration. Every job either finds its
	// handler or falls through to ErrNoHandler; the point is that the race
	// detector sees no unsynchronized access either way.
	for i := range queues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := job.New(fmt.Sprintf("queue-%d", i), nil)
			if err := mux.Serve(context.Background(), j); err != nil && !errors.Is(err, ErrNoHandler) {
				t.Errorf("Serve = %v, want nil or ErrNoHandler", err)
			}
		}()
	}
	wg.Wait()

	if got := len(mux.Queues()); got != queues {
		t.Errorf("registered %d queues, want %d", got, queues)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
