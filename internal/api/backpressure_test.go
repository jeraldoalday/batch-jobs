package api

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

func TestHandleEnqueueQueueFullReturns503(t *testing.T) {
	store := newFakeStore()
	store.enqueueErr = queue.ErrFull
	s := &Server{Store: store}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "emails"})

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want %q", got, "1")
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
}

// ErrFull may reach the handler wrapped by a Store that adds context; the
// mapping must still be by errors.Is, not by equality.
func TestHandleEnqueueWrappedErrFullReturns503(t *testing.T) {
	store := newFakeStore()
	store.enqueueErr = fmt.Errorf("sqlite: enqueue abc: %w", queue.ErrFull)
	s := &Server{Store: store}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "emails"})

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d for a wrapped ErrFull", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleEnqueueRetryAfterOverride(t *testing.T) {
	tests := []struct {
		name  string
		after time.Duration
		want  string
	}{
		{"default", 0, "1"},
		{"whole seconds", 30 * time.Second, "30"},
		{"rounds up to the next second", 1500 * time.Millisecond, "2"},
		{"never below one second", 100 * time.Millisecond, "1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			store.enqueueErr = queue.ErrFull
			s := &Server{Store: store, RetryAfter: tt.after}
			h := s.Handler()

			rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "emails"})

			if got := rec.Header().Get("Retry-After"); got != tt.want {
				t.Errorf("Retry-After = %q, want %q", got, tt.want)
			}
		})
	}
}

// Only ErrFull gets the retryable treatment; every other Enqueue failure
// stays a 500 with no Retry-After hint.
func TestHandleEnqueueOtherErrorsStay500(t *testing.T) {
	store := newFakeStore()
	store.enqueueErr = errBoom
	s := &Server{Store: store}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "emails"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After = %q, want it unset on a 500", got)
	}
}

// A bounded queue behind the API should produce 201s until it fills and
// 503s afterward, with no request ever failing in a way a client would read
// as its own fault.
func TestHandleEnqueueBackpressureSequence(t *testing.T) {
	const capacity = 3

	store := newFakeStore()
	bounded := queue.NewMemory(queue.WithCapacity(capacity))
	store.enqueueFunc = bounded.Enqueue

	s := &Server{Store: store}
	h := s.Handler()

	var created, rejected int
	for i := 0; i < capacity*2; i++ {
		rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "emails"})
		switch rec.Code {
		case http.StatusCreated:
			created++
		case http.StatusServiceUnavailable:
			rejected++
		default:
			t.Fatalf("request %d: status = %d, want 201 or 503", i, rec.Code)
		}
	}

	if created != capacity {
		t.Errorf("created %d jobs, want %d", created, capacity)
	}
	if rejected != capacity {
		t.Errorf("rejected %d jobs, want %d", rejected, capacity)
	}
}
