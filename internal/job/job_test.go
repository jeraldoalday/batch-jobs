package job

import (
	"errors"
	"testing"
	"time"
)

func TestNewDefaults(t *testing.T) {
	j := New("emails", []byte("hello"))

	if j.ID == "" {
		t.Fatal("expected non-empty ID")
	}
	if j.Queue != "emails" {
		t.Errorf("Queue = %q, want %q", j.Queue, "emails")
	}
	if j.Status != StatusPending {
		t.Errorf("Status = %q, want %q", j.Status, StatusPending)
	}
	if j.Priority != PriorityNormal {
		t.Errorf("Priority = %v, want %v", j.Priority, PriorityNormal)
	}
	if j.RunAt.Before(j.CreatedAt) || j.RunAt.After(j.CreatedAt.Add(time.Millisecond)) {
		t.Errorf("RunAt = %v, want ~= CreatedAt %v", j.RunAt, j.CreatedAt)
	}
}

func TestNewUniqueIDs(t *testing.T) {
	a := New("q", nil)
	b := New("q", nil)
	if a.ID == b.ID {
		t.Fatalf("expected distinct IDs, got %q twice", a.ID)
	}
}

func TestWithPriority(t *testing.T) {
	j := New("q", nil, WithPriority(PriorityHigh))
	if j.Priority != PriorityHigh {
		t.Errorf("Priority = %v, want %v", j.Priority, PriorityHigh)
	}
}

func TestWithMaxRetries(t *testing.T) {
	j := New("q", nil, WithMaxRetries(5))
	if j.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", j.MaxRetries)
	}
}

func TestWithDelay(t *testing.T) {
	j := New("q", nil, WithDelay(time.Hour))
	want := j.CreatedAt.Add(time.Hour)
	if !j.RunAt.Equal(want) {
		t.Errorf("RunAt = %v, want %v", j.RunAt, want)
	}
}

func TestWithRunAt(t *testing.T) {
	target := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	j := New("q", nil, WithRunAt(target))
	if !j.RunAt.Equal(target) {
		t.Errorf("RunAt = %v, want %v", j.RunAt, target)
	}
}

func TestWithTimeout(t *testing.T) {
	j := New("q", nil, WithTimeout(30*time.Second))
	if j.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want %v", j.Timeout, 30*time.Second)
	}
}

func TestWithTimeoutZeroByDefault(t *testing.T) {
	j := New("q", nil)
	if j.Timeout != 0 {
		t.Errorf("Timeout = %v, want 0", j.Timeout)
	}
}

func TestWithIdempotencyKey(t *testing.T) {
	j := New("q", nil, WithIdempotencyKey("order-123"))
	if j.IdempotencyKey != "order-123" {
		t.Errorf("IdempotencyKey = %q, want %q", j.IdempotencyKey, "order-123")
	}
}

func TestReady(t *testing.T) {
	now := time.Now().UTC()

	cases := []struct {
		name   string
		status Status
		runAt  time.Time
		at     time.Time
		want   bool
	}{
		{"pending and due", StatusPending, now, now, true},
		{"pending and past due", StatusPending, now.Add(-time.Minute), now, true},
		{"pending but scheduled in future", StatusPending, now.Add(time.Minute), now, false},
		{"running is never ready", StatusRunning, now.Add(-time.Minute), now, false},
		{"succeeded is never ready", StatusSucceeded, now.Add(-time.Minute), now, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := New("q", nil)
			j.Status = tc.status
			j.RunAt = tc.runAt
			if got := j.Ready(tc.at); got != tc.want {
				t.Errorf("Ready(%v) = %v, want %v", tc.at, got, tc.want)
			}
		})
	}
}

func TestCanRetry(t *testing.T) {
	j := New("q", nil, WithMaxRetries(2))

	if !j.CanRetry() {
		t.Fatal("expected CanRetry to be true with 0/2 retries used")
	}
	j.RetryCount = 2
	if j.CanRetry() {
		t.Fatal("expected CanRetry to be false with 2/2 retries used")
	}
}

func TestMarkRunning(t *testing.T) {
	j := New("q", nil)
	now := j.CreatedAt.Add(time.Second)
	j.MarkRunning(now)

	if j.Status != StatusRunning {
		t.Errorf("Status = %q, want %q", j.Status, StatusRunning)
	}
	if !j.UpdatedAt.Equal(now) {
		t.Errorf("UpdatedAt = %v, want %v", j.UpdatedAt, now)
	}
}

func TestMarkSucceeded(t *testing.T) {
	j := New("q", nil)
	j.LastError = "previous attempt failed"
	now := j.CreatedAt.Add(time.Second)

	j.MarkSucceeded(now)

	if j.Status != StatusSucceeded {
		t.Errorf("Status = %q, want %q", j.Status, StatusSucceeded)
	}
	if j.LastError != "" {
		t.Errorf("LastError = %q, want empty", j.LastError)
	}
}

func TestMarkFailedRetries(t *testing.T) {
	j := New("q", nil, WithMaxRetries(3))
	now := j.CreatedAt.Add(time.Second)

	j.MarkFailed(now, errors.New("boom"), 10*time.Second)

	if j.Status != StatusRetrying {
		t.Errorf("Status = %q, want %q", j.Status, StatusRetrying)
	}
	if j.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", j.RetryCount)
	}
	if j.LastError != "boom" {
		t.Errorf("LastError = %q, want %q", j.LastError, "boom")
	}
	wantRunAt := now.Add(10 * time.Second)
	if !j.RunAt.Equal(wantRunAt) {
		t.Errorf("RunAt = %v, want %v", j.RunAt, wantRunAt)
	}
}

func TestMarkFailedExhausted(t *testing.T) {
	j := New("q", nil, WithMaxRetries(1))
	j.RetryCount = 1 // already used its one retry
	now := j.CreatedAt.Add(time.Second)

	j.MarkFailed(now, errors.New("boom again"), time.Second)

	if j.Status != StatusDead {
		t.Errorf("Status = %q, want %q", j.Status, StatusDead)
	}
	if j.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want unchanged 1", j.RetryCount)
	}
}

func TestMarkFailedPermanently(t *testing.T) {
	j := New("q", nil, WithMaxRetries(5))
	now := j.CreatedAt.Add(time.Second)

	j.MarkFailedPermanently(now, errors.New("malformed payload"))

	if j.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", j.Status, StatusFailed)
	}
	if j.LastError != "malformed payload" {
		t.Errorf("LastError = %q, want %q", j.LastError, "malformed payload")
	}
	if j.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want unchanged 0", j.RetryCount)
	}
	if !j.UpdatedAt.Equal(now) {
		t.Errorf("UpdatedAt = %v, want %v", j.UpdatedAt, now)
	}
}

func TestStatusTerminal(t *testing.T) {
	terminal := []Status{StatusSucceeded, StatusFailed, StatusDead, StatusCancelled}
	nonTerminal := []Status{StatusPending, StatusRunning, StatusRetrying}

	for _, s := range terminal {
		if !s.Terminal() {
			t.Errorf("%q.Terminal() = false, want true", s)
		}
	}
	for _, s := range nonTerminal {
		if s.Terminal() {
			t.Errorf("%q.Terminal() = true, want false", s)
		}
	}
}

func TestStatusValid(t *testing.T) {
	if !StatusPending.Valid() {
		t.Error("StatusPending should be valid")
	}
	if Status("bogus").Valid() {
		t.Error(`Status("bogus") should not be valid`)
	}
}
