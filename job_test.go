package batchjobs

import (
	"strings"
	"testing"
)

func TestStatusString(t *testing.T) {
	if got := StatusRetrying.String(); got != "retrying" {
		t.Errorf("String() = %q, want %q", got, "retrying")
	}
}

func TestPriorityString(t *testing.T) {
	cases := map[Priority]string{
		PriorityLow:    "low",
		PriorityNormal: "normal",
		PriorityHigh:   "high",
		Priority(99):   "priority(99)",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("Priority(%d).String() = %q, want %q", int(p), got, want)
		}
	}
}

// TestJobAttemptsLeftFloorsAtZero pins down the one branch
// TestEngineJobOptions (which only checks a freshly enqueued job with
// retries still remaining) doesn't reach: RetryCount can reach or exceed
// MaxRetries by the time a job is dead-lettered, and AttemptsLeft must not
// go negative once it does.
func TestJobAttemptsLeftFloorsAtZero(t *testing.T) {
	cases := []struct {
		maxRetries, retryCount int
	}{
		{maxRetries: 3, retryCount: 3},
		{maxRetries: 3, retryCount: 4},
	}
	for _, tt := range cases {
		j := &Job{MaxRetries: tt.maxRetries, RetryCount: tt.retryCount}
		if got := j.AttemptsLeft(); got != 0 {
			t.Errorf("AttemptsLeft() with MaxRetries=%d RetryCount=%d = %d, want 0", tt.maxRetries, tt.retryCount, got)
		}
	}
}

func TestJobDecodeRejectsAnEmptyPayload(t *testing.T) {
	j := &Job{ID: "job-1"}

	var v struct{}
	err := j.Decode(&v)
	if err == nil {
		t.Fatal("Decode with an empty payload = nil error, want one")
	}
	if !strings.Contains(err.Error(), "job-1") {
		t.Errorf("error %q does not name the job, want it to", err.Error())
	}
}

func TestJobDecodeWrapsAnInvalidJSONPayload(t *testing.T) {
	j := &Job{ID: "job-2", Payload: []byte("not json")}

	var v struct{}
	if err := j.Decode(&v); err == nil {
		t.Fatal("Decode with an invalid JSON payload = nil error, want one")
	}
}
