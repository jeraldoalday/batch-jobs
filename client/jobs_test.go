package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// jobJSON is a canned server response for a single job.
const jobJSON = `{
	"id": "job-1",
	"queue": "emails",
	"payload": "aGVsbG8=",
	"status": "pending",
	"priority": 1,
	"max_retries": 3,
	"retry_count": 1,
	"last_error": "smtp timeout",
	"created_at": "2026-08-24T10:00:00Z",
	"updated_at": "2026-08-24T10:01:00Z",
	"run_at": "2026-08-24T10:05:00Z",
	"timeout_seconds": 30,
	"idempotency_key": "key-1"
}`

func TestEnqueueEncodesRequest(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusCreated, jobJSON)

	runAt := time.Now().Add(2 * time.Minute)
	_, err := c.Enqueue(context.Background(), "emails", []byte("hello"),
		WithPriority(PriorityHigh),
		WithMaxRetries(3),
		WithTimeout(30*time.Second),
		WithIdempotencyKey("key-1"),
		WithRunAt(runAt),
	)
	if err != nil {
		t.Fatalf("Enqueue = %v", err)
	}

	method, rawURL, body, headers := s.lastRequest()
	if method != http.MethodPost || rawURL != "/v1/jobs" {
		t.Errorf("sent %s %s, want POST /v1/jobs", method, rawURL)
	}
	if got := headers.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}

	var sent enqueueRequest
	if err := json.Unmarshal([]byte(body), &sent); err != nil {
		t.Fatalf("request body is not JSON: %v (%s)", err, body)
	}
	if sent.Queue != "emails" {
		t.Errorf("queue = %q, want emails", sent.Queue)
	}
	if string(sent.Payload) != "hello" {
		t.Errorf("payload = %q, want hello", sent.Payload)
	}
	if sent.Priority == nil || *sent.Priority != int(PriorityHigh) {
		t.Errorf("priority = %v, want %d", sent.Priority, PriorityHigh)
	}
	if sent.MaxRetries != 3 {
		t.Errorf("max_retries = %d, want 3", sent.MaxRetries)
	}
	if sent.TimeoutSeconds != 30 {
		t.Errorf("timeout_seconds = %d, want 30", sent.TimeoutSeconds)
	}
	if sent.IdempotencyKey != "key-1" {
		t.Errorf("idempotency_key = %q, want key-1", sent.IdempotencyKey)
	}
	// WithRunAt is a delay computed at send time, so it lands near 120s
	// without being exactly it.
	if sent.DelaySeconds < 118 || sent.DelaySeconds > 121 {
		t.Errorf("delay_seconds = %d, want ~120", sent.DelaySeconds)
	}
}

func TestEnqueueDecodesJob(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusCreated, jobJSON)

	j, err := c.Enqueue(context.Background(), "emails", []byte("hello"))
	if err != nil {
		t.Fatalf("Enqueue = %v", err)
	}

	if j.ID != "job-1" || j.Queue != "emails" {
		t.Errorf("job = %s/%s, want job-1/emails", j.ID, j.Queue)
	}
	if string(j.Payload) != "hello" {
		t.Errorf("payload = %q, want hello (base64-decoded)", j.Payload)
	}
	if j.Status != StatusPending || j.Priority != PriorityHigh {
		t.Errorf("status/priority = %s/%s, want pending/high", j.Status, j.Priority)
	}
	// The wire format carries seconds; the exported type carries a Duration.
	if j.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", j.Timeout)
	}
	if j.IdempotencyKey != "key-1" {
		t.Errorf("IdempotencyKey = %q, want key-1", j.IdempotencyKey)
	}
	if got := j.Attempt(); got != 2 {
		t.Errorf("Attempt() = %d, want 2 (one retry made)", got)
	}
	if got := j.AttemptsLeft(); got != 2 {
		t.Errorf("AttemptsLeft() = %d, want 2", got)
	}
}

func TestEnqueueResultReportsDeduplication(t *testing.T) {
	s := newStub(t)
	c := s.client(t)

	s.respond(http.StatusCreated, jobJSON)
	if _, created, err := c.EnqueueResult(context.Background(), "emails", nil); err != nil || !created {
		t.Errorf("EnqueueResult on 201 = created %v, err %v; want created", created, err)
	}

	// 200 means an idempotency key matched an existing job, so nothing was
	// created and the caller is looking at the original.
	s.respond(http.StatusOK, jobJSON)
	if _, created, err := c.EnqueueResult(context.Background(), "emails", nil); err != nil || created {
		t.Errorf("EnqueueResult on 200 = created %v, err %v; want deduplicated", created, err)
	}
}

func TestEnqueueJSONRoundTrip(t *testing.T) {
	type message struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}

	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusCreated, jobJSON)

	want := message{To: "a@example.com", Subject: "hi"}
	if _, err := c.EnqueueJSON(context.Background(), "emails", want); err != nil {
		t.Fatalf("EnqueueJSON = %v", err)
	}

	_, _, body, _ := s.lastRequest()
	var sent enqueueRequest
	if err := json.Unmarshal([]byte(body), &sent); err != nil {
		t.Fatalf("request body is not JSON: %v", err)
	}

	// Job.Decode is the worker-side mirror of EnqueueJSON.
	var got message
	if err := (&Job{ID: "job-1", Payload: sent.Payload}).Decode(&got); err != nil {
		t.Fatalf("Decode = %v", err)
	}
	if got != want {
		t.Errorf("round-tripped %+v, want %+v", got, want)
	}
}

func TestEnqueueJSONRejectsUnmarshalablePayload(t *testing.T) {
	s := newStub(t)
	c := s.client(t)

	if _, err := c.EnqueueJSON(context.Background(), "emails", make(chan int)); err == nil {
		t.Error("EnqueueJSON with an unencodable value = nil error, want one")
	}
}

func TestDecodeRejectsEmptyPayload(t *testing.T) {
	var v map[string]any
	if err := (&Job{ID: "job-1"}).Decode(&v); err == nil {
		t.Error("Decode on an empty payload = nil error, want one")
	}
}

func TestCeilSeconds(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want int
	}{
		{0, 0},
		{-time.Second, 0},
		// The API's unit is whole seconds: rounding down would turn a
		// caller's short delay into "run now" and a short timeout into
		// "no timeout".
		{time.Millisecond, 1},
		{500 * time.Millisecond, 1},
		{time.Second, 1},
		{1500 * time.Millisecond, 2},
		{90 * time.Second, 90},
	}

	for _, tt := range tests {
		if got := ceilSeconds(tt.in); got != tt.want {
			t.Errorf("ceilSeconds(%v) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestListBuildsQuery(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"jobs":[`+jobJSON+`,`+jobJSON+`]}`)

	jobs, err := c.List(context.Background(), StatusDead, 10)
	if err != nil {
		t.Fatalf("List = %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("List returned %d jobs, want 2", len(jobs))
	}
	if jobs[0].ID != "job-1" {
		t.Errorf("first job ID = %q, want job-1", jobs[0].ID)
	}

	_, rawURL, _, _ := s.lastRequest()
	if !strings.Contains(rawURL, "status=dead") || !strings.Contains(rawURL, "limit=10") {
		t.Errorf("requested %q, want status=dead and limit=10", rawURL)
	}
}

func TestListOmitsZeroLimit(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"jobs":[]}`)

	// A zero limit means "server's default", which the API expresses by
	// the parameter being absent, not by limit=0 (which it rejects).
	if _, err := c.List(context.Background(), StatusPending, 0); err != nil {
		t.Fatalf("List = %v", err)
	}
	if _, rawURL, _, _ := s.lastRequest(); strings.Contains(rawURL, "limit") {
		t.Errorf("requested %q, want no limit parameter", rawURL)
	}
}

func TestListEmptyResultIsNotNil(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"jobs":[]}`)

	jobs, err := c.List(context.Background(), StatusPending, 0)
	if err != nil {
		t.Fatalf("List = %v", err)
	}
	if jobs == nil {
		t.Error("List returned a nil slice for an empty result, want an empty one")
	}
}

func TestCancelAndRetryTargetTheRightRoutes(t *testing.T) {
	s := newStub(t)
	c := s.client(t)

	tests := []struct {
		name string
		call func(ctx context.Context, id string) error
		want string
	}{
		{"cancel", c.Cancel, "/v1/jobs/job-1/cancel"},
		{"retry", c.Retry, "/v1/jobs/job-1/retry"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s.respond(http.StatusNoContent, "")
			if err := tt.call(context.Background(), "job-1"); err != nil {
				t.Fatalf("%s = %v", tt.name, err)
			}
			method, rawURL, _, _ := s.lastRequest()
			if method != http.MethodPost || rawURL != tt.want {
				t.Errorf("sent %s %s, want POST %s", method, rawURL, tt.want)
			}

			// A job in the wrong state is a 409, and losing that race is
			// an answer rather than a fault.
			s.respond(http.StatusConflict, `{"error":"wrong state"}`)
			if err := tt.call(context.Background(), "job-1"); !errors.Is(err, ErrConflict) {
				t.Errorf("%s on 409 = %v, want ErrConflict", tt.name, err)
			}
		})
	}
}

func TestJobIDsArePathEscaped(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, jobJSON)

	if _, err := c.Get(context.Background(), "weird id/../etc"); err != nil {
		t.Fatalf("Get = %v", err)
	}
	// Escaped, the segment cannot climb out of /v1/jobs/ and hit another
	// route.
	if _, rawURL, _, _ := s.lastRequest(); strings.Contains(rawURL, "..") && !strings.Contains(rawURL, "%2F") {
		t.Errorf("requested %q, want the ID escaped into one path segment", rawURL)
	}
}

func TestArgumentValidation(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	ctx := context.Background()

	if _, err := c.Enqueue(ctx, "", nil); err == nil {
		t.Error("Enqueue with an empty queue = nil error, want one")
	}
	if _, err := c.Get(ctx, ""); err == nil {
		t.Error("Get with an empty ID = nil error, want one")
	}
	if _, err := c.List(ctx, "", 0); err == nil {
		t.Error("List with an empty status = nil error, want one")
	}
	if _, err := c.List(ctx, StatusPending, -1); err == nil {
		t.Error("List with a negative limit = nil error, want one")
	}
	if err := c.Cancel(ctx, ""); err == nil {
		t.Error("Cancel with an empty ID = nil error, want one")
	}
	if err := c.Retry(ctx, ""); err == nil {
		t.Error("Retry with an empty ID = nil error, want one")
	}

	// None of those should have reached the server.
	if method, _, _, _ := s.lastRequest(); method != "" {
		t.Errorf("a rejected call still sent %s to the server", method)
	}
}
