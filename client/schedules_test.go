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

// lastBody is the stub's recorded request body, named for readability in
// the assertions below.
func lastBody(s *stub) string {
	_, _, body, _ := s.lastRequest()
	return body
}

func TestAddScheduleSendsTheExpectedRequest(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusCreated, `{"id":"sched-1","queue":"reports","cron":"0 3 * * *","enabled":true}`)

	got, err := c.AddSchedule(context.Background(), "reports", "0 3 * * *", []byte("nightly"),
		WithSchedulePriority(PriorityHigh),
		WithScheduleMaxRetries(3),
		WithScheduleTimeout(90*time.Second),
	)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	if got.ID != "sched-1" {
		t.Errorf("ID = %q, want %q", got.ID, "sched-1")
	}

	if method, path, _, _ := s.lastRequest(); method != http.MethodPost || path != "/v1/schedules" {
		t.Errorf("request = %s %s, want POST /v1/schedules", method, path)
	}

	var sent createScheduleRequest
	if err := json.Unmarshal([]byte(lastBody(s)), &sent); err != nil {
		t.Fatalf("unmarshal request body: %v", err)
	}
	if sent.Queue != "reports" || sent.Cron != "0 3 * * *" {
		t.Errorf("body queue/cron = %q/%q, want reports/0 3 * * *", sent.Queue, sent.Cron)
	}
	if sent.Priority == nil || *sent.Priority != int(PriorityHigh) {
		t.Errorf("body priority = %v, want %d", sent.Priority, PriorityHigh)
	}
	if sent.MaxRetries != 3 {
		t.Errorf("body max_retries = %d, want 3", sent.MaxRetries)
	}
	if sent.TimeoutSeconds != 90 {
		t.Errorf("body timeout_seconds = %d, want 90", sent.TimeoutSeconds)
	}
}

func TestAddScheduleTimeoutRoundsUp(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusCreated, `{"id":"sched-1"}`)

	// A sub-second timeout must not cross the wire as zero, which the
	// server reads as "no timeout at all".
	if _, err := c.AddSchedule(context.Background(), "q", "* * * * *", nil, WithScheduleTimeout(1500*time.Millisecond)); err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}

	var sent createScheduleRequest
	if err := json.Unmarshal([]byte(lastBody(s)), &sent); err != nil {
		t.Fatalf("unmarshal request body: %v", err)
	}
	if sent.TimeoutSeconds != 2 {
		t.Errorf("body timeout_seconds = %d, want 2", sent.TimeoutSeconds)
	}
}

func TestAddScheduleValidatesLocally(t *testing.T) {
	c, err := New("http://example.invalid")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if _, err := c.AddSchedule(ctx, "", "* * * * *", nil); err == nil {
		t.Error("AddSchedule with no queue returned nil error")
	}
	if _, err := c.AddSchedule(ctx, "q", "", nil); err == nil {
		t.Error("AddSchedule with no cron expression returned nil error")
	}
}

func TestScheduleMethodsRequireAnID(t *testing.T) {
	c, err := New("http://example.invalid")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if _, err := c.GetSchedule(ctx, ""); err == nil {
		t.Error("GetSchedule with an empty ID returned nil error")
	}
	if _, err := c.UpdateSchedule(ctx, "", (ScheduleUpdate{}).SetMaxRetries(1)); err == nil {
		t.Error("UpdateSchedule with an empty ID returned nil error")
	}
	if err := c.PauseSchedule(ctx, ""); err == nil {
		t.Error("PauseSchedule with an empty ID returned nil error")
	}
	if err := c.ResumeSchedule(ctx, ""); err == nil {
		t.Error("ResumeSchedule with an empty ID returned nil error")
	}
	if err := c.DeleteSchedule(ctx, ""); err == nil {
		t.Error("DeleteSchedule with an empty ID returned nil error")
	}
	if _, err := c.TriggerSchedule(ctx, ""); err == nil {
		t.Error("TriggerSchedule with an empty ID returned nil error")
	}
}

func TestUpdateScheduleSendsOnlyTheNamedFields(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"id":"sched-1","cron":"*/5 * * * *"}`)

	if _, err := c.UpdateSchedule(context.Background(), "sched-1", (ScheduleUpdate{}).SetCron("*/5 * * * *")); err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}

	if method, path, _, _ := s.lastRequest(); method != http.MethodPatch || path != "/v1/schedules/sched-1" {
		t.Errorf("request = %s %s, want PATCH /v1/schedules/sched-1", method, path)
	}

	// Only the field the caller named may appear: an omitted one the
	// server would read as "set this to its zero value" is exactly the
	// accident the pointer fields exist to prevent.
	_, _, body, _ := s.lastRequest()
	if !strings.Contains(body, `"cron":"*/5 * * * *"`) {
		t.Errorf("body = %s, want it to carry the new cron expression", body)
	}
	for _, field := range []string{"queue", "payload", "priority", "max_retries", "timeout_seconds"} {
		if strings.Contains(body, `"`+field+`"`) {
			t.Errorf("body = %s, want %q omitted", body, field)
		}
	}
}

func TestUpdateScheduleRefusesAnEmptyUpdate(t *testing.T) {
	c, err := New("http://example.invalid")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := c.UpdateSchedule(context.Background(), "sched-1", (ScheduleUpdate{})); err == nil {
		t.Fatal("UpdateSchedule with an empty update returned nil error")
	}
}

func TestScheduleUpdateSetters(t *testing.T) {
	u := ScheduleUpdate{}
	if !u.IsZero() {
		t.Error("a fresh ScheduleUpdate is not zero")
	}

	u = u.SetQueue("digests").
		SetCron("0 * * * *").
		SetPayload([]byte("body")).
		SetPriority(PriorityLow).
		SetMaxRetries(4).
		SetTimeout(1500 * time.Millisecond)

	if u.IsZero() {
		t.Fatal("a populated ScheduleUpdate reports itself zero")
	}
	if *u.Queue != "digests" {
		t.Errorf("Queue = %q, want %q", *u.Queue, "digests")
	}
	if *u.Cron != "0 * * * *" {
		t.Errorf("Cron = %q, want %q", *u.Cron, "0 * * * *")
	}
	if string(*u.Payload) != "body" {
		t.Errorf("Payload = %q, want %q", *u.Payload, "body")
	}
	if *u.Priority != int(PriorityLow) {
		t.Errorf("Priority = %d, want %d", *u.Priority, PriorityLow)
	}
	if *u.MaxRetries != 4 {
		t.Errorf("MaxRetries = %d, want 4", *u.MaxRetries)
	}
	if *u.TimeoutSeconds != 2 {
		t.Errorf("TimeoutSeconds = %d, want 2 (rounded up)", *u.TimeoutSeconds)
	}
}

func TestScheduleUpdateSetJSONPayload(t *testing.T) {
	u, err := ScheduleUpdate{}.SetJSONPayload(map[string]string{"window": "daily"})
	if err != nil {
		t.Fatalf("SetJSONPayload: %v", err)
	}
	if u.Payload == nil {
		t.Fatal("Payload is nil after SetJSONPayload")
	}
	if got := string(*u.Payload); got != `{"window":"daily"}` {
		t.Errorf("Payload = %q, want %q", got, `{"window":"daily"}`)
	}

	// A value that cannot be marshalled is reported rather than
	// silently producing an empty payload.
	if _, err := (ScheduleUpdate{}).SetJSONPayload(make(chan int)); err == nil {
		t.Error("SetJSONPayload of an unmarshalable value returned nil error")
	}
}

func TestPauseResumeDeleteAndTriggerPaths(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	ctx := context.Background()

	s.respond(http.StatusNoContent, "")
	if err := c.PauseSchedule(ctx, "sched-1"); err != nil {
		t.Fatalf("PauseSchedule: %v", err)
	}
	if method, path, _, _ := s.lastRequest(); method != http.MethodPost || path != "/v1/schedules/sched-1/pause" {
		t.Errorf("request = %s %s, want POST /v1/schedules/sched-1/pause", method, path)
	}

	s.respond(http.StatusNoContent, "")
	if err := c.ResumeSchedule(ctx, "sched-1"); err != nil {
		t.Fatalf("ResumeSchedule: %v", err)
	}
	if method, path, _, _ := s.lastRequest(); method != http.MethodPost || path != "/v1/schedules/sched-1/resume" {
		t.Errorf("request = %s %s, want POST /v1/schedules/sched-1/resume", method, path)
	}

	s.respond(http.StatusNoContent, "")
	if err := c.DeleteSchedule(ctx, "sched-1"); err != nil {
		t.Fatalf("DeleteSchedule: %v", err)
	}
	if method, path, _, _ := s.lastRequest(); method != http.MethodDelete || path != "/v1/schedules/sched-1" {
		t.Errorf("request = %s %s, want DELETE /v1/schedules/sched-1", method, path)
	}

	s.respond(http.StatusCreated, `{"id":"job-1","queue":"reports"}`)
	j, err := c.TriggerSchedule(ctx, "sched-1")
	if err != nil {
		t.Fatalf("TriggerSchedule: %v", err)
	}
	if j.ID != "job-1" {
		t.Errorf("job ID = %q, want %q", j.ID, "job-1")
	}
	if method, path, _, _ := s.lastRequest(); method != http.MethodPost || path != "/v1/schedules/sched-1/trigger" {
		t.Errorf("request = %s %s, want POST /v1/schedules/sched-1/trigger", method, path)
	}
}

func TestScheduleIDsAreEscapedIntoThePath(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"id":"a/b"}`)

	if _, err := c.GetSchedule(context.Background(), "a/b"); err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if _, path, _, _ := s.lastRequest(); path == "/v1/schedules/a/b" {
		t.Errorf("path = %q, want the slash escaped rather than routed as a path separator", path)
	}
}

func TestScheduleDecodingOfWireFields(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{
		"id":"sched-1",
		"queue":"reports",
		"cron":"0 3 * * *",
		"priority":1,
		"max_retries":3,
		"timeout_seconds":90,
		"enabled":true,
		"next_run_at":"2026-03-15T03:00:00Z",
		"last_run_at":"2026-03-14T03:00:00Z",
		"upcoming":["2026-03-15T03:00:00Z","2026-03-16T03:00:00Z"]
	}`)

	got, err := c.GetSchedule(context.Background(), "sched-1")
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got.Priority != PriorityHigh {
		t.Errorf("Priority = %v, want %v", got.Priority, PriorityHigh)
	}
	if got.Timeout != 90*time.Second {
		t.Errorf("Timeout = %v, want %v -- seconds on the wire become a Duration here", got.Timeout, 90*time.Second)
	}
	if !got.Fired() {
		t.Error("Fired() = false, want true for a schedule with a last run")
	}
	if len(got.Upcoming) != 2 {
		t.Errorf("Upcoming holds %d times, want 2", len(got.Upcoming))
	}
	if !got.Overdue(time.Date(2026, time.March, 16, 0, 0, 0, 0, time.UTC)) {
		t.Error("Overdue() = false for an enabled schedule whose next run has passed")
	}
}

func TestNeverFiredScheduleReportsNoLastRun(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"id":"sched-1","enabled":true,"next_run_at":"2026-03-15T03:00:00Z"}`)

	got, err := c.GetSchedule(context.Background(), "sched-1")
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got.Fired() {
		t.Error("Fired() = true for a schedule the server reported no last run for")
	}
	if !got.LastRunAt.IsZero() {
		t.Errorf("LastRunAt = %v, want the zero time", got.LastRunAt)
	}
}

func TestScheduleDecodePayload(t *testing.T) {
	empty := &Schedule{ID: "sched-1"}
	if err := empty.Decode(&struct{}{}); err == nil {
		t.Error("Decode of an empty payload returned nil error")
	}

	s := &Schedule{ID: "sched-1", Payload: []byte(`{"window":"daily"}`)}
	var got struct {
		Window string `json:"window"`
	}
	if err := s.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Window != "daily" {
		t.Errorf("Window = %q, want %q", got.Window, "daily")
	}
}

func TestListSchedulesDecodesTheEnvelope(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"schedules":[{"id":"a"},{"id":"b"}]}`)

	got, err := c.ListSchedules(context.Background())
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("ListSchedules = %+v, want schedules a and b in order", got)
	}
	if _, path, _, _ := s.lastRequest(); path != "/v1/schedules" {
		t.Errorf("path = %q, want /v1/schedules", path)
	}
}

func TestScheduleErrorsMapToSentinels(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	ctx := context.Background()

	s.respond(http.StatusNotImplemented, `{"error":"scheduling is not enabled on this server"}`)
	if _, err := c.ListSchedules(ctx); !errors.Is(err, ErrUnsupported) {
		t.Errorf("ListSchedules against a server without scheduling = %v, want ErrUnsupported", err)
	}

	s.respond(http.StatusNotFound, `{"error":"schedule not found"}`)
	if _, err := c.GetSchedule(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetSchedule of an unknown schedule = %v, want ErrNotFound", err)
	}

	s.respond(http.StatusBadRequest, `{"error":"schedule: cron expression \"nonsense\" must have 5 fields"}`)
	if _, err := c.AddSchedule(ctx, "q", "nonsense", nil); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("AddSchedule with a bad expression = %v, want ErrInvalidRequest", err)
	}
}
