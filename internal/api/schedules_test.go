package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

var _ ScheduleStore = (*fakeScheduleStore)(nil)

// fakeScheduleStore is a ScheduleStore backed by a real schedule.Manager
// over an in-memory map, so the handlers are tested against the actual
// editing rules rather than a second, more forgiving implementation of
// them. Only the failure injection is faked.
type fakeScheduleStore struct {
	*schedule.Manager
	store *fakeScheduleRows
}

func newFakeScheduleStore() *fakeScheduleStore {
	rows := &fakeScheduleRows{byID: make(map[string]*schedule.Schedule)}
	queue := &fakeScheduleQueue{}
	return &fakeScheduleStore{
		Manager: &schedule.Manager{Store: rows, Queue: queue},
		store:   rows,
	}
}

// add is the test-side shorthand for putting a schedule in place.
func (f *fakeScheduleStore) add(t *testing.T, queueName, cronExpr string, opts ...schedule.Option) *schedule.Schedule {
	t.Helper()
	s, err := f.Add(queueName, cronExpr, []byte("payload"), opts...)
	if err != nil {
		t.Fatalf("seed schedule: %v", err)
	}
	return s
}

// fakeScheduleRows is a minimal schedule.Store: keyed lookup, insertion
// order, and an injectable error for the handlers' 500 paths.
type fakeScheduleRows struct {
	mu    sync.Mutex
	byID  map[string]*schedule.Schedule
	order []string
	err   error
}

func (f *fakeScheduleRows) Create(s *schedule.Schedule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	copied := *s
	f.byID[s.ID] = &copied
	f.order = append(f.order, s.ID)
	return nil
}

func (f *fakeScheduleRows) Update(s *schedule.Schedule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	stored, ok := f.byID[s.ID]
	if !ok {
		return schedule.ErrNotFound
	}
	enabled, lastRun := stored.Enabled, stored.LastRunAt
	copied := *s
	copied.Enabled, copied.LastRunAt = enabled, lastRun
	f.byID[s.ID] = &copied
	return nil
}

func (f *fakeScheduleRows) Get(id string) (*schedule.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	s, ok := f.byID[id]
	if !ok {
		return nil, schedule.ErrNotFound
	}
	copied := *s
	return &copied, nil
}

func (f *fakeScheduleRows) List() ([]*schedule.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := make([]*schedule.Schedule, 0, len(f.order))
	for _, id := range f.order {
		if s, ok := f.byID[id]; ok {
			copied := *s
			out = append(out, &copied)
		}
	}
	return out, nil
}

func (f *fakeScheduleRows) Due(time.Time) ([]*schedule.Schedule, error) { return nil, nil }

func (f *fakeScheduleRows) MarkFired(id string, firedAt, next time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.byID[id]
	if !ok {
		return schedule.ErrNotFound
	}
	s.LastRunAt, s.NextRunAt = firedAt, next
	return nil
}

func (f *fakeScheduleRows) SetEnabled(id string, enabled bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	s, ok := f.byID[id]
	if !ok {
		return schedule.ErrNotFound
	}
	s.Enabled = enabled
	return nil
}

func (f *fakeScheduleRows) Delete(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	delete(f.byID, id)
	return nil
}

func (f *fakeScheduleRows) Close() error { return nil }

// fakeScheduleQueue captures what a trigger enqueues.
type fakeScheduleQueue struct {
	mu   sync.Mutex
	jobs []*job.Job
}

func (q *fakeScheduleQueue) Enqueue(j *job.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, j)
	return nil
}

// scheduleServer wires a handler over a fresh fake schedule store.
func scheduleServer(t *testing.T) (http.Handler, *fakeScheduleStore) {
	t.Helper()
	sched := newFakeScheduleStore()
	h := (&Server{Store: newFakeStore(), ScheduleStore: sched}).Handler()
	return h, sched
}

func TestHandleCreateSchedule(t *testing.T) {
	h, _ := scheduleServer(t)

	priority := int(job.PriorityHigh)
	rec := doJSON(t, h, http.MethodPost, "/v1/schedules", CreateScheduleRequest{
		Queue:          "reports",
		Cron:           "0 3 * * *",
		Payload:        []byte("nightly"),
		Priority:       &priority,
		MaxRetries:     3,
		TimeoutSeconds: 90,
	})

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body)
	}
	var got ScheduleDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID == "" {
		t.Error("id is empty, want a generated one")
	}
	if got.Queue != "reports" || got.Cron != "0 3 * * *" {
		t.Errorf("queue/cron = %q/%q, want reports/0 3 * * *", got.Queue, got.Cron)
	}
	if got.Priority != int(job.PriorityHigh) {
		t.Errorf("priority = %d, want %d", got.Priority, job.PriorityHigh)
	}
	if got.MaxRetries != 3 {
		t.Errorf("max_retries = %d, want 3", got.MaxRetries)
	}
	if got.TimeoutSeconds != 90 {
		t.Errorf("timeout_seconds = %d, want 90", got.TimeoutSeconds)
	}
	if !got.Enabled {
		t.Error("enabled = false, want a new schedule enabled")
	}
	if got.LastRunAt != nil {
		t.Errorf("last_run_at = %v, want it absent for a schedule that never fired", got.LastRunAt)
	}
	if len(got.Upcoming) != ScheduleUpcomingCount {
		t.Errorf("upcoming holds %d times, want %d", len(got.Upcoming), ScheduleUpcomingCount)
	}
}

func TestHandleCreateScheduleRejectsBadRequests(t *testing.T) {
	h, _ := scheduleServer(t)

	badPriority, negative := 7, -1
	tests := []struct {
		name string
		body CreateScheduleRequest
	}{
		{"no queue", CreateScheduleRequest{Cron: "0 3 * * *"}},
		{"no cron", CreateScheduleRequest{Queue: "reports"}},
		{"unparsable cron", CreateScheduleRequest{Queue: "reports", Cron: "nightly please"}},
		{"cron that never fires", CreateScheduleRequest{Queue: "reports", Cron: "0 0 30 2 *"}},
		{"bad priority", CreateScheduleRequest{Queue: "reports", Cron: "0 3 * * *", Priority: &badPriority}},
		{"negative retries", CreateScheduleRequest{Queue: "reports", Cron: "0 3 * * *", MaxRetries: negative}},
		{"negative timeout", CreateScheduleRequest{Queue: "reports", Cron: "0 3 * * *", TimeoutSeconds: negative}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doJSON(t, h, http.MethodPost, "/v1/schedules", tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusBadRequest, rec.Body)
			}
		})
	}
}

func TestHandleGetSchedule(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *")

	rec := doJSON(t, h, http.MethodGet, "/v1/schedules/"+created.ID, nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var got ScheduleDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("id = %q, want %q", got.ID, created.ID)
	}
}

func TestHandleGetScheduleNotFound(t *testing.T) {
	h, _ := scheduleServer(t)

	rec := doJSON(t, h, http.MethodGet, "/v1/schedules/nope", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleListSchedules(t *testing.T) {
	h, sched := scheduleServer(t)
	sched.add(t, "reports", "0 3 * * *")
	sched.add(t, "digests", "0 * * * *")

	rec := doJSON(t, h, http.MethodGet, "/v1/schedules", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var resp ListSchedulesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Schedules) != 2 {
		t.Fatalf("schedules holds %d entries, want 2", len(resp.Schedules))
	}
}

func TestHandleListSchedulesEmpty(t *testing.T) {
	h, _ := scheduleServer(t)

	rec := doJSON(t, h, http.MethodGet, "/v1/schedules", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}

	// An empty listing must be [] rather than null, so a client can
	// iterate the field without checking it first.
	var resp struct {
		Schedules []ScheduleDTO `json:"schedules"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Schedules == nil {
		t.Error("schedules = null, want an empty array")
	}
}

func TestHandleUpdateScheduleLeavesOmittedFieldsAlone(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *", schedule.WithMaxRetries(5))

	retries := 9
	rec := doJSON(t, h, http.MethodPatch, "/v1/schedules/"+created.ID, UpdateScheduleRequest{
		MaxRetries: &retries,
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var got ScheduleDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.MaxRetries != 9 {
		t.Errorf("max_retries = %d, want 9", got.MaxRetries)
	}
	// The fields the patch did not mention keep their values, which is
	// the whole difference between a patch and a replacement.
	if got.Queue != "reports" {
		t.Errorf("queue = %q, want it left at %q", got.Queue, "reports")
	}
	if got.Cron != "0 3 * * *" {
		t.Errorf("cron = %q, want it left at %q", got.Cron, "0 3 * * *")
	}
	if string(got.Payload) != "payload" {
		t.Errorf("payload = %q, want it left at %q", got.Payload, "payload")
	}
	if !got.NextRunAt.Equal(created.NextRunAt) {
		t.Errorf("next_run_at = %v, want it left at %v", got.NextRunAt, created.NextRunAt)
	}
}

func TestHandleUpdateScheduleRecomputesNextRunForANewCron(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *")

	cron := "*/5 * * * *"
	rec := doJSON(t, h, http.MethodPatch, "/v1/schedules/"+created.ID, UpdateScheduleRequest{Cron: &cron})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var got ScheduleDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Cron != cron {
		t.Errorf("cron = %q, want %q", got.Cron, cron)
	}
	if !got.NextRunAt.Before(created.NextRunAt) {
		t.Errorf("next_run_at = %v, want it recomputed to before %v", got.NextRunAt, created.NextRunAt)
	}
}

func TestHandleUpdateScheduleRejectsBadRequests(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *")

	empty, badCron := "", "nonsense"
	badPriority, negative := 7, -1
	tests := []struct {
		name string
		body UpdateScheduleRequest
	}{
		{"empty queue", UpdateScheduleRequest{Queue: &empty}},
		{"unparsable cron", UpdateScheduleRequest{Cron: &badCron}},
		{"bad priority", UpdateScheduleRequest{Priority: &badPriority}},
		{"negative retries", UpdateScheduleRequest{MaxRetries: &negative}},
		{"negative timeout", UpdateScheduleRequest{TimeoutSeconds: &negative}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doJSON(t, h, http.MethodPatch, "/v1/schedules/"+created.ID, tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusBadRequest, rec.Body)
			}
		})
	}
}

func TestHandleUpdateScheduleNotFound(t *testing.T) {
	h, _ := scheduleServer(t)

	retries := 2
	rec := doJSON(t, h, http.MethodPatch, "/v1/schedules/nope", UpdateScheduleRequest{MaxRetries: &retries})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNotFound, rec.Body)
	}
}

func TestHandlePauseAndResumeSchedule(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *")

	rec := doJSON(t, h, http.MethodPost, "/v1/schedules/"+created.ID+"/pause", nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("pause status = %d, want %d; body = %s", rec.Code, http.StatusNoContent, rec.Body)
	}
	if got, _ := sched.Get(created.ID); got.Enabled {
		t.Error("schedule still enabled after a pause")
	}

	rec = doJSON(t, h, http.MethodPost, "/v1/schedules/"+created.ID+"/resume", nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("resume status = %d, want %d; body = %s", rec.Code, http.StatusNoContent, rec.Body)
	}
	if got, _ := sched.Get(created.ID); !got.Enabled {
		t.Error("schedule still paused after a resume")
	}
}

func TestHandlePauseAndResumeNotFound(t *testing.T) {
	h, _ := scheduleServer(t)

	for _, action := range []string{"pause", "resume"} {
		rec := doJSON(t, h, http.MethodPost, "/v1/schedules/nope/"+action, nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want %d", action, rec.Code, http.StatusNotFound)
		}
	}
}

func TestHandleDeleteScheduleIsIdempotent(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *")

	for i := range 2 {
		rec := doJSON(t, h, http.MethodDelete, "/v1/schedules/"+created.ID, nil)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("delete %d: status = %d, want %d; body = %s", i+1, rec.Code, http.StatusNoContent, rec.Body)
		}
	}
	if _, err := sched.Get(created.ID); !errors.Is(err, schedule.ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
}

func TestHandleTriggerScheduleReturnsTheJob(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *", schedule.WithMaxRetries(4))

	rec := doJSON(t, h, http.MethodPost, "/v1/schedules/"+created.ID+"/trigger", nil)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body)
	}
	var got JobDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Queue != "reports" {
		t.Errorf("queue = %q, want %q", got.Queue, "reports")
	}
	if got.MaxRetries != 4 {
		t.Errorf("max_retries = %d, want the schedule's 4", got.MaxRetries)
	}

	// The timetable is untouched by a manual run.
	after, err := sched.Get(created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !after.NextRunAt.Equal(created.NextRunAt) {
		t.Errorf("next_run_at = %v, want it left at %v", after.NextRunAt, created.NextRunAt)
	}
	if !after.LastRunAt.IsZero() {
		t.Errorf("last_run_at = %v, want a manual trigger not recorded as a firing", after.LastRunAt)
	}
}

func TestHandleTriggerScheduleNotFound(t *testing.T) {
	h, _ := scheduleServer(t)

	rec := doJSON(t, h, http.MethodPost, "/v1/schedules/nope/trigger", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestScheduleRoutesReportNotImplementedWithoutAStore(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	requests := []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/v1/schedules", CreateScheduleRequest{Queue: "q", Cron: "* * * * *"}},
		{http.MethodGet, "/v1/schedules", nil},
		{http.MethodGet, "/v1/schedules/id", nil},
		{http.MethodPatch, "/v1/schedules/id", UpdateScheduleRequest{}},
		{http.MethodDelete, "/v1/schedules/id", nil},
		{http.MethodPost, "/v1/schedules/id/pause", nil},
		{http.MethodPost, "/v1/schedules/id/resume", nil},
		{http.MethodPost, "/v1/schedules/id/trigger", nil},
	}
	for _, req := range requests {
		rec := doJSON(t, h, req.method, req.path, req.body)
		if rec.Code != http.StatusNotImplemented {
			t.Errorf("%s %s: status = %d, want %d", req.method, req.path, rec.Code, http.StatusNotImplemented)
		}
	}
}

func TestScheduleStoreFailuresAreServerErrors(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *")
	sched.store.err = errBoom

	requests := []struct {
		method, path string
	}{
		{http.MethodGet, "/v1/schedules"},
		{http.MethodGet, "/v1/schedules/" + created.ID},
		{http.MethodDelete, "/v1/schedules/" + created.ID},
		{http.MethodPost, "/v1/schedules/" + created.ID + "/pause"},
		{http.MethodPost, "/v1/schedules/" + created.ID + "/trigger"},
	}
	for _, req := range requests {
		rec := doJSON(t, h, req.method, req.path, nil)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s %s: status = %d, want %d", req.method, req.path, rec.Code, http.StatusInternalServerError)
		}
	}
}

func TestScheduleDTOReportsLastRunOnceFired(t *testing.T) {
	h, sched := scheduleServer(t)
	created := sched.add(t, "reports", "0 3 * * *")

	fired := time.Now().UTC().Truncate(time.Second)
	if err := sched.store.MarkFired(created.ID, fired, fired.Add(time.Hour)); err != nil {
		t.Fatalf("MarkFired: %v", err)
	}

	rec := doJSON(t, h, http.MethodGet, "/v1/schedules/"+created.ID, nil)
	var got ScheduleDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.LastRunAt == nil {
		t.Fatal("last_run_at is absent, want the recorded firing")
	}
	if !got.LastRunAt.Equal(fired) {
		t.Errorf("last_run_at = %v, want %v", got.LastRunAt, fired)
	}
}
