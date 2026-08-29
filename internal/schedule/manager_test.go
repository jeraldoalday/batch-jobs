package schedule

import (
	"errors"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// memStore is an in-memory Store, enough for exercising the Manager's
// rules without a database. It keeps insertion order so List is stable.
type memStore struct {
	byID  map[string]*Schedule
	order []string
	fail  error // returned by every mutating call when set
}

func newMemStore() *memStore {
	return &memStore{byID: make(map[string]*Schedule)}
}

func (m *memStore) Create(s *Schedule) error {
	if m.fail != nil {
		return m.fail
	}
	copied := *s
	m.byID[s.ID] = &copied
	m.order = append(m.order, s.ID)
	return nil
}

func (m *memStore) Update(s *Schedule) error {
	if m.fail != nil {
		return m.fail
	}
	stored, ok := m.byID[s.ID]
	if !ok {
		return ErrNotFound
	}
	// Mirror the SQLite store: an update does not touch enabled or the
	// firing history.
	enabled, lastRun := stored.Enabled, stored.LastRunAt
	copied := *s
	copied.Enabled, copied.LastRunAt = enabled, lastRun
	m.byID[s.ID] = &copied
	return nil
}

func (m *memStore) Get(id string) (*Schedule, error) {
	s, ok := m.byID[id]
	if !ok {
		return nil, ErrNotFound
	}
	copied := *s
	return &copied, nil
}

func (m *memStore) List() ([]*Schedule, error) {
	out := make([]*Schedule, 0, len(m.order))
	for _, id := range m.order {
		if s, ok := m.byID[id]; ok {
			copied := *s
			out = append(out, &copied)
		}
	}
	return out, nil
}

func (m *memStore) Due(now time.Time) ([]*Schedule, error) {
	var due []*Schedule
	for _, id := range m.order {
		s, ok := m.byID[id]
		if ok && s.Enabled && !s.NextRunAt.After(now) {
			copied := *s
			due = append(due, &copied)
		}
	}
	return due, nil
}

func (m *memStore) MarkFired(id string, firedAt, next time.Time) error {
	s, ok := m.byID[id]
	if !ok {
		return ErrNotFound
	}
	s.LastRunAt, s.NextRunAt = firedAt, next
	return nil
}

func (m *memStore) SetEnabled(id string, enabled bool) error {
	if m.fail != nil {
		return m.fail
	}
	s, ok := m.byID[id]
	if !ok {
		return ErrNotFound
	}
	s.Enabled = enabled
	return nil
}

func (m *memStore) Delete(id string) error {
	delete(m.byID, id)
	return nil
}

func (m *memStore) Close() error { return nil }

// recordingQueue captures what a trigger enqueues.
type recordingQueue struct {
	jobs []*job.Job
	err  error
}

func (q *recordingQueue) Enqueue(j *job.Job) error {
	if q.err != nil {
		return q.err
	}
	q.jobs = append(q.jobs, j)
	return nil
}

func newManager(t *testing.T) (*Manager, *memStore, *recordingQueue) {
	t.Helper()
	store, queue := newMemStore(), &recordingQueue{}
	return &Manager{Store: store, Queue: queue}, store, queue
}

func TestManagerAddPersistsAnEnabledSchedule(t *testing.T) {
	m, store, _ := newManager(t)

	s, err := m.Add("reports", "0 3 * * *", []byte("nightly"), WithMaxRetries(3))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !s.Enabled {
		t.Error("Enabled = false, want a new schedule enabled")
	}
	if _, ok := store.byID[s.ID]; !ok {
		t.Error("schedule was not persisted")
	}
}

func TestManagerAddRejectsBadInput(t *testing.T) {
	m, _, _ := newManager(t)

	if _, err := m.Add("", "0 3 * * *", nil); err == nil {
		t.Error("Add with an empty queue returned nil error")
	}
	if _, err := m.Add("reports", "nonsense", nil); err == nil {
		t.Error("Add with an unparsable expression returned nil error")
	}
}

func TestManagerUpdateMovesNextRunOnlyForANewExpression(t *testing.T) {
	m, _, _ := newManager(t)

	s, err := m.Add("reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	original := s.NextRunAt

	same, err := m.Update(s.ID, func(s *Schedule) error {
		s.MaxRetries = 5
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !same.NextRunAt.Equal(original) {
		t.Errorf("NextRunAt = %v after an unrelated edit, want it left at %v", same.NextRunAt, original)
	}

	changed, err := m.Update(s.ID, func(s *Schedule) error {
		s.CronExpr = "*/5 * * * *"
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !changed.NextRunAt.Before(original) {
		t.Errorf("NextRunAt = %v after a new expression, want it recomputed to before %v", changed.NextRunAt, original)
	}
}

func TestManagerUpdateDiscardsARejectedEdit(t *testing.T) {
	m, store, _ := newManager(t)

	s, err := m.Add("reports", "0 3 * * *", []byte("original"))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	if _, err := m.Update(s.ID, func(s *Schedule) error {
		s.Payload = []byte("edited")
		s.CronExpr = "nonsense"
		return nil
	}); err == nil {
		t.Fatal("Update with an unparsable expression returned nil error")
	}
	if got := string(store.byID[s.ID].Payload); got != "original" {
		t.Errorf("Payload = %q, want the whole rejected edit discarded", got)
	}

	// An edit function that reports its own failure is equally binding.
	sentinel := errors.New("caller said no")
	if _, err := m.Update(s.ID, func(s *Schedule) error {
		s.Payload = []byte("edited")
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("Update = %v, want the edit function's own error", err)
	}
	if got := string(store.byID[s.ID].Payload); got != "original" {
		t.Errorf("Payload = %q, want the abandoned edit discarded", got)
	}
}

func TestManagerUpdateRejectsAnEmptyQueue(t *testing.T) {
	m, _, _ := newManager(t)

	s, err := m.Add("reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := m.Update(s.ID, func(s *Schedule) error {
		s.Queue = ""
		return nil
	}); err == nil {
		t.Fatal("Update clearing the queue returned nil error")
	}
}

func TestManagerResumeClearsAnOverdueFireTime(t *testing.T) {
	m, store, _ := newManager(t)

	s, err := m.Add("reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := m.Pause(s.ID); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	// A week's pause, so the stored fire time is long past.
	store.byID[s.ID].NextRunAt = time.Now().UTC().Add(-7 * 24 * time.Hour)

	now := time.Now().UTC()
	if err := m.Resume(s.ID); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	got := store.byID[s.ID]
	if !got.Enabled {
		t.Error("Enabled = false after Resume, want true")
	}
	if !got.NextRunAt.After(now) {
		t.Errorf("NextRunAt = %v, want it recomputed to after %v", got.NextRunAt, now)
	}
}

func TestManagerResumeOfAnEnabledScheduleChangesNothing(t *testing.T) {
	m, store, _ := newManager(t)

	s, err := m.Add("reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	before := store.byID[s.ID].NextRunAt

	if err := m.Resume(s.ID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !store.byID[s.ID].NextRunAt.Equal(before) {
		t.Errorf("NextRunAt = %v, want it left at %v", store.byID[s.ID].NextRunAt, before)
	}
}

func TestManagerTriggerLeavesTheTimetableAlone(t *testing.T) {
	m, store, queue := newManager(t)

	s, err := m.Add("reports", "0 3 * * *", []byte("nightly"), WithPriority(job.PriorityHigh))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	before := *store.byID[s.ID]

	j, err := m.Trigger(s.ID)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if len(queue.jobs) != 1 {
		t.Fatalf("queue holds %d jobs, want 1", len(queue.jobs))
	}
	if j.Queue != "reports" || string(j.Payload) != "nightly" {
		t.Errorf("job = queue %q payload %q, want reports/nightly", j.Queue, j.Payload)
	}
	if j.Priority != job.PriorityHigh {
		t.Errorf("Priority = %v, want the schedule's %v", j.Priority, job.PriorityHigh)
	}

	after := store.byID[s.ID]
	if !after.NextRunAt.Equal(before.NextRunAt) {
		t.Errorf("NextRunAt = %v, want it left at %v", after.NextRunAt, before.NextRunAt)
	}
	if !after.LastRunAt.IsZero() {
		t.Errorf("LastRunAt = %v, want a manual trigger not recorded as a firing", after.LastRunAt)
	}
}

func TestManagerTriggerWithoutAQueue(t *testing.T) {
	store := newMemStore()
	m := &Manager{Store: store}

	s, err := m.Add("reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := m.Trigger(s.ID); !errors.Is(err, ErrNoQueue) {
		t.Fatalf("Trigger = %v, want ErrNoQueue", err)
	}
}

func TestManagerMethodsRequireAnID(t *testing.T) {
	m, _, _ := newManager(t)

	if _, err := m.Get(""); err == nil {
		t.Error("Get with an empty ID returned nil error")
	}
	if _, err := m.Update("", func(*Schedule) error { return nil }); err == nil {
		t.Error("Update with an empty ID returned nil error")
	}
	if _, err := m.Update("id", nil); err == nil {
		t.Error("Update with a nil edit function returned nil error")
	}
	if err := m.Pause(""); err == nil {
		t.Error("Pause with an empty ID returned nil error")
	}
	if err := m.Resume(""); err == nil {
		t.Error("Resume with an empty ID returned nil error")
	}
	if err := m.Delete(""); err == nil {
		t.Error("Delete with an empty ID returned nil error")
	}
	if _, err := m.Trigger(""); err == nil {
		t.Error("Trigger with an empty ID returned nil error")
	}
}

func TestManagerUnknownIDIsNotFound(t *testing.T) {
	m, _, _ := newManager(t)

	if _, err := m.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
	if _, err := m.Update("nope", func(*Schedule) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Errorf("Update = %v, want ErrNotFound", err)
	}
	if err := m.Resume("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Resume = %v, want ErrNotFound", err)
	}
	if _, err := m.Trigger("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Trigger = %v, want ErrNotFound", err)
	}
	// Deleting what is not there is deliberately fine.
	if err := m.Delete("nope"); err != nil {
		t.Errorf("Delete of an unknown schedule = %v, want nil", err)
	}
}

func TestManagerUsesTheInjectedClock(t *testing.T) {
	store := newMemStore()
	pinned := time.Date(2026, time.March, 14, 12, 0, 0, 0, time.UTC)
	m := &Manager{Store: store, Now: func() time.Time { return pinned }}

	s, err := m.Add("reports", "0 3 * * *", nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	updated, err := m.Update(s.ID, func(s *Schedule) error {
		s.CronExpr = "0 * * * *"
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	want := time.Date(2026, time.March, 14, 13, 0, 0, 0, time.UTC)
	if !updated.NextRunAt.Equal(want) {
		t.Errorf("NextRunAt = %v, want %v computed from the pinned clock", updated.NextRunAt, want)
	}
}
