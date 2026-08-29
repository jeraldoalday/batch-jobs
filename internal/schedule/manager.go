package schedule

import (
	"errors"
	"fmt"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// Enqueuer is the sliver of queue.Queue a Manager needs: somewhere to put
// the job a manual trigger produces. It is named separately rather than
// taken as a queue.Queue so this package does not depend on that one.
type Enqueuer interface {
	Enqueue(j *job.Job) error
}

// ErrNoQueue is returned by Trigger on a Manager with no Enqueuer, which
// is a Manager that can define schedules but not run one on demand.
var ErrNoQueue = errors.New("schedule: manager has no queue to enqueue into")

// Manager holds the rules about editing schedules that are more than a
// store operation: validating a cron expression before writing it,
// deciding when an edit may move the next fire time, clearing an overdue
// one on resume, and producing a job on demand without disturbing the
// timetable.
//
// It exists because those rules have two callers -- the embedded Engine
// and the HTTP API -- and a rule implemented twice is a rule that will
// eventually be implemented twice differently.
//
// A Manager holds no state of its own and is safe for concurrent use if
// its Store is.
type Manager struct {
	// Store is where schedules are persisted. Required.
	Store Store

	// Queue is where Trigger puts the job it produces. A Manager without
	// one supports everything except Trigger.
	Queue Enqueuer

	// Now supplies the current time, defaulting to time.Now().UTC(). It
	// exists so tests can pin the clock the fire-time arithmetic reads.
	Now func() time.Time
}

// now reads the Manager's clock.
func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now().UTC()
}

// Add validates cronExpr, builds the schedule it describes, and persists
// it enabled.
func (m *Manager) Add(queueName, cronExpr string, payload []byte, opts ...Option) (*Schedule, error) {
	if queueName == "" {
		return nil, errors.New("schedule: queue name is required")
	}

	s, err := New(queueName, cronExpr, payload, opts...)
	if err != nil {
		return nil, err
	}
	if err := m.Store.Create(s); err != nil {
		return nil, err
	}
	return s, nil
}

// Get returns the schedule with the given ID, or ErrNotFound.
func (m *Manager) Get(id string) (*Schedule, error) {
	if id == "" {
		return nil, errors.New("schedule: ID is required")
	}
	return m.Store.Get(id)
}

// List returns every schedule, most recently created first.
func (m *Manager) List() ([]*Schedule, error) { return m.Store.List() }

// Update reads the schedule, hands it to edit, and persists the result.
//
// The next fire time moves only when edit actually changed the cron
// expression: rewriting it on every edit would let a caller adjusting a
// retry budget silently postpone tonight's run. A changed expression is
// validated before anything is written, so an edit that will not parse
// leaves the stored schedule exactly as it was -- as does an edit
// function that returns an error.
func (m *Manager) Update(id string, edit func(*Schedule) error) (*Schedule, error) {
	if id == "" {
		return nil, errors.New("schedule: ID is required")
	}
	if edit == nil {
		return nil, errors.New("schedule: update requires an edit function")
	}

	stored, err := m.Store.Get(id)
	if err != nil {
		return nil, err
	}

	before := stored.CronExpr
	if err := edit(stored); err != nil {
		return nil, err
	}
	if stored.Queue == "" {
		return nil, errors.New("schedule: queue name is required")
	}
	if stored.CronExpr != before {
		if err := stored.Reschedule(stored.CronExpr, m.now()); err != nil {
			return nil, err
		}
	}

	if err := m.Store.Update(stored); err != nil {
		return nil, err
	}
	return stored, nil
}

// Pause stops a schedule firing without deleting it. A paused schedule
// accumulates no missed firings: the time it was paused for simply does
// not happen.
func (m *Manager) Pause(id string) error {
	if id == "" {
		return errors.New("schedule: ID is required")
	}
	return m.Store.SetEnabled(id, false)
}

// Resume re-enables a paused schedule, recomputing its next fire time
// from now.
//
// Recomputing is the point: a schedule paused for a week would otherwise
// come back a week overdue and fire the instant it is resumed, which is
// exactly the surprise pausing it was meant to avoid. Resuming an
// already-enabled schedule changes nothing, so it is safe to repeat.
func (m *Manager) Resume(id string) error {
	if id == "" {
		return errors.New("schedule: ID is required")
	}

	stored, err := m.Store.Get(id)
	if err != nil {
		return err
	}
	if stored.Enabled {
		return nil
	}

	now := m.now()
	next, err := stored.Advance(now)
	if err != nil {
		return err
	}
	stored.NextRunAt = next

	// The fire time is written before the schedule is re-enabled, so a
	// scheduler polling in between cannot see an enabled schedule still
	// carrying the stale time it was paused with.
	if err := m.Store.Update(stored); err != nil {
		return err
	}
	return m.Store.SetEnabled(id, true)
}

// Delete removes a schedule permanently. Deleting one that does not exist
// is not an error, so a cleanup path does not have to look first.
func (m *Manager) Delete(id string) error {
	if id == "" {
		return errors.New("schedule: ID is required")
	}
	return m.Store.Delete(id)
}

// Trigger enqueues the job a schedule produces right now and returns it,
// leaving the schedule's timetable untouched: the next fire time does not
// move, and the firing is not recorded as one.
//
// It works on a paused schedule, since pausing stops the timetable rather
// than the operator.
func (m *Manager) Trigger(id string) (*job.Job, error) {
	if id == "" {
		return nil, errors.New("schedule: ID is required")
	}
	if m.Queue == nil {
		return nil, ErrNoQueue
	}

	stored, err := m.Store.Get(id)
	if err != nil {
		return nil, err
	}

	j := stored.NewJob()
	if err := m.Queue.Enqueue(j); err != nil {
		return nil, fmt.Errorf("schedule: trigger %s: %w", id, err)
	}
	return j, nil
}
