package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Schedule is a recurring job as the server reports it: a cron expression
// plus the template every firing is stamped from.
//
// As with Job, the wire format carries the per-attempt timeout as whole
// seconds and this type exposes it as a time.Duration, so callers work in
// Go's own units on both sides of the wire.
type Schedule struct {
	// ID uniquely identifies the schedule, and is what every other call
	// takes.
	ID string

	// Queue is the queue each produced job is enqueued onto.
	Queue string

	// Cron is the 5-field cron expression deciding when the schedule
	// fires.
	Cron string

	// Payload is copied onto every job the schedule produces.
	Payload []byte

	// Priority is applied to every job the schedule produces.
	Priority Priority

	// MaxRetries is applied to every job the schedule produces.
	MaxRetries int

	// Timeout bounds a single attempt of every job the schedule
	// produces. Zero means no per-attempt timeout.
	Timeout time.Duration

	// Enabled reports whether the schedule is eligible to fire.
	Enabled bool

	// NextRunAt is when the schedule is next due. A time in the past
	// means the server's scheduler has not caught up yet.
	NextRunAt time.Time

	// LastRunAt is when the schedule last fired, or the zero time if it
	// never has.
	LastRunAt time.Time

	// CreatedAt is when the schedule was created.
	CreatedAt time.Time

	// UpdatedAt is when the schedule last changed.
	UpdatedAt time.Time

	// Upcoming is the next few times the schedule fires, as the server
	// computed them. It is what makes a cron expression legible without
	// the caller having to parse one.
	Upcoming []time.Time
}

// Fired reports whether the schedule has ever fired.
func (s *Schedule) Fired() bool { return !s.LastRunAt.IsZero() }

// Overdue reports whether the schedule was due before now and has not
// fired yet. A schedule that stays overdue means the server's scheduler is
// not running, or is not keeping up.
func (s *Schedule) Overdue(now time.Time) bool {
	return s.Enabled && s.NextRunAt.Before(now)
}

// Decode unmarshals the schedule's payload as JSON into v, the mirror of
// AddScheduleJSON.
func (s *Schedule) Decode(v any) error {
	if len(s.Payload) == 0 {
		return fmt.Errorf("client: schedule %s has an empty payload", s.ID)
	}
	if err := json.Unmarshal(s.Payload, v); err != nil {
		return fmt.Errorf("client: decode schedule %s payload: %w", s.ID, err)
	}
	return nil
}

// scheduleDTO is the wire representation of a schedule, matching the
// server's JSON exactly.
type scheduleDTO struct {
	ID             string      `json:"id"`
	Queue          string      `json:"queue"`
	Cron           string      `json:"cron"`
	Payload        []byte      `json:"payload,omitempty"`
	Priority       int         `json:"priority"`
	MaxRetries     int         `json:"max_retries"`
	TimeoutSeconds int         `json:"timeout_seconds,omitempty"`
	Enabled        bool        `json:"enabled"`
	NextRunAt      time.Time   `json:"next_run_at"`
	LastRunAt      *time.Time  `json:"last_run_at,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	Upcoming       []time.Time `json:"upcoming,omitempty"`
}

// schedule converts the wire representation into the exported one.
func (d scheduleDTO) schedule() *Schedule {
	s := &Schedule{
		ID:         d.ID,
		Queue:      d.Queue,
		Cron:       d.Cron,
		Payload:    d.Payload,
		Priority:   Priority(d.Priority),
		MaxRetries: d.MaxRetries,
		Timeout:    time.Duration(d.TimeoutSeconds) * time.Second,
		Enabled:    d.Enabled,
		NextRunAt:  d.NextRunAt,
		CreatedAt:  d.CreatedAt,
		UpdatedAt:  d.UpdatedAt,
		Upcoming:   d.Upcoming,
	}
	if d.LastRunAt != nil {
		s.LastRunAt = *d.LastRunAt
	}
	return s
}

// createScheduleRequest is the wire body for POST /v1/schedules.
type createScheduleRequest struct {
	Queue          string `json:"queue"`
	Cron           string `json:"cron"`
	Payload        []byte `json:"payload,omitempty"`
	Priority       *int   `json:"priority,omitempty"`
	MaxRetries     int    `json:"max_retries,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

// ScheduleOption customizes a schedule being created. The options are the
// enqueue options that describe a job template rather than a single
// submission, so code that enqueues and code that schedules read the same.
type ScheduleOption func(*createScheduleRequest)

// WithSchedulePriority sets the dequeue priority of every job the schedule
// produces.
func WithSchedulePriority(p Priority) ScheduleOption {
	return func(r *createScheduleRequest) {
		v := int(p)
		r.Priority = &v
	}
}

// WithScheduleMaxRetries allows n further attempts, after the first
// failure, on every job the schedule produces.
func WithScheduleMaxRetries(n int) ScheduleOption {
	return func(r *createScheduleRequest) { r.MaxRetries = n }
}

// WithScheduleTimeout bounds a single attempt of every job the schedule
// produces. As with WithTimeout, a sub-second d rounds up to one second,
// since the API's unit is whole seconds and a timeout that rounded to zero
// would silently mean "no timeout".
func WithScheduleTimeout(d time.Duration) ScheduleOption {
	return func(r *createScheduleRequest) { r.TimeoutSeconds = ceilSeconds(d) }
}

// AddSchedule creates a recurring job: each time cronExpr comes due, the
// server enqueues a fresh job carrying payload onto the named queue.
//
// The expression is the standard five fields -- minute, hour, day of
// month, month, day of week. A server without scheduling answers with an
// error wrapping ErrUnsupported.
func (c *Client) AddSchedule(ctx context.Context, queue, cronExpr string, payload []byte, opts ...ScheduleOption) (*Schedule, error) {
	if queue == "" {
		return nil, errors.New("client: queue is required")
	}
	if cronExpr == "" {
		return nil, errors.New("client: cron expression is required")
	}

	body := createScheduleRequest{Queue: queue, Cron: cronExpr, Payload: payload}
	for _, opt := range opts {
		opt(&body)
	}

	var dto scheduleDTO
	if err := c.send(ctx, http.MethodPost, "/v1/schedules", body, &dto); err != nil {
		return nil, err
	}
	return dto.schedule(), nil
}

// AddScheduleJSON marshals v as the payload of every job the schedule
// produces, the recurring counterpart of EnqueueJSON.
func (c *Client) AddScheduleJSON(ctx context.Context, queue, cronExpr string, v any, opts ...ScheduleOption) (*Schedule, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("client: encode payload for schedule on %q: %w", queue, err)
	}
	return c.AddSchedule(ctx, queue, cronExpr, payload, opts...)
}

// GetSchedule returns the schedule with the given ID, or an error wrapping
// ErrNotFound.
func (c *Client) GetSchedule(ctx context.Context, id string) (*Schedule, error) {
	if id == "" {
		return nil, errors.New("client: schedule ID is required")
	}

	var dto scheduleDTO
	if err := c.send(ctx, http.MethodGet, "/v1/schedules/"+url.PathEscape(id), nil, &dto); err != nil {
		return nil, err
	}
	return dto.schedule(), nil
}

// ListSchedules returns every schedule the server holds, most recently
// created first.
//
// It takes no limit, unlike List: a schedule set is written by people and
// stays small, and an operator asking what is scheduled wants all of it.
func (c *Client) ListSchedules(ctx context.Context) ([]*Schedule, error) {
	var body struct {
		Schedules []scheduleDTO `json:"schedules"`
	}
	if err := c.send(ctx, http.MethodGet, "/v1/schedules", nil, &body); err != nil {
		return nil, err
	}

	schedules := make([]*Schedule, len(body.Schedules))
	for i, dto := range body.Schedules {
		schedules[i] = dto.schedule()
	}
	return schedules, nil
}

// ScheduleUpdate describes a partial edit to a schedule. Every field is a
// pointer, and a nil one is left as it is on the server rather than being
// reset -- which is what makes this a patch rather than a replacement. Use
// the Set* helpers to fill it in.
//
//	err := c.UpdateSchedule(ctx, id, client.ScheduleUpdate{}.SetCron("*/5 * * * *"))
type ScheduleUpdate struct {
	Queue          *string `json:"queue,omitempty"`
	Cron           *string `json:"cron,omitempty"`
	Payload        *[]byte `json:"payload,omitempty"`
	Priority       *int    `json:"priority,omitempty"`
	MaxRetries     *int    `json:"max_retries,omitempty"`
	TimeoutSeconds *int    `json:"timeout_seconds,omitempty"`
}

// SetQueue moves the schedule's produced jobs to a different queue.
func (u ScheduleUpdate) SetQueue(name string) ScheduleUpdate {
	u.Queue = &name
	return u
}

// SetCron replaces the cron expression. The server recomputes the next
// fire time from the new expression, and rejects one it cannot parse
// without changing anything.
func (u ScheduleUpdate) SetCron(expr string) ScheduleUpdate {
	u.Cron = &expr
	return u
}

// SetPayload replaces the payload copied onto every produced job.
func (u ScheduleUpdate) SetPayload(payload []byte) ScheduleUpdate {
	u.Payload = &payload
	return u
}

// SetJSONPayload marshals v as the payload copied onto every produced job.
func (u ScheduleUpdate) SetJSONPayload(v any) (ScheduleUpdate, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return u, fmt.Errorf("client: encode schedule payload: %w", err)
	}
	return u.SetPayload(payload), nil
}

// SetPriority replaces the priority applied to every produced job.
func (u ScheduleUpdate) SetPriority(p Priority) ScheduleUpdate {
	v := int(p)
	u.Priority = &v
	return u
}

// SetMaxRetries replaces the retry budget of every produced job.
func (u ScheduleUpdate) SetMaxRetries(n int) ScheduleUpdate {
	u.MaxRetries = &n
	return u
}

// SetTimeout replaces the per-attempt timeout of every produced job,
// rounding up to whole seconds as WithTimeout does.
func (u ScheduleUpdate) SetTimeout(d time.Duration) ScheduleUpdate {
	v := ceilSeconds(d)
	u.TimeoutSeconds = &v
	return u
}

// IsZero reports whether the update would change nothing.
func (u ScheduleUpdate) IsZero() bool {
	return u.Queue == nil && u.Cron == nil && u.Payload == nil &&
		u.Priority == nil && u.MaxRetries == nil && u.TimeoutSeconds == nil
}

// UpdateSchedule applies a partial edit and returns the schedule as it
// stands afterwards.
//
// An edit that names no field is refused here rather than sent, since a
// request that cannot change anything is a mistake in the caller worth
// pointing at.
func (c *Client) UpdateSchedule(ctx context.Context, id string, update ScheduleUpdate) (*Schedule, error) {
	if id == "" {
		return nil, errors.New("client: schedule ID is required")
	}
	if update.IsZero() {
		return nil, errors.New("client: schedule update names no fields to change")
	}

	var dto scheduleDTO
	if err := c.send(ctx, http.MethodPatch, "/v1/schedules/"+url.PathEscape(id), update, &dto); err != nil {
		return nil, err
	}
	return dto.schedule(), nil
}

// PauseSchedule stops a schedule firing without deleting it. A paused
// schedule accumulates no missed firings.
func (c *Client) PauseSchedule(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("client: schedule ID is required")
	}
	return c.send(ctx, http.MethodPost, "/v1/schedules/"+url.PathEscape(id)+"/pause", nil, nil)
}

// ResumeSchedule re-enables a paused schedule. The server recomputes the
// next fire time, so a schedule paused for a week does not fire the
// instant it comes back.
func (c *Client) ResumeSchedule(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("client: schedule ID is required")
	}
	return c.send(ctx, http.MethodPost, "/v1/schedules/"+url.PathEscape(id)+"/resume", nil, nil)
}

// DeleteSchedule removes a schedule permanently. Jobs it has already
// produced are untouched. Deleting one that does not exist succeeds, so a
// cleanup path can retry without interpreting a 404.
func (c *Client) DeleteSchedule(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("client: schedule ID is required")
	}
	return c.send(ctx, http.MethodDelete, "/v1/schedules/"+url.PathEscape(id), nil, nil)
}

// TriggerSchedule runs a schedule now and returns the job it produced,
// leaving the schedule's own timetable untouched: its next fire time does
// not move, and the run is not recorded as a firing.
//
// It works on a paused schedule, since pausing stops the timetable rather
// than the operator.
func (c *Client) TriggerSchedule(ctx context.Context, id string) (*Job, error) {
	if id == "" {
		return nil, errors.New("client: schedule ID is required")
	}

	var dto jobDTO
	if err := c.send(ctx, http.MethodPost, "/v1/schedules/"+url.PathEscape(id)+"/trigger", nil, &dto); err != nil {
		return nil, err
	}
	return dto.job(), nil
}
