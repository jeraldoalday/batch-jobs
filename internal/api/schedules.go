package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// ScheduleStore is the schedule surface the HTTP API needs, satisfied by
// schedule.Manager. It is an interface for the same reason Store is: the
// handlers are transport, and every rule about what an edit may change
// lives behind this line.
type ScheduleStore interface {
	// Add validates the cron expression and persists an enabled schedule.
	Add(queueName, cronExpr string, payload []byte, opts ...schedule.Option) (*schedule.Schedule, error)

	// Get returns the schedule with the given ID, or schedule.ErrNotFound.
	Get(id string) (*schedule.Schedule, error)

	// List returns every schedule, most recently created first.
	List() ([]*schedule.Schedule, error)

	// Update applies edit to the stored schedule and persists the result.
	Update(id string, edit func(*schedule.Schedule) error) (*schedule.Schedule, error)

	// Pause stops a schedule firing without deleting it.
	Pause(id string) error

	// Resume re-enables a paused schedule, clearing an overdue fire time.
	Resume(id string) error

	// Delete removes a schedule permanently.
	Delete(id string) error

	// Trigger enqueues a schedule's job now, leaving its timetable alone.
	Trigger(id string) (*job.Job, error)
}

// CreateScheduleRequest is the request body for POST /v1/schedules.
type CreateScheduleRequest struct {
	// Queue is the queue each produced job is enqueued onto. Required.
	Queue string `json:"queue"`

	// Cron is the 5-field cron expression -- minute, hour, day of month,
	// month, day of week -- deciding when the schedule fires. Required.
	Cron string `json:"cron"`

	// Payload is copied onto every job the schedule produces,
	// base64-encoded in the JSON body.
	Payload []byte `json:"payload,omitempty"`

	// Priority is one of -1 (low), 0 (normal), 1 (high). Defaults to 0.
	Priority *int `json:"priority,omitempty"`

	// MaxRetries is applied to every job the schedule produces.
	MaxRetries int `json:"max_retries,omitempty"`

	// TimeoutSeconds bounds a single attempt of every job the schedule
	// produces. Defaults to 0 (no per-attempt timeout).
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// UpdateScheduleRequest is the request body for PATCH
// /v1/schedules/{id}. Every field is a pointer, so an omitted one is
// left as it is rather than being reset to its zero value -- which is
// what makes this a patch rather than a replacement.
type UpdateScheduleRequest struct {
	Queue          *string `json:"queue,omitempty"`
	Cron           *string `json:"cron,omitempty"`
	Payload        *[]byte `json:"payload,omitempty"`
	Priority       *int    `json:"priority,omitempty"`
	MaxRetries     *int    `json:"max_retries,omitempty"`
	TimeoutSeconds *int    `json:"timeout_seconds,omitempty"`
}

// schedules returns the configured ScheduleStore, or writes 501 and
// reports false. A server without scheduling answers the well-formed
// request with "this server does not do that" rather than a 404 the
// caller cannot tell from a typo in the path.
func (s *Server) schedules(w http.ResponseWriter) (ScheduleStore, bool) {
	if s.ScheduleStore == nil {
		writeError(w, http.StatusNotImplemented, "scheduling is not enabled on this server")
		return nil, false
	}
	return s.ScheduleStore, true
}

// writeScheduleError maps a store error onto a status code, so every
// handler below reports the same failure the same way.
func (s *Server) writeScheduleError(w http.ResponseWriter, op, id string, err error) {
	switch {
	case errors.Is(err, schedule.ErrNotFound):
		writeError(w, http.StatusNotFound, "schedule not found")
	case errors.Is(err, queue.ErrFull):
		writeRetryable(w, http.StatusServiceUnavailable, "queue is full, retry later", s.RetryAfter)
	default:
		s.Logger.Error("api: "+op+" failed", "schedule_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to "+op)
	}
}

// handleCreateSchedule handles POST /v1/schedules.
func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	store, ok := s.schedules(w)
	if !ok {
		return
	}

	var req CreateScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Queue == "" {
		writeError(w, http.StatusBadRequest, "queue is required")
		return
	}
	if req.Cron == "" {
		writeError(w, http.StatusBadRequest, "cron is required")
		return
	}

	priority, err := parsePriority(req.Priority)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.MaxRetries < 0 {
		writeError(w, http.StatusBadRequest, "max_retries must be >= 0")
		return
	}
	if req.TimeoutSeconds < 0 {
		writeError(w, http.StatusBadRequest, "timeout_seconds must be >= 0")
		return
	}

	sch, err := store.Add(req.Queue, req.Cron, req.Payload,
		schedule.WithPriority(priority),
		schedule.WithMaxRetries(req.MaxRetries),
		schedule.WithTimeout(time.Duration(req.TimeoutSeconds)*time.Second),
	)
	if err != nil {
		// A cron expression the parser rejects is the caller's mistake,
		// and the parser's message names the field that is wrong, so it
		// is worth passing on rather than flattening to "bad request".
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, toScheduleDTO(sch))
}

// handleGetSchedule handles GET /v1/schedules/{id}.
func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	store, ok := s.schedules(w)
	if !ok {
		return
	}

	id := r.PathValue("id")
	sch, err := store.Get(id)
	if err != nil {
		s.writeScheduleError(w, "fetch schedule", id, err)
		return
	}
	writeJSON(w, http.StatusOK, toScheduleDTO(sch))
}

// handleListSchedules handles GET /v1/schedules.
//
// It takes no limit or filter: a schedule set is written by people and
// stays small, and an operator asking what is scheduled wants all of it.
func (s *Server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	store, ok := s.schedules(w)
	if !ok {
		return
	}

	schedules, err := store.List()
	if err != nil {
		s.writeScheduleError(w, "list schedules", "", err)
		return
	}

	dtos := make([]ScheduleDTO, len(schedules))
	for i, sch := range schedules {
		dtos[i] = toScheduleDTO(sch)
	}
	writeJSON(w, http.StatusOK, ListSchedulesResponse{Schedules: dtos})
}

// handleUpdateSchedule handles PATCH /v1/schedules/{id}.
func (s *Server) handleUpdateSchedule(w http.ResponseWriter, r *http.Request) {
	store, ok := s.schedules(w)
	if !ok {
		return
	}

	var req UpdateScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	// Everything that can be judged without reading the schedule is
	// judged here, so a bad request is a 400 rather than a 500 from
	// deep inside the edit.
	var priority job.Priority
	if req.Priority != nil {
		p, err := parsePriority(req.Priority)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		priority = p
	}
	if req.Queue != nil && *req.Queue == "" {
		writeError(w, http.StatusBadRequest, "queue must not be empty")
		return
	}
	if req.MaxRetries != nil && *req.MaxRetries < 0 {
		writeError(w, http.StatusBadRequest, "max_retries must be >= 0")
		return
	}
	if req.TimeoutSeconds != nil && *req.TimeoutSeconds < 0 {
		writeError(w, http.StatusBadRequest, "timeout_seconds must be >= 0")
		return
	}

	id := r.PathValue("id")
	sch, err := store.Update(id, func(sch *schedule.Schedule) error {
		if req.Queue != nil {
			sch.Queue = *req.Queue
		}
		if req.Cron != nil {
			sch.CronExpr = *req.Cron
		}
		if req.Payload != nil {
			sch.Payload = *req.Payload
		}
		if req.Priority != nil {
			sch.Priority = priority
		}
		if req.MaxRetries != nil {
			sch.MaxRetries = *req.MaxRetries
		}
		if req.TimeoutSeconds != nil {
			sch.Timeout = time.Duration(*req.TimeoutSeconds) * time.Second
		}
		return nil
	})
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, toScheduleDTO(sch))
	case errors.Is(err, schedule.ErrNotFound):
		writeError(w, http.StatusNotFound, "schedule not found")
	default:
		// What is left is an expression the parser rejected, which is
		// the caller's mistake and worth reporting in its own words.
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

// handleDeleteSchedule handles DELETE /v1/schedules/{id}.
func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	store, ok := s.schedules(w)
	if !ok {
		return
	}

	id := r.PathValue("id")
	if err := store.Delete(id); err != nil {
		s.writeScheduleError(w, "delete schedule", id, err)
		return
	}
	// Deleting an absent schedule succeeds, which makes DELETE
	// idempotent: a client that retries after a lost response gets the
	// same answer rather than a 404 it has to interpret.
	w.WriteHeader(http.StatusNoContent)
}

// handlePauseSchedule handles POST /v1/schedules/{id}/pause.
func (s *Server) handlePauseSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, false)
}

// handleResumeSchedule handles POST /v1/schedules/{id}/resume.
func (s *Server) handleResumeSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, true)
}

// setScheduleEnabled is the shared body of the pause and resume handlers.
func (s *Server) setScheduleEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	store, ok := s.schedules(w)
	if !ok {
		return
	}

	id := r.PathValue("id")
	op, err := "pause schedule", error(nil)
	if enabled {
		op, err = "resume schedule", store.Resume(id)
	} else {
		err = store.Pause(id)
	}
	if err != nil {
		s.writeScheduleError(w, op, id, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleTriggerSchedule handles POST /v1/schedules/{id}/trigger, running
// a schedule now without disturbing its timetable. It answers with the
// job it created, so a caller can follow that job's progress.
func (s *Server) handleTriggerSchedule(w http.ResponseWriter, r *http.Request) {
	store, ok := s.schedules(w)
	if !ok {
		return
	}

	id := r.PathValue("id")
	j, err := store.Trigger(id)
	if err != nil {
		s.writeScheduleError(w, "trigger schedule", id, err)
		return
	}
	writeJSON(w, http.StatusCreated, toDTO(j))
}

// parsePriority validates the wire representation of a priority, where
// nil means "unset" and resolves to normal.
func parsePriority(v *int) (job.Priority, error) {
	if v == nil {
		return job.PriorityNormal, nil
	}
	p := job.Priority(*v)
	switch p {
	case job.PriorityLow, job.PriorityNormal, job.PriorityHigh:
		return p, nil
	default:
		return 0, errors.New("priority must be -1 (low), 0 (normal), or 1 (high)")
	}
}
