package api

import (
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
)

// JobDTO is the JSON representation of a job returned by the API. Payload
// marshals as a base64 string, matching how encoding/json already treats
// []byte fields.
type JobDTO struct {
	ID             string    `json:"id"`
	Queue          string    `json:"queue"`
	Payload        []byte    `json:"payload,omitempty"`
	Status         string    `json:"status"`
	Priority       int       `json:"priority"`
	MaxRetries     int       `json:"max_retries"`
	RetryCount     int       `json:"retry_count"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	RunAt          time.Time `json:"run_at"`
	TimeoutSeconds int       `json:"timeout_seconds,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
}

// toDTO converts a job.Job to its JSON representation.
func toDTO(j *job.Job) JobDTO {
	return JobDTO{
		ID:             j.ID,
		Queue:          j.Queue,
		Payload:        j.Payload,
		Status:         string(j.Status),
		Priority:       int(j.Priority),
		MaxRetries:     j.MaxRetries,
		RetryCount:     j.RetryCount,
		LastError:      j.LastError,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		RunAt:          j.RunAt,
		TimeoutSeconds: int(j.Timeout / time.Second),
		IdempotencyKey: j.IdempotencyKey,
	}
}

// ScheduleUpcomingCount is how many future fire times a schedule's JSON
// representation carries. Three is enough to read a cron expression's
// intent at a glance -- hourly, nightly, weekdays-only -- which is what a
// caller checking their own expression actually needs.
const ScheduleUpcomingCount = 3

// ScheduleDTO is the JSON representation of a recurring schedule. As with
// JobDTO, the payload marshals as a base64 string.
type ScheduleDTO struct {
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

// toScheduleDTO converts a schedule.Schedule to its JSON representation.
func toScheduleDTO(s *schedule.Schedule) ScheduleDTO {
	dto := ScheduleDTO{
		ID:             s.ID,
		Queue:          s.Queue,
		Cron:           s.CronExpr,
		Payload:        s.Payload,
		Priority:       int(s.Priority),
		MaxRetries:     s.MaxRetries,
		TimeoutSeconds: int(s.Timeout / time.Second),
		Enabled:        s.Enabled,
		NextRunAt:      s.NextRunAt,
		CreatedAt:      s.CreatedAt,
		UpdatedAt:      s.UpdatedAt,
	}
	// A schedule that has never fired reports no last run at all, rather
	// than the zero time -- which a client would have to know to
	// recognize as "never" rather than "in the year 1".
	if !s.LastRunAt.IsZero() {
		lastRun := s.LastRunAt
		dto.LastRunAt = &lastRun
	}
	// An unparsable expression cannot be projected forward, but the rest
	// of the schedule is still worth returning: the field is simply
	// omitted rather than the whole response failing.
	if upcoming, err := s.Upcoming(ScheduleUpcomingCount); err == nil {
		dto.Upcoming = upcoming
	}
	return dto
}

// ListSchedulesResponse is the response body for GET /v1/schedules.
type ListSchedulesResponse struct {
	Schedules []ScheduleDTO `json:"schedules"`
}

// ListJobsResponse is the response body for GET /v1/jobs.
type ListJobsResponse struct {
	Jobs []JobDTO `json:"jobs"`
}

// StatsResponse is the response body for GET /v1/stats.
type StatsResponse struct {
	Counts map[string]int `json:"counts"`
}

// RecoverResponse is the response body for POST /v1/recover.
type RecoverResponse struct {
	// Requeued is how many orphaned jobs were put back in the queue.
	Requeued int `json:"requeued"`

	// DeadLettered is how many orphaned jobs had no retries left.
	DeadLettered int `json:"dead_lettered"`

	// Remaining is how many expired claims are still outstanding after
	// the pass, normally zero.
	Remaining int `json:"remaining"`
}

// errorResponse is the response body for any non-2xx response.
type errorResponse struct {
	Error string `json:"error"`
}
