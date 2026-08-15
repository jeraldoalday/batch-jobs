package api

import (
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// JobDTO is the JSON representation of a job returned by the API. Payload
// marshals as a base64 string, matching how encoding/json already treats
// []byte fields.
type JobDTO struct {
	ID         string    `json:"id"`
	Queue      string    `json:"queue"`
	Payload    []byte    `json:"payload,omitempty"`
	Status     string    `json:"status"`
	Priority   int       `json:"priority"`
	MaxRetries int       `json:"max_retries"`
	RetryCount int       `json:"retry_count"`
	LastError  string    `json:"last_error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	RunAt      time.Time `json:"run_at"`
}

// toDTO converts a job.Job to its JSON representation.
func toDTO(j *job.Job) JobDTO {
	return JobDTO{
		ID:         j.ID,
		Queue:      j.Queue,
		Payload:    j.Payload,
		Status:     string(j.Status),
		Priority:   int(j.Priority),
		MaxRetries: j.MaxRetries,
		RetryCount: j.RetryCount,
		LastError:  j.LastError,
		CreatedAt:  j.CreatedAt,
		UpdatedAt:  j.UpdatedAt,
		RunAt:      j.RunAt,
	}
}

// ListJobsResponse is the response body for GET /v1/jobs.
type ListJobsResponse struct {
	Jobs []JobDTO `json:"jobs"`
}

// StatsResponse is the response body for GET /v1/stats.
type StatsResponse struct {
	Counts map[string]int `json:"counts"`
}

// errorResponse is the response body for any non-2xx response.
type errorResponse struct {
	Error string `json:"error"`
}
