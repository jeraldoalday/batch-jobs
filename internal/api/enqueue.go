package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// EnqueueRequest is the request body for POST /v1/jobs.
type EnqueueRequest struct {
	// Queue is the logical queue the job belongs to. Required.
	Queue string `json:"queue"`

	// Payload is the job's opaque handler-specific data, base64-encoded in
	// the JSON body (encoding/json's standard []byte handling).
	Payload []byte `json:"payload,omitempty"`

	// Priority is one of -1 (low), 0 (normal), 1 (high). Defaults to 0.
	Priority *int `json:"priority,omitempty"`

	// MaxRetries is the number of additional attempts allowed after the
	// first failure. Defaults to 0.
	MaxRetries int `json:"max_retries,omitempty"`

	// DelaySeconds delays the job's earliest eligible run time by this
	// many seconds from now. Defaults to 0 (immediately eligible).
	DelaySeconds int `json:"delay_seconds,omitempty"`
}

// handleEnqueue handles POST /v1/jobs.
func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Queue == "" {
		writeError(w, http.StatusBadRequest, "queue is required")
		return
	}

	priority := job.PriorityNormal
	if req.Priority != nil {
		p := job.Priority(*req.Priority)
		switch p {
		case job.PriorityLow, job.PriorityNormal, job.PriorityHigh:
			priority = p
		default:
			writeError(w, http.StatusBadRequest, "priority must be -1 (low), 0 (normal), or 1 (high)")
			return
		}
	}
	if req.MaxRetries < 0 {
		writeError(w, http.StatusBadRequest, "max_retries must be >= 0")
		return
	}
	if req.DelaySeconds < 0 {
		writeError(w, http.StatusBadRequest, "delay_seconds must be >= 0")
		return
	}

	opts := []job.Option{job.WithPriority(priority), job.WithMaxRetries(req.MaxRetries)}
	if req.DelaySeconds > 0 {
		opts = append(opts, job.WithDelay(time.Duration(req.DelaySeconds)*time.Second))
	}
	j := job.New(req.Queue, req.Payload, opts...)

	if err := s.Store.Enqueue(j); err != nil {
		s.Logger.Error("api: enqueue failed", "queue", req.Queue, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}
	writeJSON(w, http.StatusCreated, toDTO(j))
}
