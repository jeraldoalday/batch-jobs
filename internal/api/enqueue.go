package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
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

	// TimeoutSeconds bounds how long a single execution attempt may run
	// before the worker processing it cancels its context. Defaults to 0
	// (no per-attempt timeout).
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`

	// IdempotencyKey, if set, lets a caller safely retry this request
	// (e.g. after a network error) without risking a duplicate job: a
	// second request with the same (Queue, IdempotencyKey) pair returns
	// the original job instead of creating a new one.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
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

	priority, err := parsePriority(req.Priority)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.MaxRetries < 0 {
		writeError(w, http.StatusBadRequest, "max_retries must be >= 0")
		return
	}
	if req.DelaySeconds < 0 {
		writeError(w, http.StatusBadRequest, "delay_seconds must be >= 0")
		return
	}
	if req.TimeoutSeconds < 0 {
		writeError(w, http.StatusBadRequest, "timeout_seconds must be >= 0")
		return
	}

	opts := []job.Option{job.WithPriority(priority), job.WithMaxRetries(req.MaxRetries)}
	if req.DelaySeconds > 0 {
		opts = append(opts, job.WithDelay(time.Duration(req.DelaySeconds)*time.Second))
	}
	if req.TimeoutSeconds > 0 {
		opts = append(opts, job.WithTimeout(time.Duration(req.TimeoutSeconds)*time.Second))
	}
	if req.IdempotencyKey != "" {
		opts = append(opts, job.WithIdempotencyKey(req.IdempotencyKey))
	}
	j := job.New(req.Queue, req.Payload, opts...)
	newID := j.ID

	if err := s.Store.Enqueue(j); err != nil {
		// A full queue is backpressure, not a server fault: the request
		// was well-formed and will likely succeed once workers drain the
		// backlog, so say so with 503 + Retry-After rather than 500.
		if errors.Is(err, queue.ErrFull) {
			s.Logger.Warn("api: enqueue rejected, queue full", "queue", req.Queue)
			writeRetryable(w, http.StatusServiceUnavailable, "queue is full, retry later", s.RetryAfter)
			return
		}
		s.Logger.Error("api: enqueue failed", "queue", req.Queue, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}

	// A Store that supports idempotency keys leaves j.ID unchanged from
	// newID when it inserted a new job, but rewrites *j to an existing
	// job's fields (including a different ID) when this request's key
	// duplicated one already on file. Report that case as 200 with the
	// original job rather than 201, since nothing new was created.
	status := http.StatusCreated
	if j.ID != newID {
		status = http.StatusOK
	}
	writeJSON(w, status, toDTO(j))
}
