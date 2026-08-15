package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

// handleGetJob handles GET /v1/jobs/{id}.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	j, err := s.Store.Get(id)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, toDTO(j))
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	default:
		s.Logger.Error("api: get job failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to fetch job")
	}
}

// handleListJobs handles GET /v1/jobs?status=pending&limit=50.
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	statusParam := r.URL.Query().Get("status")
	if statusParam == "" {
		writeError(w, http.StatusBadRequest, "status query parameter is required")
		return
	}
	status := job.Status(statusParam)
	if !status.Valid() {
		writeError(w, http.StatusBadRequest, "invalid status: "+statusParam)
		return
	}

	limit := s.ListDefaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > s.ListMaxLimit {
		limit = s.ListMaxLimit
	}

	jobs, err := s.Store.List(status, limit)
	if err != nil {
		s.Logger.Error("api: list jobs failed", "status", status, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}

	dtos := make([]JobDTO, len(jobs))
	for i, j := range jobs {
		dtos[i] = toDTO(j)
	}
	writeJSON(w, http.StatusOK, ListJobsResponse{Jobs: dtos})
}

// handleCancelJob handles POST /v1/jobs/{id}/cancel.
func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	err := s.Store.Cancel(id)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, queue.ErrNotCancelable):
		writeError(w, http.StatusConflict, "job is not cancelable in its current state")
	default:
		s.Logger.Error("api: cancel job failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
	}
}
