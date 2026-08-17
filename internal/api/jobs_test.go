package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestHandleGetJobFound(t *testing.T) {
	store := newFakeStore()
	j := job.New("emails", []byte("hi"))
	store.jobs[j.ID] = j
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/jobs/"+j.ID, nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var got JobDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("ID = %q, want %q", got.ID, j.ID)
	}
}

func TestHandleGetJobNotFound(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/jobs/nope", nil)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleListJobsFiltersByStatus(t *testing.T) {
	store := newFakeStore()
	pending := job.New("q", nil)
	store.jobs[pending.ID] = pending
	running := job.New("q", nil)
	running.Status = job.StatusRunning
	store.jobs[running.ID] = running
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/jobs?status=pending", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var resp ListJobsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Jobs) != 1 || resp.Jobs[0].ID != pending.ID {
		t.Fatalf("Jobs = %+v, want just %q", resp.Jobs, pending.ID)
	}
}

func TestHandleListJobsRequiresStatus(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/jobs", nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleListJobsRejectsInvalidStatus(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/jobs?status=bogus", nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleListJobsRejectsInvalidLimit(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/jobs?status=pending&limit=-1", nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleListJobsCapsLimit(t *testing.T) {
	store := newFakeStore()
	for i := 0; i < 5; i++ {
		j := job.New("q", nil)
		store.jobs[j.ID] = j
	}
	s := &Server{Store: store, ListMaxLimit: 2}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/jobs?status=pending&limit=1000", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var resp ListJobsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Jobs) > 2 {
		t.Fatalf("got %d jobs, want at most 2 (ListMaxLimit)", len(resp.Jobs))
	}
}

func TestHandleCancelJobSuccess(t *testing.T) {
	store := newFakeStore()
	j := job.New("q", nil)
	store.jobs[j.ID] = j
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs/"+j.ID+"/cancel", nil)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNoContent, rec.Body)
	}
	if store.jobs[j.ID].Status != job.StatusCancelled {
		t.Errorf("Status = %q, want %q", store.jobs[j.ID].Status, job.StatusCancelled)
	}
}

func TestHandleCancelJobNotFound(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs/nope/cancel", nil)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleCancelJobNotCancelable(t *testing.T) {
	store := newFakeStore()
	j := job.New("q", nil)
	j.Status = job.StatusRunning
	store.jobs[j.ID] = j
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs/"+j.ID+"/cancel", nil)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestHandleRetryJobSuccess(t *testing.T) {
	store := newFakeStore()
	j := job.New("q", nil)
	j.Status = job.StatusDead
	j.RetryCount = 3
	j.LastError = "boom"
	store.jobs[j.ID] = j
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs/"+j.ID+"/retry", nil)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNoContent, rec.Body)
	}
	got := store.jobs[j.ID]
	if got.Status != job.StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusPending)
	}
	if got.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", got.RetryCount)
	}
	if got.LastError != "" {
		t.Errorf("LastError = %q, want empty", got.LastError)
	}
}

func TestHandleRetryJobNotFound(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs/nope/retry", nil)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleRetryJobNotRetryable(t *testing.T) {
	store := newFakeStore()
	j := job.New("q", nil)
	store.jobs[j.ID] = j
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs/"+j.ID+"/retry", nil)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}
