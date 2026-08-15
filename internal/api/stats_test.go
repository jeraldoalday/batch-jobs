package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestHandleStats(t *testing.T) {
	store := newFakeStore()
	a := job.New("q", nil)
	store.jobs[a.ID] = a
	b := job.New("q", nil)
	b.Status = job.StatusSucceeded
	store.jobs[b.ID] = b
	c := job.New("q", nil)
	c.Status = job.StatusSucceeded
	store.jobs[c.ID] = c
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/stats", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body)
	}
	var resp StatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Counts["pending"] != 1 {
		t.Errorf("Counts[pending] = %d, want 1", resp.Counts["pending"])
	}
	if resp.Counts["succeeded"] != 2 {
		t.Errorf("Counts[succeeded] = %d, want 2", resp.Counts["succeeded"])
	}
}

func TestHandleStatsStoreError(t *testing.T) {
	store := newFakeStore()
	store.statsErr = errBoom
	h := (&Server{Store: store}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/v1/stats", nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestHandleHealthz(t *testing.T) {
	h := (&Server{Store: newFakeStore()}).Handler()

	rec := doJSON(t, h, http.MethodGet, "/healthz", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}
