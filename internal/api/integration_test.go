package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
)

// newIntegrationServer wires a Server to a real, private in-memory SQLite
// database via queue/sqlite.Queue -- the same Store implementation used in
// production -- and serves it over an actual httptest.Server so requests
// exercise real HTTP encoding/decoding, not just direct ServeHTTP calls.
func newIntegrationServer(t *testing.T) (*httptest.Server, *sqlite.Queue) {
	t.Helper()
	q, err := sqlite.Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	srv := &Server{Store: q}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, q
}

func TestIntegrationEnqueueGetListCancelStats(t *testing.T) {
	ts, _ := newIntegrationServer(t)
	client := ts.Client()

	// Enqueue.
	body, _ := json.Marshal(EnqueueRequest{Queue: "reports", Payload: []byte("payload"), MaxRetries: 2})
	resp, err := client.Post(ts.URL+"/v1/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/jobs: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/jobs status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	var created JobDTO
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode enqueue response: %v", err)
	}
	resp.Body.Close()
	if created.Queue != "reports" {
		t.Errorf("Queue = %q, want %q", created.Queue, "reports")
	}

	// Get.
	resp, err = client.Get(ts.URL + "/v1/jobs/" + created.ID)
	if err != nil {
		t.Fatalf("GET /v1/jobs/{id}: %v", err)
	}
	var fetched JobDTO
	if err := json.NewDecoder(resp.Body).Decode(&fetched); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	resp.Body.Close()
	if fetched.ID != created.ID {
		t.Errorf("fetched ID = %q, want %q", fetched.ID, created.ID)
	}
	if string(fetched.Payload) != "payload" {
		t.Errorf("Payload = %q, want %q", fetched.Payload, "payload")
	}

	// List.
	resp, err = client.Get(ts.URL + "/v1/jobs?status=pending")
	if err != nil {
		t.Fatalf("GET /v1/jobs: %v", err)
	}
	var listed ListJobsResponse
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	resp.Body.Close()
	found := false
	for _, j := range listed.Jobs {
		if j.ID == created.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("listed jobs %+v does not contain %q", listed.Jobs, created.ID)
	}

	// Stats.
	resp, err = client.Get(ts.URL + "/v1/stats")
	if err != nil {
		t.Fatalf("GET /v1/stats: %v", err)
	}
	var stats StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats response: %v", err)
	}
	resp.Body.Close()
	if stats.Counts["pending"] != 1 {
		t.Errorf("Counts[pending] = %d, want 1", stats.Counts["pending"])
	}

	// Cancel.
	resp, err = client.Post(ts.URL+"/v1/jobs/"+created.ID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /v1/jobs/{id}/cancel: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("cancel status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Get again: status should now be cancelled.
	resp, err = client.Get(ts.URL + "/v1/jobs/" + created.ID)
	if err != nil {
		t.Fatalf("GET /v1/jobs/{id} after cancel: %v", err)
	}
	var afterCancel JobDTO
	if err := json.NewDecoder(resp.Body).Decode(&afterCancel); err != nil {
		t.Fatalf("decode post-cancel response: %v", err)
	}
	resp.Body.Close()
	if afterCancel.Status != "cancelled" {
		t.Errorf("Status after cancel = %q, want %q", afterCancel.Status, "cancelled")
	}

	// Cancelling again should now 409, since it's terminal.
	resp, err = client.Post(ts.URL+"/v1/jobs/"+created.ID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /v1/jobs/{id}/cancel (second time): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("re-cancel status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
}

func TestIntegrationEnqueuedJobIsDequeuableByWorker(t *testing.T) {
	ts, q := newIntegrationServer(t)
	client := ts.Client()

	body, _ := json.Marshal(EnqueueRequest{Queue: "emails", Payload: []byte("hi")})
	resp, err := client.Post(ts.URL+"/v1/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/jobs: %v", err)
	}
	resp.Body.Close()

	got, err := q.TryDequeue()
	if err != nil {
		t.Fatalf("TryDequeue: %v", err)
	}
	if got.Queue != "emails" {
		t.Errorf("Queue = %q, want %q", got.Queue, "emails")
	}
}

func TestIntegrationHealthzUnauthenticated(t *testing.T) {
	q, err := sqlite.Open("file:" + t.Name() + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	srv := &Server{Store: q, AuthToken: "secret"}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	resp, err = ts.Client().Get(ts.URL + "/v1/stats")
	if err != nil {
		t.Fatalf("GET /v1/stats: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /v1/stats status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}
