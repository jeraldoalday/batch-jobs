package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func doJSON(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		r = httptest.NewRequest(method, path, bytes.NewReader(b))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func TestHandleEnqueueSuccess(t *testing.T) {
	store := newFakeStore()
	s := &Server{Store: store}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{
		Queue:   "emails",
		Payload: []byte("hello"),
	})

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body)
	}

	var got JobDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got.Queue != "emails" {
		t.Errorf("Queue = %q, want %q", got.Queue, "emails")
	}
	if got.Status != string(job.StatusPending) {
		t.Errorf("Status = %q, want %q", got.Status, job.StatusPending)
	}
	if got.ID == "" {
		t.Error("expected non-empty ID")
	}
	if _, ok := store.jobs[got.ID]; !ok {
		t.Error("job was not stored")
	}
}

func TestHandleEnqueueMissingQueue(t *testing.T) {
	s := &Server{Store: newFakeStore()}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Payload: []byte("x")})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleEnqueueInvalidPriority(t *testing.T) {
	s := &Server{Store: newFakeStore()}
	h := s.Handler()

	bogus := 7
	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "q", Priority: &bogus})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleEnqueueNegativeMaxRetries(t *testing.T) {
	s := &Server{Store: newFakeStore()}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "q", MaxRetries: -1})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleEnqueueNegativeDelay(t *testing.T) {
	s := &Server{Store: newFakeStore()}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "q", DelaySeconds: -1})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleEnqueueNegativeTimeout(t *testing.T) {
	s := &Server{Store: newFakeStore()}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "q", TimeoutSeconds: -1})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleEnqueueTimeoutRoundTrips(t *testing.T) {
	store := newFakeStore()
	s := &Server{Store: store}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "q", TimeoutSeconds: 30})

	var got JobDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got.TimeoutSeconds != 30 {
		t.Errorf("TimeoutSeconds = %d, want 30", got.TimeoutSeconds)
	}
}

func TestHandleEnqueueIdempotencyKeyDeduplicates(t *testing.T) {
	store := newFakeStore()
	s := &Server{Store: store}
	h := s.Handler()

	req := EnqueueRequest{Queue: "orders", Payload: []byte("v1"), IdempotencyKey: "order-42"}

	first := doJSON(t, h, http.MethodPost, "/v1/jobs", req)
	if first.Code != http.StatusCreated {
		t.Fatalf("first request status = %d, want %d; body = %s", first.Code, http.StatusCreated, first.Body)
	}
	var firstJob JobDTO
	if err := json.Unmarshal(first.Body.Bytes(), &firstJob); err != nil {
		t.Fatalf("unmarshal first response: %v", err)
	}

	second := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{
		Queue: "orders", Payload: []byte("v2"), IdempotencyKey: "order-42",
	})
	if second.Code != http.StatusOK {
		t.Fatalf("second request status = %d, want %d (duplicate, not created); body = %s", second.Code, http.StatusOK, second.Body)
	}
	var secondJob JobDTO
	if err := json.Unmarshal(second.Body.Bytes(), &secondJob); err != nil {
		t.Fatalf("unmarshal second response: %v", err)
	}

	if secondJob.ID != firstJob.ID {
		t.Errorf("second.ID = %q, want %q (deduped to the original job)", secondJob.ID, firstJob.ID)
	}
	if len(store.jobs) != 1 {
		t.Fatalf("stored jobs = %d, want 1", len(store.jobs))
	}
}

func TestHandleEnqueueMalformedJSON(t *testing.T) {
	s := &Server{Store: newFakeStore()}
	h := s.Handler()

	r := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader("{not json"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleEnqueueStoreError(t *testing.T) {
	store := newFakeStore()
	store.enqueueErr = errBoom
	s := &Server{Store: store}
	h := s.Handler()

	rec := doJSON(t, h, http.MethodPost, "/v1/jobs", EnqueueRequest{Queue: "q"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
