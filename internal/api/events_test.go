package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

func TestHandleEventsStreamsStats(t *testing.T) {
	store := newFakeStore()
	j := job.New("q", nil)
	store.jobs[j.ID] = j
	s := &Server{Store: store, EventInterval: 10 * time.Millisecond}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/events", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want %q", ct, "text/event-stream")
	}

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	data, ok := strings.CutPrefix(strings.TrimSpace(line), "data: ")
	if !ok {
		t.Fatalf("line = %q, want %q prefix", line, "data: ")
	}

	var payload StatsResponse
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	if payload.Counts["pending"] != 1 {
		t.Errorf("Counts[pending] = %d, want 1", payload.Counts["pending"])
	}
}

func TestHandleEventsStopsOnStoreError(t *testing.T) {
	store := newFakeStore()
	store.statsErr = errBoom
	s := &Server{Store: store, EventInterval: 10 * time.Millisecond}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	// The stream ends immediately (the first snapshot fails), so the body
	// should be empty rather than hang waiting for a frame that never
	// comes.
	buf := make([]byte, 1)
	n, err := resp.Body.Read(buf)
	if n != 0 {
		t.Errorf("read %d bytes from a store-error stream, want 0", n)
	}
	_ = err
}

// flusherlessWriter wraps an httptest.ResponseRecorder but hides its
// http.Flusher implementation, simulating a ResponseWriter that can't
// stream.
type flusherlessWriter struct {
	http.ResponseWriter
}

func TestHandleEventsRequiresFlusher(t *testing.T) {
	s := &Server{Store: newFakeStore()}
	rec := httptest.NewRecorder()
	w := flusherlessWriter{ResponseWriter: rec}

	r := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	s.handleEvents(w, r)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
