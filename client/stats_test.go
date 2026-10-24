package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStatsDecodesCounts(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"counts":{"pending":3,"running":1,"dead":2}}`)

	stats, err := c.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats = %v", err)
	}

	if got := stats.Get(StatusPending); got != 3 {
		t.Errorf("pending = %d, want 3", got)
	}
	// The server omits statuses with no jobs, so a missing one must read
	// as zero rather than panicking or being absent.
	if got := stats.Get(StatusSucceeded); got != 0 {
		t.Errorf("succeeded = %d, want 0 for an omitted status", got)
	}
	if got := stats.Total(); got != 6 {
		t.Errorf("Total() = %d, want 6", got)
	}
	if got := stats.Active(); got != 4 {
		t.Errorf("Active() = %d, want 4 (pending + running)", got)
	}
	if got := stats.Backlog(); got != 3 {
		t.Errorf("Backlog() = %d, want 3 (pending only, running excluded)", got)
	}
	if got := stats.Failing(); got != 2 {
		t.Errorf("Failing() = %d, want 2 (dead)", got)
	}
}

func TestStatsDerivedCounts(t *testing.T) {
	stats := Stats{
		StatusPending:   1,
		StatusRunning:   2,
		StatusRetrying:  4,
		StatusSucceeded: 8,
		StatusFailed:    16,
		StatusDead:      32,
		StatusCancelled: 64,
	}

	if got := stats.Total(); got != 127 {
		t.Errorf("Total() = %d, want 127", got)
	}
	if got := stats.Active(); got != 7 {
		t.Errorf("Active() = %d, want 7", got)
	}
	if got := stats.Backlog(); got != 5 {
		t.Errorf("Backlog() = %d, want 5", got)
	}
	if got := stats.Failing(); got != 48 {
		t.Errorf("Failing() = %d, want 48", got)
	}
}

func TestStatsString(t *testing.T) {
	if got := (Stats{}).String(); got != "no jobs" {
		t.Errorf("empty Stats.String() = %q, want %q", got, "no jobs")
	}

	// Lifecycle order, not map order, so the value is loggable as-is.
	stats := Stats{StatusDead: 2, StatusPending: 3, StatusRunning: 1}
	if got := stats.String(); got != "pending=3 running=1 dead=2" {
		t.Errorf("String() = %q, want lifecycle order", got)
	}

	// A status from a newer server must still appear, after the known ones.
	stats[Status("archived")] = 5
	if got := stats.String(); !strings.HasSuffix(got, "archived=5") {
		t.Errorf("String() = %q, want the unknown status appended", got)
	}
}

func TestStatusHelpers(t *testing.T) {
	terminal := []Status{StatusSucceeded, StatusFailed, StatusDead, StatusCancelled}
	for _, s := range terminal {
		if !s.Terminal() {
			t.Errorf("%s.Terminal() = false, want true", s)
		}
	}
	for _, s := range []Status{StatusPending, StatusRunning, StatusRetrying} {
		if s.Terminal() {
			t.Errorf("%s.Terminal() = true, want false", s)
		}
	}

	for _, s := range Statuses {
		if !s.Valid() {
			t.Errorf("%s.Valid() = false, want true", s)
		}
	}
	if Status("archived").Valid() {
		t.Error(`Status("archived").Valid() = true, want false`)
	}
}

func TestPriorityHelpers(t *testing.T) {
	for _, p := range []Priority{PriorityLow, PriorityNormal, PriorityHigh} {
		if !p.Valid() {
			t.Errorf("%v.Valid() = false, want true", p)
		}
	}
	if Priority(7).Valid() {
		t.Error("Priority(7).Valid() = true, want false")
	}

	for _, tc := range []struct {
		p    Priority
		want string
	}{
		{PriorityLow, "low"},
		{PriorityNormal, "normal"},
		{PriorityHigh, "high"},
	} {
		if got := tc.p.String(); got != tc.want {
			t.Errorf("%v.String() = %q, want %q", tc.p, got, tc.want)
		}
	}
	if got := Priority(7).String(); !strings.Contains(got, "7") {
		t.Errorf("Priority(7).String() = %q, want it to name the value", got)
	}
}

func TestRecover(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"requeued":4,"dead_lettered":1,"remaining":2}`)

	rec, err := c.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover = %v", err)
	}
	if rec.Requeued != 4 || rec.DeadLettered != 1 || rec.Remaining != 2 {
		t.Errorf("Recover = %+v, want {4 1 2}", rec)
	}
	if got := rec.Total(); got != 5 {
		t.Errorf("Total() = %d, want 5", got)
	}

	method, rawURL, _, _ := s.lastRequest()
	if method != http.MethodPost || rawURL != "/v1/recover" {
		t.Errorf("sent %s %s, want POST /v1/recover", method, rawURL)
	}
}

func TestRecoverAgainstNonLeasingStore(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusNotImplemented, `{"error":"this store does not lease jobs"}`)

	_, err := c.Recover(context.Background())
	if !errors.Is(err, ErrUnsupported) {
		t.Errorf("Recover on 501 = %v, want ErrUnsupported", err)
	}
	// A capability the backend does not have will not appear between two
	// attempts, so retrying is pointless.
	if Temporary(err) {
		t.Error("Temporary(501) = true, want false")
	}
}

func TestHealth(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusOK, `{"status":"ok"}`)

	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health = %v", err)
	}
	if _, rawURL, _, _ := s.lastRequest(); rawURL != "/healthz" {
		t.Errorf("requested %q, want /healthz", rawURL)
	}

	s.respond(http.StatusServiceUnavailable, `{"error":"down"}`)
	if err := c.Health(context.Background()); err == nil {
		t.Error("Health against an unhealthy server = nil, want an error")
	}
}

// sseServer serves a fixed script of SSE frames, then holds the connection
// open until the client goes away, the way a real event stream does.
func sseServer(t *testing.T, frames []string) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server response writer is not a Flusher")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		for _, frame := range frames {
			if _, err := io.WriteString(w, frame); err != nil {
				return
			}
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestEventsReadsFrames(t *testing.T) {
	srv := sseServer(t, []string{
		"data: {\"counts\":{\"pending\":1}}\n\n",
		": keep-alive\n\n",
		"event: stats\nid: 7\n",
		"data: {\"counts\":{\"pending\":2,\"running\":1}}\n\n",
	})

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := c.Events(ctx)
	if err != nil {
		t.Fatalf("Events = %v", err)
	}
	defer stream.Close()

	first, err := stream.Next()
	if err != nil {
		t.Fatalf("first Next = %v", err)
	}
	if got := first.Get(StatusPending); got != 1 {
		t.Errorf("first frame pending = %d, want 1", got)
	}

	// Keep-alive comments and unused SSE fields are skipped, not surfaced
	// as frames or errors.
	second, err := stream.Next()
	if err != nil {
		t.Fatalf("second Next = %v", err)
	}
	if got := second.Get(StatusPending); got != 2 {
		t.Errorf("second frame pending = %d, want 2", got)
	}
	if got := second.Get(StatusRunning); got != 1 {
		t.Errorf("second frame running = %d, want 1", got)
	}
}

func TestEventsCloseUnblocksNext(t *testing.T) {
	srv := sseServer(t, []string{"data: {\"counts\":{}}\n\n"})

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	stream, err := c.Events(context.Background())
	if err != nil {
		t.Fatalf("Events = %v", err)
	}
	if _, err := stream.Next(); err != nil {
		t.Fatalf("first Next = %v", err)
	}

	// The server is holding the connection open with nothing to say, so
	// this Next is parked until Close cuts it loose from another goroutine.
	done := make(chan error, 1)
	go func() {
		_, err := stream.Next()
		done <- err
	}()

	if err := stream.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Errorf("Next after Close = %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not return after Close")
	}

	// Close is idempotent, and a closed stream keeps reporting EOF.
	if err := stream.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	if _, err := stream.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("Next on a closed stream = %v, want io.EOF", err)
	}
}

func TestEventsEndOfStream(t *testing.T) {
	// A server that sends one frame and hangs up ends the stream cleanly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"counts\":{\"dead\":1}}\n\n")
	}))
	defer srv.Close()

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	stream, err := c.Events(context.Background())
	if err != nil {
		t.Fatalf("Events = %v", err)
	}
	defer stream.Close()

	if _, err := stream.Next(); err != nil {
		t.Fatalf("first Next = %v", err)
	}
	if _, err := stream.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("Next at end of stream = %v, want io.EOF", err)
	}
}

func TestEventsRejectsMalformedFrame(t *testing.T) {
	srv := sseServer(t, []string{"data: not json\n\n"})

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	stream, err := c.Events(context.Background())
	if err != nil {
		t.Fatalf("Events = %v", err)
	}
	defer stream.Close()

	if _, err := stream.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Errorf("Next on a malformed frame = %v, want a decode error", err)
	}
}

func TestEventsPropagatesHTTPError(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	s.respond(http.StatusUnauthorized, `{"error":"missing key"}`)

	if _, err := c.Events(context.Background()); !errors.Is(err, ErrUnauthorized) {
		t.Errorf("Events without a key = %v, want ErrUnauthorized", err)
	}
}

func TestWatchStopsOnCallbackError(t *testing.T) {
	srv := sseServer(t, []string{
		"data: {\"counts\":{\"pending\":1}}\n\n",
		"data: {\"counts\":{\"pending\":2}}\n\n",
	})

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New = %v", err)
	}

	stop := errors.New("seen enough")
	seen := 0
	err = c.Watch(context.Background(), func(Stats) error {
		seen++
		if seen == 2 {
			return stop
		}
		return nil
	})

	if !errors.Is(err, stop) {
		t.Errorf("Watch = %v, want the callback's error", err)
	}
	if seen != 2 {
		t.Errorf("callback ran %d times, want 2", seen)
	}
}

func TestWatchStopsOnContextCancel(t *testing.T) {
	srv := sseServer(t, []string{"data: {\"counts\":{}}\n\n"})

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	err = c.Watch(ctx, func(Stats) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Watch after cancel = %v, want context.Canceled", err)
	}
}
