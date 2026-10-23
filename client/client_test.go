package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// stub is a test server that records the last request it saw and answers
// with a canned status and body.
type stub struct {
	*httptest.Server

	mu      sync.Mutex
	method  string
	path    string
	rawURL  string
	headers http.Header
	body    string

	status int
	reply  string
	header map[string]string
}

func newStub(t *testing.T) *stub {
	t.Helper()

	s := &stub{status: http.StatusOK, reply: "{}"}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		s.mu.Lock()
		s.method, s.path, s.rawURL = r.Method, r.URL.Path, r.URL.String()
		s.headers = r.Header.Clone()
		s.body = string(body)
		status, reply, extra := s.status, s.reply, s.header
		s.mu.Unlock()

		for k, v := range extra {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(s.Close)
	return s
}

// respond sets the canned response for the next request.
func (s *stub) respond(status int, body string, headers ...map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.reply = status, body
	if len(headers) > 0 {
		s.header = headers[0]
	}
}

func (s *stub) lastRequest() (method, rawURL, body string, headers http.Header) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.method, s.rawURL, s.body, s.headers
}

// client returns a Client pointed at the stub.
func (s *stub) client(t *testing.T, opts ...Option) *Client {
	t.Helper()
	c, err := New(s.URL, opts...)
	if err != nil {
		t.Fatalf("New(%q) = %v", s.URL, err)
	}
	return c
}

func TestNewValidatesBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
		want    string
	}{
		{name: "plain", url: "http://localhost:8080", want: "http://localhost:8080"},
		{name: "https", url: "https://jobs.example.com", want: "https://jobs.example.com"},
		{name: "trailing slash trimmed", url: "http://localhost:8080/", want: "http://localhost:8080"},
		{name: "path prefix preserved", url: "https://example.com/jobs", want: "https://example.com/jobs"},
		{name: "empty", url: "", wantErr: true},
		{name: "no scheme", url: "localhost:8080", wantErr: true},
		{name: "wrong scheme", url: "ftp://example.com", wantErr: true},
		{name: "no host", url: "http://", wantErr: true},
		{name: "unparsable", url: "http://[::1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("New(%q) = nil error, want one", tt.url)
				}
				return
			}
			if err != nil {
				t.Fatalf("New(%q) = %v", tt.url, err)
			}
			if c.BaseURL() != tt.want {
				t.Errorf("BaseURL() = %q, want %q", c.BaseURL(), tt.want)
			}
		})
	}
}

func TestClientSetsHeaders(t *testing.T) {
	s := newStub(t)
	c := s.client(t, WithAPIKey("secret"), WithUserAgent("test-agent/9"))

	s.respond(http.StatusOK, `{"counts":{}}`)
	if _, err := c.Stats(context.Background()); err != nil {
		t.Fatalf("Stats = %v", err)
	}

	_, _, _, headers := s.lastRequest()
	if got := headers.Get("X-API-Key"); got != "secret" {
		t.Errorf("X-API-Key = %q, want %q", got, "secret")
	}
	if got := headers.Get("User-Agent"); got != "test-agent/9" {
		t.Errorf("User-Agent = %q, want %q", got, "test-agent/9")
	}
	if got := headers.Get("Accept"); got != "application/json" {
		t.Errorf("Accept = %q, want %q", got, "application/json")
	}
}

func TestClientDefaultsUserAgentAndOmitsAPIKey(t *testing.T) {
	s := newStub(t)
	c := s.client(t)

	s.respond(http.StatusOK, `{"counts":{}}`)
	if _, err := c.Stats(context.Background()); err != nil {
		t.Fatalf("Stats = %v", err)
	}

	_, _, _, headers := s.lastRequest()
	if got := headers.Get("User-Agent"); got != DefaultUserAgent {
		t.Errorf("User-Agent = %q, want %q", got, DefaultUserAgent)
	}
	if _, ok := headers["X-Api-Key"]; ok {
		t.Error("X-API-Key was sent even though no key was configured")
	}
}

func TestWithHTTPClientReplacesTransport(t *testing.T) {
	custom := &http.Client{Timeout: 42 * time.Second}
	c, err := New("http://localhost:8080", WithHTTPClient(custom))
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if c.httpc != custom {
		t.Error("WithHTTPClient did not install the supplied *http.Client")
	}

	// A nil client is documented to leave the default in place rather
	// than clearing it.
	before := c.httpc
	c2, err := New("http://localhost:8080", WithHTTPClient(custom), WithHTTPClient(nil))
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if c2.httpc != custom {
		t.Error("WithHTTPClient(nil) replaced an already-configured client")
	}
	_ = before
}

func TestWithRequestTimeoutOverridesDefault(t *testing.T) {
	c, err := New("http://localhost:8080", WithRequestTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if c.httpc.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", c.httpc.Timeout)
	}

	// Documented to leave the existing timeout alone for a non-positive
	// duration rather than disabling it.
	c2, err := New("http://localhost:8080", WithRequestTimeout(5*time.Second), WithRequestTimeout(0))
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if c2.httpc.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want unchanged 5s", c2.httpc.Timeout)
	}
}

func TestClientPreservesBaseURLPath(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	// Simulate a server mounted behind a path prefix by giving the client
	// a base URL with one; the stub answers every path, so what matters is
	// what the client asked for.
	c.baseURL = s.URL + "/api"

	s.respond(http.StatusOK, `{"counts":{}}`)
	if _, err := c.Stats(context.Background()); err != nil {
		t.Fatalf("Stats = %v", err)
	}

	_, rawURL, _, _ := s.lastRequest()
	if rawURL != "/api/v1/stats" {
		t.Errorf("requested %q, want %q", rawURL, "/api/v1/stats")
	}
}

func TestErrorMapsStatusToSentinel(t *testing.T) {
	tests := []struct {
		status int
		want   error
	}{
		{http.StatusNotFound, ErrNotFound},
		{http.StatusConflict, ErrConflict},
		{http.StatusServiceUnavailable, ErrQueueFull},
		{http.StatusTooManyRequests, ErrRateLimited},
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusBadRequest, ErrInvalidRequest},
		{http.StatusNotImplemented, ErrUnsupported},
		{http.StatusInternalServerError, ErrServer},
		{http.StatusBadGateway, ErrServer},
	}

	s := newStub(t)
	c := s.client(t)

	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			s.respond(tt.status, `{"error":"nope"}`)

			_, err := c.Get(context.Background(), "job-1")
			if err == nil {
				t.Fatalf("Get on %d = nil error, want one", tt.status)
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("Get on %d = %v, want it to wrap %v", tt.status, err, tt.want)
			}

			var apiErr *Error
			if !errors.As(err, &apiErr) {
				t.Fatalf("Get on %d = %v, want an *Error", tt.status, err)
			}
			if apiErr.StatusCode != tt.status {
				t.Errorf("StatusCode = %d, want %d", apiErr.StatusCode, tt.status)
			}
			if apiErr.Message != "nope" {
				t.Errorf("Message = %q, want the server's message", apiErr.Message)
			}
			if apiErr.Method != http.MethodGet || apiErr.Path != "/v1/jobs/job-1" {
				t.Errorf("error identifies %s %s, want GET /v1/jobs/job-1", apiErr.Method, apiErr.Path)
			}
		})
	}
}

func TestErrorFallsBackToRawBody(t *testing.T) {
	s := newStub(t)
	c := s.client(t)
	// A proxy in front of the API answers with HTML, not the documented
	// JSON error shape.
	s.respond(http.StatusBadGateway, "<html>upstream is down</html>")

	_, err := c.Get(context.Background(), "job-1")
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("Get = %v, want an *Error", err)
	}
	if !strings.Contains(apiErr.Message, "upstream is down") {
		t.Errorf("Message = %q, want the raw body", apiErr.Message)
	}
}

func TestErrorTemporaryAndRetryAfter(t *testing.T) {
	tests := []struct {
		status        int
		retryAfterHdr string
		wantTemporary bool
		wantAfter     time.Duration
	}{
		{status: http.StatusServiceUnavailable, retryAfterHdr: "3", wantTemporary: true, wantAfter: 3 * time.Second},
		{status: http.StatusTooManyRequests, retryAfterHdr: "1", wantTemporary: true, wantAfter: time.Second},
		{status: http.StatusInternalServerError, wantTemporary: true},
		{status: http.StatusNotFound, wantTemporary: false},
		{status: http.StatusBadRequest, wantTemporary: false},
		{status: http.StatusNotImplemented, wantTemporary: false},
		// An HTTP-date Retry-After is legal HTTP but not what this API
		// sends; it must degrade to "no hint", not to a bogus duration.
		{status: http.StatusServiceUnavailable, retryAfterHdr: "Wed, 21 Oct 2015 07:28:00 GMT", wantTemporary: true},
	}

	s := newStub(t)
	c := s.client(t)

	for _, tt := range tests {
		headers := map[string]string{}
		if tt.retryAfterHdr != "" {
			headers["Retry-After"] = tt.retryAfterHdr
		}
		s.respond(tt.status, `{"error":"nope"}`, headers)

		_, err := c.Get(context.Background(), "job-1")
		if got := Temporary(err); got != tt.wantTemporary {
			t.Errorf("status %d: Temporary() = %v, want %v", tt.status, got, tt.wantTemporary)
		}
		if got := RetryAfter(err, 0); got != tt.wantAfter {
			t.Errorf("status %d: RetryAfter() = %v, want %v", tt.status, got, tt.wantAfter)
		}
		if got := RetryAfter(err, 42*time.Second); tt.wantAfter == 0 && got != 42*time.Second {
			t.Errorf("status %d: RetryAfter fallback = %v, want the fallback", tt.status, got)
		}
	}

	// A transport-level failure is not an *Error, so neither helper should
	// claim to know anything about it.
	if Temporary(errors.New("dial failed")) {
		t.Error("Temporary(non-API error) = true, want false")
	}
	if got := RetryAfter(errors.New("dial failed"), time.Second); got != time.Second {
		t.Errorf("RetryAfter(non-API error) = %v, want the fallback", got)
	}
}

func TestContextCancellationSurfaces(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))
	defer srv.Close()
	defer close(blocked)

	c, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := c.Get(ctx, "job-1"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a cancelled context = %v, want context.Canceled", err)
	}
}
