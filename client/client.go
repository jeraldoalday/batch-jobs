// Package client is a Go client for the batch-jobs HTTP API: enqueueing
// jobs, looking them up, listing them by status, cancelling and retrying
// them, reading queue stats, and following the live stats stream.
//
// It talks to a server run by cmd/batch-jobs-server (or any process
// serving the same routes) and deliberately shares no types with the
// engine, so importing it costs nothing but the standard library.
//
//	c, err := client.New("http://localhost:8080")
//	if err != nil {
//	    return err
//	}
//	j, err := c.Enqueue(ctx, "emails", []byte(`{"to":"a@example.com"}`),
//	    client.WithMaxRetries(3),
//	    client.WithTimeout(30*time.Second),
//	)
//
// Every method takes a context and returns *Error on a non-2xx response;
// see that type and the sentinel errors in this package for the failure
// model.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultTimeout bounds a single API request when the caller supplies
// neither its own *http.Client nor WithTimeout. It is generous enough for a
// loaded server and short enough that a wedged connection surfaces as an
// error rather than a hang.
const DefaultTimeout = 30 * time.Second

// DefaultUserAgent identifies this client to the server, so a server
// operator reading request logs can tell SDK traffic from curl.
const DefaultUserAgent = "batch-jobs-client/1"

// maxErrorBody bounds how much of a non-2xx response body is read into an
// *Error's Message. A server that answers with a megabyte of HTML (a proxy
// error page, say) should not have that pasted into the caller's logs.
const maxErrorBody = 8 << 10

// Client talks to a batch-jobs server. It is safe for concurrent use by
// multiple goroutines, and is intended to be created once and shared:
// creating one per request defeats the underlying transport's connection
// pooling.
type Client struct {
	baseURL   string
	httpc     *http.Client
	apiKey    string
	userAgent string
}

// Option configures a Client at construction time.
type Option func(*Client)

// WithHTTPClient makes the Client issue its requests through h, for callers
// that need their own transport: a custom TLS config, a proxy, connection
// pool tuning, or an instrumented round tripper.
//
// The supplied client's Timeout is used as-is, so a caller wanting the
// package default must set it themselves; passing nil leaves the default
// client in place.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		if h != nil {
			c.httpc = h
		}
	}
}

// WithAPIKey sends key as the X-API-Key header on every request, which a
// server started with -auth-token requires on everything but /healthz.
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// WithUserAgent overrides DefaultUserAgent, so an application embedding
// this client can identify itself in the server's request logs.
func WithUserAgent(ua string) Option {
	return func(c *Client) {
		if ua != "" {
			c.userAgent = ua
		}
	}
}

// WithRequestTimeout bounds each request, overriding DefaultTimeout. It
// applies to the whole request/response exchange, so it must not be
// combined with Events: following a stream is meant to outlive any single
// timeout, and Events documents how it opts out.
func WithRequestTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.httpc.Timeout = d
		}
	}
}

// New returns a Client for the server at baseURL (e.g.
// "http://localhost:8080"). Any path on baseURL is preserved, so a server
// mounted behind a prefix -- "https://internal.example.com/jobs" -- works
// without the caller rewriting every route.
func New(baseURL string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, errors.New("client: base URL is required")
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("client: parse base URL %q: %w", baseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("client: base URL %q must be http or https", baseURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("client: base URL %q has no host", baseURL)
	}

	c := &Client{
		baseURL:   strings.TrimSuffix(u.String(), "/"),
		httpc:     &http.Client{Timeout: DefaultTimeout},
		userAgent: DefaultUserAgent,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// BaseURL returns the normalized base URL the Client sends requests to.
func (c *Client) BaseURL() string { return c.baseURL }

// newRequest builds a request for path (which must start with "/"),
// encoding body as JSON when it is non-nil.
func (c *Client) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("client: encode %s %s body: %w", method, path, err)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("client: build %s %s: %w", method, path, err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	return req, nil
}

// do sends req and returns the response with a 2xx status, or an *Error
// describing what the server said instead. The caller owns the returned
// body and must close it; on error, do has already drained and closed it.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.httpc.Do(req)
	if err != nil {
		// Unwrap the *url.Error the transport wraps everything in, so a
		// cancelled context still satisfies errors.Is(err,
		// context.Canceled) for the caller.
		return nil, fmt.Errorf("client: %s %s: %w", req.Method, req.URL.Path, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}

	defer resp.Body.Close()
	return nil, newError(req, resp)
}

// send issues a request and decodes a JSON response body into out, which
// may be nil for endpoints that answer with no content.
func (c *Client) send(ctx context.Context, method, path string, body, out any) error {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return err
	}

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() {
		// Drain before closing so the connection can go back to the
		// transport's idle pool rather than being torn down.
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("client: decode %s %s response: %w", method, path, err)
	}
	return nil
}

// newError builds an *Error from a non-2xx response, preferring the API's
// documented {"error": ...} body and falling back to the raw text for
// responses that did not come from the API itself (a proxy, say).
func newError(req *http.Request, resp *http.Response) *Error {
	apiErr := &Error{
		Method:     req.Method,
		Path:       req.URL.Path,
		StatusCode: resp.StatusCode,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if err != nil || len(raw) == 0 {
		return apiErr
	}

	var body struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &body) == nil && body.Error != "" {
		apiErr.Message = body.Error
		return apiErr
	}
	apiErr.Message = strings.TrimSpace(string(raw))
	return apiErr
}

// parseRetryAfter reads the delay-seconds form of the Retry-After header,
// the only form this API sends. An HTTP-date or an unparsable value yields
// zero rather than an error: the hint is advisory, and a caller that gets
// none simply uses its own backoff.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	seconds, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}
