package client

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Sentinel errors for the failures a caller is likely to branch on. Every
// one of them is reachable through errors.Is on the error returned by a
// Client method; use errors.As with *Error to get at the status code,
// server message, and Retry-After hint behind it.
var (
	// ErrNotFound reports that no job with the given ID exists (HTTP 404).
	ErrNotFound = errors.New("client: job not found")

	// ErrConflict reports that the job exists but is not in a state that
	// permits the operation (HTTP 409): Cancel on a job already running or
	// finished, Retry on a job that is not dead or failed.
	ErrConflict = errors.New("client: job is not in a state that allows this")

	// ErrQueueFull reports that the server is applying backpressure and
	// declined to accept the job (HTTP 503). It is temporary by
	// definition: the same request will likely succeed once workers drain
	// the backlog. *Error's RetryAfter carries the server's hint.
	ErrQueueFull = errors.New("client: queue is full")

	// ErrRateLimited reports that this client's IP has exceeded the
	// server's per-client request rate (HTTP 429), not that anything is
	// wrong with the request.
	ErrRateLimited = errors.New("client: rate limited")

	// ErrUnauthorized reports a missing or wrong API key (HTTP 401).
	ErrUnauthorized = errors.New("client: unauthorized")

	// ErrInvalidRequest reports that the server rejected the request as
	// malformed (HTTP 400) -- an unknown status filter, a negative retry
	// count, a payload the server would not parse.
	ErrInvalidRequest = errors.New("client: invalid request")

	// ErrUnsupported reports that the server understood the request but
	// its backend has no such capability (HTTP 501) -- Recover against a
	// store that does not lease jobs, for instance.
	ErrUnsupported = errors.New("client: server does not support this operation")

	// ErrServer reports a server-side failure (HTTP 5xx other than 503).
	ErrServer = errors.New("client: server error")
)

// Error is the error returned for any non-2xx response. It keeps the
// request that produced it alongside the server's own message, because a
// bare "404 not found" in a log tells an operator nothing about which call
// failed.
type Error struct {
	// Method and Path identify the request, e.g. "POST /v1/jobs".
	Method string
	Path   string

	// StatusCode is the HTTP status the server returned.
	StatusCode int

	// Message is the server's {"error": ...} text, or the raw body when
	// the response was not the JSON error shape the API documents.
	Message string

	// RetryAfter is the server's Retry-After hint, when it sent one. It is
	// set for backpressure (503) and rate limiting (429), and zero
	// otherwise.
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("client: %s %s: %d %s", e.Method, e.Path, e.StatusCode, http.StatusText(e.StatusCode))
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.RetryAfter > 0 {
		msg += fmt.Sprintf(" (retry after %s)", e.RetryAfter)
	}
	return msg
}

// Unwrap maps the status code onto this package's sentinel errors, so
// callers can write errors.Is(err, client.ErrNotFound) without caring that
// the concrete error is an *Error carrying a status code.
func (e *Error) Unwrap() error {
	switch e.StatusCode {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusConflict:
		return ErrConflict
	case http.StatusServiceUnavailable:
		return ErrQueueFull
	case http.StatusTooManyRequests:
		return ErrRateLimited
	case http.StatusUnauthorized:
		return ErrUnauthorized
	case http.StatusBadRequest:
		return ErrInvalidRequest
	case http.StatusNotImplemented:
		return ErrUnsupported
	}
	if e.StatusCode >= 500 {
		return ErrServer
	}
	return nil
}

// Temporary reports whether the request is worth repeating as-is. Rate
// limiting, backpressure, and server faults are; a 404 or a malformed
// request is not, and retrying it only turns one wrong answer into many.
func (e *Error) Temporary() bool {
	switch e.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return true
	case http.StatusNotImplemented:
		// The backend will not grow the capability between two attempts.
		return false
	}
	return e.StatusCode >= 500
}

// Temporary reports whether err came from the server and is worth
// retrying, as *Error.Temporary defines it. It is the package-level form,
// for callers that would rather not unwrap the error themselves.
func Temporary(err error) bool {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		return apiErr.Temporary()
	}
	return false
}

// RetryAfter returns the server's Retry-After hint from err, or fallback if
// err carries none. Servers are not required to send the header, so a
// caller that backs off needs a default anyway.
func RetryAfter(err error, fallback time.Duration) time.Duration {
	var apiErr *Error
	if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
		return apiErr.RetryAfter
	}
	return fallback
}
