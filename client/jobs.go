package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// enqueueRequest is the wire body for POST /v1/jobs.
type enqueueRequest struct {
	Queue          string `json:"queue"`
	Payload        []byte `json:"payload,omitempty"`
	Priority       *int   `json:"priority,omitempty"`
	MaxRetries     int    `json:"max_retries,omitempty"`
	DelaySeconds   int    `json:"delay_seconds,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// EnqueueOption customizes a job being enqueued. The options mirror the
// engine's own job options, so code that moves from embedding the library
// to talking to a server over HTTP reads the same.
type EnqueueOption func(*enqueueRequest)

// WithPriority sets the job's dequeue priority relative to other ready
// jobs. Higher priorities run first.
func WithPriority(p Priority) EnqueueOption {
	return func(r *enqueueRequest) {
		v := int(p)
		r.Priority = &v
	}
}

// WithMaxRetries allows n further attempts after the first failure, after
// which the job is dead-lettered.
func WithMaxRetries(n int) EnqueueOption {
	return func(r *enqueueRequest) { r.MaxRetries = n }
}

// WithDelay makes the job ineligible to run until d has elapsed.
//
// The API expresses delays in whole seconds, so a sub-second d rounds up to
// one second rather than down to "run immediately": a caller who asked for
// a delay should never get a job that runs at once.
func WithDelay(d time.Duration) EnqueueOption {
	return func(r *enqueueRequest) { r.DelaySeconds = ceilSeconds(d) }
}

// WithRunAt makes the job ineligible to run until t. It is WithDelay
// expressed as an instant; a time already past leaves the job immediately
// eligible.
func WithRunAt(t time.Time) EnqueueOption {
	return func(r *enqueueRequest) { r.DelaySeconds = ceilSeconds(time.Until(t)) }
}

// WithTimeout bounds a single attempt to d, after which the worker running
// the job cancels its context. As with WithDelay, a sub-second d rounds up
// to one second, since the API's unit is whole seconds and a timeout that
// rounded to zero would silently mean "no timeout".
func WithTimeout(d time.Duration) EnqueueOption {
	return func(r *enqueueRequest) { r.TimeoutSeconds = ceilSeconds(d) }
}

// WithIdempotencyKey makes the enqueue safe to repeat: a second request
// carrying the same key for the same queue returns the job the first one
// created instead of creating another. Use it whenever a failed enqueue
// will be retried and a duplicate job would be harmful -- charging a card,
// sending an email.
//
// EnqueueResult reports which of the two happened; Enqueue hides it, since
// the point of a key is that the caller can stop caring.
func WithIdempotencyKey(key string) EnqueueOption {
	return func(r *enqueueRequest) { r.IdempotencyKey = key }
}

// ceilSeconds converts d to whole seconds, rounding any positive remainder
// up so a duration the caller meant as "not zero" never crosses the wire as
// zero. Non-positive durations become 0, the API's "unset".
func ceilSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int((d + time.Second - 1) / time.Second)
}

// Enqueue submits a job to the named queue and returns it as stored,
// including the server-assigned ID.
//
// When the job carries an idempotency key that matched an existing job, the
// returned job is that original one -- use EnqueueResult to tell the two
// apart.
func (c *Client) Enqueue(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) (*Job, error) {
	j, _, err := c.EnqueueResult(ctx, queue, payload, opts...)
	return j, err
}

// EnqueueResult is Enqueue, additionally reporting whether the server
// created a new job (true) or returned one an idempotency key had already
// claimed (false).
func (c *Client) EnqueueResult(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) (*Job, bool, error) {
	if queue == "" {
		return nil, false, errors.New("client: queue is required")
	}

	body := enqueueRequest{Queue: queue, Payload: payload}
	for _, opt := range opts {
		opt(&body)
	}

	req, err := c.newRequest(ctx, http.MethodPost, "/v1/jobs", body)
	if err != nil {
		return nil, false, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	var dto jobDTO
	if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
		return nil, false, fmt.Errorf("client: decode POST /v1/jobs response: %w", err)
	}
	// The API distinguishes the two outcomes by status code: 201 for a job
	// it created, 200 for one an idempotency key already covered.
	return dto.job(), resp.StatusCode == http.StatusCreated, nil
}

// EnqueueJSON marshals v as the job's payload and enqueues it, for the
// common case of a JSON document. Job.Decode is the mirror image on the
// worker side.
func (c *Client) EnqueueJSON(ctx context.Context, queue string, v any, opts ...EnqueueOption) (*Job, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("client: encode payload for queue %q: %w", queue, err)
	}
	return c.Enqueue(ctx, queue, payload, opts...)
}

// Get returns the job with the given ID, or an error wrapping ErrNotFound
// if the server has no such job.
func (c *Client) Get(ctx context.Context, id string) (*Job, error) {
	if id == "" {
		return nil, errors.New("client: job ID is required")
	}

	var dto jobDTO
	if err := c.send(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &dto); err != nil {
		return nil, err
	}
	return dto.job(), nil
}

// List returns up to limit jobs in the given status, most recently updated
// first. A limit of 0 leaves the page size to the server's default, and one
// above its maximum is capped there rather than rejected.
func (c *Client) List(ctx context.Context, status Status, limit int) ([]*Job, error) {
	if status == "" {
		return nil, errors.New("client: status is required")
	}
	if limit < 0 {
		return nil, fmt.Errorf("client: limit %d must be >= 0", limit)
	}

	query := url.Values{"status": {string(status)}}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}

	var body struct {
		Jobs []jobDTO `json:"jobs"`
	}
	if err := c.send(ctx, http.MethodGet, "/v1/jobs?"+query.Encode(), nil, &body); err != nil {
		return nil, err
	}

	jobs := make([]*Job, len(body.Jobs))
	for i, dto := range body.Jobs {
		jobs[i] = dto.job()
	}
	return jobs, nil
}

// Cancel stops a job that has not started yet. It returns an error
// wrapping ErrNotFound if the job is unknown, or ErrConflict if it is
// already running or already finished -- cancellation is a race the caller
// can lose, and losing it is not a fault.
func (c *Client) Cancel(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("client: job ID is required")
	}
	return c.send(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/cancel", nil, nil)
}

// Retry returns a dead or failed job to the queue with a cleared retry
// count and error, so it is attempted again from scratch. It returns an
// error wrapping ErrNotFound if the job is unknown, or ErrConflict if it
// is in any other state.
func (c *Client) Retry(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("client: job ID is required")
	}
	return c.send(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/retry", nil, nil)
}
