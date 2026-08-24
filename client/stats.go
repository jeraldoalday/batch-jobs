package client

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// Stats is the number of jobs in each status. Statuses with no jobs are
// absent rather than zero, since the server omits them; use Get, which
// reads a missing status as zero, instead of indexing directly.
type Stats map[Status]int

// Get returns the number of jobs in the given status, or zero.
func (s Stats) Get(status Status) int { return s[status] }

// Total is the number of jobs the server knows about, across every status.
func (s Stats) Total() int {
	total := 0
	for _, n := range s {
		total += n
	}
	return total
}

// Active is the number of jobs still on their way to a terminal state:
// waiting, running, or scheduled for another attempt. It is the number that
// answers "is there anything left to do?", which Total does not -- a queue
// that has finished a million jobs has a large Total and nothing to do.
func (s Stats) Active() int {
	return s[StatusPending] + s[StatusRunning] + s[StatusRetrying]
}

// Backlog is the number of jobs waiting for a worker, excluding those
// already running. It is the number to scale a worker pool on.
func (s Stats) Backlog() int {
	return s[StatusPending] + s[StatusRetrying]
}

// Failing is the number of jobs that ended badly -- permanently failed or
// dead-lettered -- and are sitting there until someone retries or discards
// them. It is the natural thing to alert on.
func (s Stats) Failing() int {
	return s[StatusFailed] + s[StatusDead]
}

// String renders the non-empty statuses in a stable order, so the value is
// usable in a log line without the caller sorting a map first.
func (s Stats) String() string {
	if len(s) == 0 {
		return "no jobs"
	}

	parts := make([]string, 0, len(s))
	for _, status := range Statuses {
		if n, ok := s[status]; ok {
			parts = append(parts, status.String()+"="+strconv.Itoa(n))
		}
	}
	// Anything the server reported that this client does not know about
	// still belongs in the output; a newer server's new status should not
	// vanish from an operator's log line.
	unknown := make([]string, 0)
	for status, n := range s {
		if !status.Valid() {
			unknown = append(unknown, status.String()+"="+strconv.Itoa(n))
		}
	}
	sort.Strings(unknown)

	return strings.Join(append(parts, unknown...), " ")
}

// Stats returns the current per-status job counts.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	var body struct {
		Counts map[string]int `json:"counts"`
	}
	if err := c.send(ctx, http.MethodGet, "/v1/stats", nil, &body); err != nil {
		return nil, err
	}

	stats := make(Stats, len(body.Counts))
	for status, n := range body.Counts {
		stats[Status(status)] = n
	}
	return stats, nil
}

// Recovery summarizes one pass of reclaiming jobs whose worker died holding
// them.
type Recovery struct {
	// Requeued is how many orphaned jobs went back in the queue for
	// another attempt.
	Requeued int

	// DeadLettered is how many orphaned jobs had no attempts left and were
	// retired instead.
	DeadLettered int

	// Remaining is how many expired claims were still outstanding when the
	// pass finished. It is normally zero; a non-zero value means orphans
	// are appearing faster than one pass can clear them.
	Remaining int
}

// Total is how many orphaned jobs the pass dealt with, either way.
func (r Recovery) Total() int { return r.Requeued + r.DeadLettered }

// Recover asks the server to reclaim every job whose lease has expired,
// rather than waiting for its reaper's next sweep. It is for the moment an
// operator has confirmed a host is gone and wants its work running again
// now, or for a deploy script reclaiming a retired instance's jobs.
//
// A server whose store does not lease jobs has nothing to recover and
// returns an error wrapping ErrUnsupported.
func (c *Client) Recover(ctx context.Context) (Recovery, error) {
	var body struct {
		Requeued     int `json:"requeued"`
		DeadLettered int `json:"dead_lettered"`
		Remaining    int `json:"remaining"`
	}
	if err := c.send(ctx, http.MethodPost, "/v1/recover", nil, &body); err != nil {
		return Recovery{}, err
	}
	return Recovery{
		Requeued:     body.Requeued,
		DeadLettered: body.DeadLettered,
		Remaining:    body.Remaining,
	}, nil
}

// Health reports whether the server is up and serving. It is a liveness
// check, not a readiness one: a server whose database has gone away still
// answers it, so use Stats to confirm the store itself is reachable.
//
// It is the one endpoint an API key is never required for, so it also works
// as a reachability probe before credentials are configured.
func (c *Client) Health(ctx context.Context) error {
	return c.send(ctx, http.MethodGet, "/healthz", nil, nil)
}
