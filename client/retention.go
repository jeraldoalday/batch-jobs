package client

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// Pruned counts what one retention pass deleted, per terminal status.
type Pruned struct {
	// Succeeded is how many completed jobs were deleted.
	Succeeded int

	// Failed is how many permanently failed jobs were deleted.
	Failed int

	// Dead is how many dead-lettered jobs were deleted.
	Dead int

	// Cancelled is how many cancelled jobs were deleted.
	Cancelled int

	// Retained is how many finished jobs the server still holds after
	// the pass. It is what distinguishes a history now within its policy
	// from one that is merely smaller than it was: a pass that deleted a
	// great deal and left a great deal behind is a queue producing
	// history faster than one pass can clear it.
	Retained int
}

// Total is how many job records the pass removed altogether.
func (p Pruned) Total() int { return p.Succeeded + p.Failed + p.Dead + p.Cancelled }

// Get returns the count for one status, or zero for a status that never
// finished and so was never a candidate.
func (p Pruned) Get(status Status) int {
	switch status {
	case StatusSucceeded:
		return p.Succeeded
	case StatusFailed:
		return p.Failed
	case StatusDead:
		return p.Dead
	case StatusCancelled:
		return p.Cancelled
	default:
		return 0
	}
}

// String renders the non-zero counts, so a pass can be summarized in a log
// line without the caller formatting a struct.
func (p Pruned) String() string {
	if p.Total() == 0 {
		return "nothing pruned"
	}

	parts := make([]string, 0, 4)
	for _, status := range []Status{StatusSucceeded, StatusCancelled, StatusFailed, StatusDead} {
		if n := p.Get(status); n > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", status, n))
		}
	}
	return strings.Join(parts, " ")
}

// Prune asks the server to delete finished jobs its retention policy has
// expired, rather than waiting for its next scheduled pass.
//
// The policy is the server's, and deliberately not something this call can
// name: a client able to supply its own retention could delete any
// finished job in the queue by asking for a retention of zero. What Prune
// controls is the timing -- a maintenance window, a migration about to
// copy the table, an operator who has just watched the disk fill.
//
// A server with no retention policy configured, or one whose store does
// not keep finished jobs at all, returns an error wrapping ErrUnsupported.
// The server's message says which.
func (c *Client) Prune(ctx context.Context) (Pruned, error) {
	var body struct {
		Succeeded int `json:"succeeded"`
		Failed    int `json:"failed"`
		Dead      int `json:"dead"`
		Cancelled int `json:"cancelled"`
		Total     int `json:"total"`
		Retained  int `json:"retained"`
	}
	if err := c.send(ctx, http.MethodPost, "/v1/prune", nil, &body); err != nil {
		return Pruned{}, err
	}
	return Pruned{
		Succeeded: body.Succeeded,
		Failed:    body.Failed,
		Dead:      body.Dead,
		Cancelled: body.Cancelled,
		Retained:  body.Retained,
	}, nil
}
