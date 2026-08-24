package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// maxEventLine bounds a single Server-Sent Events line. Stats frames are a
// few hundred bytes; the limit exists so a misbehaving or hostile server
// cannot make the client buffer without end.
const maxEventLine = 64 << 10

// EventStream is a live feed of stats snapshots from GET /v1/events, the
// same stream the dashboard's tiles are driven by. It is not safe for
// concurrent use: one goroutine should own the stream and call Next in a
// loop.
//
// A stream must be closed when the caller is done with it, or the
// connection to the server stays open:
//
//	stream, err := c.Events(ctx)
//	if err != nil {
//	    return err
//	}
//	defer stream.Close()
//
//	for {
//	    stats, err := stream.Next()
//	    if err != nil {
//	        return err
//	    }
//	    log.Println(stats)
//	}
type EventStream struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
	cancel  context.CancelFunc
	closed  bool
}

// Events opens the server's stats stream. The server pushes a snapshot
// immediately and then on its own interval, so the first Next returns
// promptly rather than after one tick.
//
// The stream lives until the caller closes it or ctx is cancelled, which is
// the reason WithRequestTimeout must not be combined with it: an
// *http.Client timeout applies to the whole exchange, so it would sever a
// healthy stream at a fixed age. Bound a stream with ctx instead.
func (c *Client) Events(ctx context.Context) (*EventStream, error) {
	// A stream outlives the call that opened it, so it gets a context of
	// its own derived from ctx: Close cancels it, which unblocks a Next
	// parked on a server that has gone quiet without closing the
	// connection.
	streamCtx, cancel := context.WithCancel(ctx)

	req, err := c.newRequest(streamCtx, http.MethodGet, "/v1/events", nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := c.do(req)
	if err != nil {
		cancel()
		return nil, err
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 4096), maxEventLine)

	return &EventStream{body: resp.Body, scanner: scanner, cancel: cancel}, nil
}

// Next blocks until the server pushes the next stats snapshot, returning
// io.EOF once the stream ends cleanly -- the server shut down, or Close was
// called -- and the underlying error otherwise. A cancelled context surfaces
// as that context's error.
func (s *EventStream) Next() (Stats, error) {
	if s.closed {
		return nil, io.EOF
	}

	for s.scanner.Scan() {
		line := s.scanner.Text()

		// Blank lines terminate a frame and comment lines (":" prefix)
		// are keep-alives; neither carries data.
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			// Any other field -- event:, id:, retry: -- is part of the
			// protocol this endpoint does not use. Skipping unknown
			// fields is what the SSE spec calls for, and it keeps this
			// client working if the server starts naming its events.
			continue
		}

		var body struct {
			Counts map[string]int `json:"counts"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(data)), &body); err != nil {
			return nil, fmt.Errorf("client: decode stats event: %w", err)
		}

		stats := make(Stats, len(body.Counts))
		for status, n := range body.Counts {
			stats[Status(status)] = n
		}
		return stats, nil
	}

	if err := s.scanner.Err(); err != nil {
		if s.closed || errors.Is(err, context.Canceled) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("client: read stats event: %w", err)
	}
	return nil, io.EOF
}

// Close ends the stream and releases the connection. It is safe to call
// more than once, and safe to call from another goroutine to unblock a Next
// that is waiting on a server with nothing to say.
func (s *EventStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.cancel()
	return s.body.Close()
}

// Watch follows the stats stream, calling fn with each snapshot until fn
// returns an error, the server ends the stream, or ctx is cancelled. It is
// the callback form of Events for callers that just want a loop, and it
// closes the stream on every path out.
//
// A clean end of stream is not an error: Watch returns nil for it, and
// ctx.Err() when the context ended it.
func (c *Client) Watch(ctx context.Context, fn func(Stats) error) error {
	stream, err := c.Events(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	for {
		stats, err := stream.Next()
		if errors.Is(err, io.EOF) {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		if err := fn(stats); err != nil {
			return err
		}
	}
}
