package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// DefaultEventInterval is how often GET /v1/events pushes a fresh stats
// snapshot when Server.EventInterval is unset.
const DefaultEventInterval = 2 * time.Second

// handleEvents handles GET /v1/events, a Server-Sent Events stream that
// pushes a StatsResponse snapshot every EventInterval until the client
// disconnects or the request is cancelled. It's what lets the dashboard's
// stats cards update live without polling.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	interval := s.EventInterval
	if interval <= 0 {
		interval = DefaultEventInterval
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if !s.writeStatsEvent(w) {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if !s.writeStatsEvent(w) {
				return
			}
			flusher.Flush()
		}
	}
}

// writeStatsEvent writes one "data: <json>\n\n" SSE frame carrying the
// current stats snapshot. It reports whether the frame was written
// successfully, so the caller can stop the stream (rather than keep ticking
// against a store that's failing, or a connection that's gone) without
// double-logging the same failure on every subsequent tick.
func (s *Server) writeStatsEvent(w http.ResponseWriter) bool {
	stats, err := s.Store.Stats()
	if err != nil {
		s.Logger.Error("api: events stats failed", "error", err)
		return false
	}

	counts := make(map[string]int, len(stats))
	for status, n := range stats {
		counts[string(status)] = n
	}

	b, err := json.Marshal(StatsResponse{Counts: counts})
	if err != nil {
		s.Logger.Error("api: events marshal failed", "error", err)
		return false
	}

	_, err = fmt.Fprintf(w, "data: %s\n\n", b)
	return err == nil
}
