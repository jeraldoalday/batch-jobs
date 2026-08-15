package api

import "net/http"

// handleStats handles GET /v1/stats, returning the number of jobs currently
// in each status.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.Store.Stats()
	if err != nil {
		s.Logger.Error("api: stats failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to fetch stats")
		return
	}

	counts := make(map[string]int, len(stats))
	for status, n := range stats {
		counts[string(status)] = n
	}
	writeJSON(w, http.StatusOK, StatsResponse{Counts: counts})
}

// handleHealthz handles GET /healthz. It always returns 200: liveness is
// "the process is up and serving", not a dependency check.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
