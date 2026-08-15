package api

import (
	"net/http"
	"time"
)

// withAuth requires a matching X-API-Key header on every request except
// /healthz (so load balancers and orchestrators can probe liveness without
// a credential). Auth is skipped entirely when s.AuthToken is empty, which
// is the default — a stand-in for a real credential store, not a substitute
// for one in production.
func (s *Server) withAuth(next http.Handler) http.Handler {
	if s.AuthToken == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("X-API-Key") != s.AuthToken {
			writeError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// statusRecorder wraps an http.ResponseWriter to capture the status code
// passed to WriteHeader, so logging middleware can report it after the
// handler returns.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// withLogging logs each request's method, path, status, and duration at
// debug level once the handler has returned.
func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		s.Logger.Debug("api: request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration", time.Since(start),
		)
	})
}
