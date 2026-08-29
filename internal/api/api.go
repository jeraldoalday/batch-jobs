package api

import (
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/metrics"
)

// DefaultListLimit is the number of jobs returned by GET /v1/jobs when the
// caller doesn't specify a limit.
const DefaultListLimit = 50

// DefaultListMaxLimit is the largest limit GET /v1/jobs accepts, regardless
// of what the caller asks for.
const DefaultListMaxLimit = 500

// DefaultRetryAfter is the Retry-After hint sent with a 503 when the
// backing queue rejects an enqueue for being full.
const DefaultRetryAfter = time.Second

// Server exposes a Store over HTTP. A zero-value Server is not usable;
// Store is required. All other fields are optional and have documented
// defaults, applied by Handler.
type Server struct {
	// Store is where jobs are enqueued, looked up, listed, and cancelled.
	// Required.
	Store Store

	// Logger is used for request and error logging. Defaults to
	// slog.Default() if unset.
	Logger *slog.Logger

	// Metrics, if set, is mounted at GET /metrics in Prometheus text
	// format. A nil Metrics (the default) leaves /metrics unmounted.
	Metrics *metrics.Metrics

	// ScheduleStore, if set, backs the /v1/schedules routes. Leaving it
	// nil (the default) still mounts them, answering 501: a caller who
	// asks a server without scheduling for its schedules deserves to be
	// told that rather than given a 404 they cannot tell from a typo.
	ScheduleStore ScheduleStore

	// AuthToken, if set, is required as the X-API-Key header on every
	// request except /healthz. Leaving it empty (the default) disables
	// auth, which is appropriate for local development only.
	AuthToken string

	// RateLimit, if non-zero, caps requests per client IP using a token
	// bucket. The zero value disables rate limiting.
	RateLimit RateLimit

	// ListDefaultLimit overrides DefaultListLimit.
	ListDefaultLimit int

	// ListMaxLimit overrides DefaultListMaxLimit.
	ListMaxLimit int

	// EventInterval overrides DefaultEventInterval, the push cadence of
	// GET /v1/events.
	EventInterval time.Duration

	// RetryAfter overrides DefaultRetryAfter, the Retry-After hint sent
	// to a client whose enqueue was rejected because the queue is full.
	RetryAfter time.Duration

	// Assets, if set, is served as static files at "/" (e.g. dashboard.FS),
	// falling in behind the API's own routes. A nil Assets (the default)
	// leaves "/" unmounted, returning 404.
	Assets fs.FS
}

// Handler builds the routed, middleware-wrapped http.Handler for the
// server. It is safe to call once and reuse the result; Server's fields
// should not be mutated afterward.
func (s *Server) Handler() http.Handler {
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	if s.ListDefaultLimit <= 0 {
		s.ListDefaultLimit = DefaultListLimit
	}
	if s.ListMaxLimit <= 0 {
		s.ListMaxLimit = DefaultListMaxLimit
	}
	if s.RetryAfter <= 0 {
		s.RetryAfter = DefaultRetryAfter
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", s.handleEnqueue)
	mux.HandleFunc("GET /v1/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("GET /v1/jobs", s.handleListJobs)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.handleCancelJob)
	mux.HandleFunc("POST /v1/jobs/{id}/retry", s.handleRetryJob)
	mux.HandleFunc("POST /v1/schedules", s.handleCreateSchedule)
	mux.HandleFunc("GET /v1/schedules", s.handleListSchedules)
	mux.HandleFunc("GET /v1/schedules/{id}", s.handleGetSchedule)
	mux.HandleFunc("PATCH /v1/schedules/{id}", s.handleUpdateSchedule)
	mux.HandleFunc("DELETE /v1/schedules/{id}", s.handleDeleteSchedule)
	mux.HandleFunc("POST /v1/schedules/{id}/pause", s.handlePauseSchedule)
	mux.HandleFunc("POST /v1/schedules/{id}/resume", s.handleResumeSchedule)
	mux.HandleFunc("POST /v1/schedules/{id}/trigger", s.handleTriggerSchedule)
	mux.HandleFunc("POST /v1/recover", s.handleRecover)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	if s.Metrics != nil {
		if h := s.Metrics.Handler(); h != nil {
			mux.Handle("GET /metrics", h)
		}
	}
	if s.Assets != nil {
		mux.Handle("/", http.FileServerFS(s.Assets))
	}

	var h http.Handler = mux
	h = s.withAuth(h)
	h = s.withRateLimit(h)
	h = s.withLogging(h)
	return h
}
