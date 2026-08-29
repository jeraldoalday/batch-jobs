// Package batchjobs is an embeddable job queue and worker pool for Go.
//
// An Engine owns a durable SQLite-backed queue, a pool of workers, and the
// background recovery sweep that reclaims jobs whose worker died holding
// them. Producing and consuming happen in the same process, so a service
// can enqueue work and process it without running a broker:
//
//	engine, err := batchjobs.Open(batchjobs.Config{DB: "jobs.db", Workers: 8})
//	if err != nil {
//	    return err
//	}
//	defer engine.Close()
//
//	engine.Handle("emails", func(ctx context.Context, j *batchjobs.Job) error {
//	    var msg Message
//	    if err := j.Decode(&msg); err != nil {
//	        // A payload that will not parse will not parse on the next
//	        // attempt either, so do not spend the job's retries on it.
//	        return batchjobs.Permanent(err)
//	    }
//	    return send(ctx, msg)
//	})
//
//	if err := engine.Start(ctx); err != nil {
//	    return err
//	}
//	defer engine.Stop()
//
//	_, err = engine.EnqueueJSON(ctx, "emails", Message{To: "a@example.com"},
//	    batchjobs.WithMaxRetries(3),
//	    batchjobs.WithTimeout(30*time.Second),
//	)
//
// Handlers are registered per queue, and a job whose queue has no handler
// fails permanently rather than being retried forever. Jobs survive
// restarts: the queue is a database file, and an Engine started against an
// existing one picks up where the last process left off.
//
// To expose the same queue over HTTP -- for producers in other processes,
// or the dashboard -- mount Engine.APIHandler. To talk to such a server
// from elsewhere, use the sibling client package.
package batchjobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/api"
	"github.com/jeraldoalday/batch-jobs/internal/dashboard"
	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
	schedulesqlite "github.com/jeraldoalday/batch-jobs/internal/schedule/sqlite"
	"github.com/jeraldoalday/batch-jobs/internal/scheduler"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitex"
	"github.com/jeraldoalday/batch-jobs/internal/worker"
)

// DefaultWorkers is the pool size an Engine runs when Config.Workers is
// unset.
const DefaultWorkers = 4

// MemoryDB is the database path for an ephemeral, process-local queue that
// is discarded when the Engine closes. It is meant for tests and for
// workloads that genuinely do not need jobs to survive a restart.
const MemoryDB = ":memory:"

// Handler processes a single job. Returning nil marks the job succeeded;
// returning an error retries it, subject to its MaxRetries, and
// dead-letters it once those are spent. Wrapping the error with Permanent
// skips both, failing the job immediately.
//
// The context is cancelled when the job's Timeout elapses, when the Engine
// stops, or when the worker loses its lease on the job -- which means
// another worker may already be running it, so a handler that ignores
// cancellation risks doing the work twice.
type Handler func(ctx context.Context, j *Job) error

// Config describes an Engine. Every field has a documented default except
// DB, which is required.
type Config struct {
	// DB is the SQLite database file backing the queue. It is created if
	// it does not exist, and its schema is migrated on Open. Use MemoryDB
	// for an ephemeral queue. Required.
	DB string

	// Workers is the number of jobs the Engine runs concurrently,
	// defaulting to DefaultWorkers. With MaxWorkers set it is the pool's
	// starting size rather than its fixed one.
	Workers int

	// MinWorkers and MaxWorkers turn on autoscaling: the pool tracks its
	// own backlog and resizes within these bounds. Leaving MaxWorkers at
	// zero keeps the pool fixed at Workers.
	MinWorkers int
	MaxWorkers int

	// RetryBase and RetryMax bound the exponential backoff between
	// attempts, defaulting to one second and one minute. RetryJitter, in
	// [0, 1], randomizes each delay by that fraction so jobs that failed
	// together do not retry in lockstep and re-overwhelm whatever they
	// were waiting on; it defaults to 0.1.
	RetryBase   time.Duration
	RetryMax    time.Duration
	RetryJitter float64

	// LeaseTTL is how long a claimed job stays claimed without a
	// heartbeat before another worker may take it over, defaulting to the
	// queue's own default. A negative value disables leasing, so a
	// crashed worker's jobs stay claimed until an operator intervenes.
	LeaseTTL time.Duration

	// ReapInterval is how often the Engine sweeps for jobs whose worker
	// died holding them, defaulting to the queue's own default. A
	// negative value disables the sweep; zero uses the default.
	ReapInterval time.Duration

	// Schedules turns on recurring, cron-driven jobs: the Engine opens a
	// second database for schedule definitions and, once started, runs
	// the loop that enqueues each schedule's job as it comes due.
	//
	// It is off by default because it is not free -- it creates a file
	// and polls it -- and a queue that only ever has jobs pushed into it
	// should not pay for either. Setting SchedulesDB implies it.
	Schedules bool

	// SchedulesDB is where schedule definitions live, defaulting to a
	// file beside DB with ".schedules" inserted before the extension:
	// "jobs.db" gives "jobs.schedules.db".
	//
	// It is a separate database rather than more tables in DB because
	// the two have separately versioned migration sets, which one
	// schema_migrations table cannot track. Use MemoryDB for schedules
	// that need not survive a restart -- which, for a definition an
	// operator typed in, is rarely what is wanted.
	SchedulesDB string

	// SchedulePoll is how often the scheduler checks for schedules that
	// have come due, defaulting to one second. It bounds how late a
	// firing can be, so it is the knob to turn up on a large schedule
	// set where being a few seconds late costs nothing.
	SchedulePoll time.Duration

	// Logger receives the Engine's operational logging, defaulting to
	// slog.Default().
	Logger *slog.Logger

	// Registry is where the Engine registers its Prometheus collectors,
	// so its metrics can sit alongside an application's own. It defaults
	// to a private registry, which Engine.MetricsHandler serves.
	Registry *prometheus.Registry
}

// Engine is an embedded job queue: a durable queue, a pool of workers
// running registered handlers, and the recovery sweep that reclaims
// orphaned jobs. It is safe for concurrent use.
//
// The lifecycle is Open, then Handle for each queue, then Start, then Stop
// and Close. Handlers may also be registered while the Engine is running,
// for a program that learns what it can process as it goes.
type Engine struct {
	cfg         Config
	queue       *sqlite.Queue
	mux         *worker.Mux
	pool        *worker.Pool
	reaper      *sqlite.Reaper
	schedules   *schedulesqlite.Store
	scheduleMgr *schedule.Manager
	scheduler   *scheduler.Scheduler
	metrics     *metrics.Metrics
	logger      *slog.Logger

	mu      sync.Mutex
	started bool
	stopped bool
	closed  bool
	cancel  context.CancelFunc
}

// Open creates an Engine against cfg.DB, applying any pending schema
// migrations. The returned Engine is not yet processing jobs -- it can
// already enqueue and inspect them, which is all a producer-only process
// needs -- until Start is called.
//
// The caller must Close the Engine to release the database.
func Open(cfg Config) (*Engine, error) {
	if cfg.DB == "" {
		return nil, errors.New("batchjobs: Config.DB is required")
	}
	if cfg.Workers < 0 {
		return nil, fmt.Errorf("batchjobs: Workers %d must be >= 0", cfg.Workers)
	}
	if cfg.MaxWorkers > 0 && cfg.MinWorkers > cfg.MaxWorkers {
		return nil, fmt.Errorf("batchjobs: MinWorkers %d exceeds MaxWorkers %d", cfg.MinWorkers, cfg.MaxWorkers)
	}
	if cfg.RetryJitter < 0 || cfg.RetryJitter > 1 {
		return nil, fmt.Errorf("batchjobs: RetryJitter %v must be in [0, 1]", cfg.RetryJitter)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	registry := cfg.Registry
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	q, err := sqlite.Open(cfg.DB, sqlite.Options{LeaseTTL: cfg.LeaseTTL})
	if err != nil {
		return nil, fmt.Errorf("batchjobs: open %s: %w", cfg.DB, err)
	}

	m := metrics.New(registry)
	if err := m.RegisterQueueDepth("default", q.Len); err != nil {
		// A duplicate registration means the caller's registry already
		// has these collectors, which is a configuration mistake worth
		// reporting rather than silently running without depth metrics.
		q.Close()
		return nil, fmt.Errorf("batchjobs: register queue depth gauge: %w", err)
	}

	e := &Engine{
		cfg:     cfg,
		queue:   q,
		mux:     worker.NewMux(),
		metrics: m,
		logger:  logger,
	}

	if cfg.schedulingEnabled() {
		path := cfg.schedulesPath()
		store, err := schedulesqlite.Open(path)
		if err != nil {
			q.Close()
			return nil, fmt.Errorf("batchjobs: open schedules %s: %w", path, err)
		}
		e.schedules = store
		e.scheduleMgr = &schedule.Manager{Store: store, Queue: q}
	}

	e.pool = &worker.Pool{
		Queue:       q,
		Handler:     e.mux.Serve,
		Concurrency: e.workers(),
		Scale:       worker.Policy{Min: cfg.MinWorkers, Max: cfg.MaxWorkers},
		Backoff:     e.backoff(),
		Logger:      logger,
		Metrics:     m,
	}
	return e, nil
}

// workers resolves the pool's starting size from the config.
func (e *Engine) workers() int {
	if e.cfg.Workers > 0 {
		return e.cfg.Workers
	}
	return DefaultWorkers
}

// schedulingEnabled reports whether the Engine should carry a schedule
// store. Naming a database is taken as asking for the feature, so a caller
// who overrides the path does not also have to set the flag.
func (c Config) schedulingEnabled() bool { return c.Schedules || c.SchedulesDB != "" }

// schedulesPath resolves where schedule definitions live, deriving a
// sibling of the jobs database when the caller did not say.
func (c Config) schedulesPath() string {
	if c.SchedulesDB != "" {
		return c.SchedulesDB
	}
	return sqlitex.SiblingPath(c.DB, ".schedules")
}

// schedulePoll resolves the scheduler's poll cadence from the config.
func (e *Engine) schedulePoll() time.Duration {
	if e.cfg.SchedulePoll > 0 {
		return e.cfg.SchedulePoll
	}
	return scheduler.DefaultPollInterval
}

// backoff builds the retry schedule from the config, jittered so a batch of
// jobs that failed at the same instant does not retry in lockstep.
func (e *Engine) backoff() worker.BackoffFunc {
	base, max := e.cfg.RetryBase, e.cfg.RetryMax
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = time.Minute
	}
	if max < base {
		max = base
	}

	jitter := e.cfg.RetryJitter
	if jitter == 0 {
		jitter = 0.1
	}
	return worker.WithJitter(worker.ExponentialBackoff(base, max), jitter)
}

// Handle registers h as the handler for jobs on the named queue. It panics
// if the name is empty, h is nil, or the queue already has a handler --
// each of which is a wiring mistake that should surface at startup rather
// than as jobs quietly running the wrong code.
//
// Handlers may be registered before or after Start.
func (e *Engine) Handle(name string, h Handler) {
	if h == nil {
		panic("batchjobs: Handle called with a nil handler for queue " + name)
	}
	e.mux.Handle(name, func(ctx context.Context, j *job.Job) error {
		return h(ctx, convert(j))
	})
}

// HandleFallback registers h for jobs whose queue has no handler of its
// own, replacing any previous fallback; nil clears it. Without one, such a
// job fails permanently rather than being retried, since no amount of
// retrying makes a handler appear.
func (e *Engine) HandleFallback(h Handler) {
	if h == nil {
		e.mux.Fallback(nil)
		return
	}
	e.mux.Fallback(func(ctx context.Context, j *job.Job) error {
		return h(ctx, convert(j))
	})
}

// Queues returns the queue names that have a handler, sorted.
func (e *Engine) Queues() []string { return e.mux.Queues() }

// Start begins processing jobs and launches the recovery sweep. It returns
// immediately; the Engine runs until Stop is called or ctx is cancelled.
//
// Starting an Engine with no handlers registered is allowed but almost
// certainly a mistake, so it is logged: every job the pool claims will fail
// permanently for want of a handler.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errors.New("batchjobs: Start on a closed Engine")
	}
	if e.started {
		return errors.New("batchjobs: Engine already started")
	}
	if len(e.mux.Queues()) == 0 {
		e.logger.Warn("batchjobs: starting with no handlers registered; every job will fail permanently")
	}

	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.started = true

	e.pool.Start(ctx)

	// Recovery only means anything for a queue that leases claims; with
	// leasing off, a job that is running simply stays running.
	if interval := e.reapInterval(); interval > 0 && e.queue.LeaseTTL() > 0 {
		e.reaper = &sqlite.Reaper{
			Queue:    e.queue,
			Interval: interval,
			Logger:   e.logger,
			Metrics:  e.metrics,
		}
		// The first sweep happens inside Start, and it is the most
		// valuable one this process will run: if it is replacing a
		// process that crashed, that process's jobs are orphaned now.
		e.reaper.Start(ctx)
	}

	if e.schedules != nil {
		e.scheduler = &scheduler.Scheduler{
			Store:        e.schedules,
			Queue:        e.queue,
			PollInterval: e.schedulePoll(),
			Logger:       e.logger,
			Metrics:      e.metrics,
		}
		e.scheduler.Start(ctx)
	}
	return nil
}

// reapInterval resolves the sweep cadence, where a negative value disables
// the sweep and zero means the queue's own default.
func (e *Engine) reapInterval() time.Duration {
	switch {
	case e.cfg.ReapInterval < 0:
		return 0
	case e.cfg.ReapInterval == 0:
		return sqlite.DefaultReapInterval
	default:
		return e.cfg.ReapInterval
	}
}

// Stop stops accepting new work and blocks until every in-flight handler
// has returned, so a job is never abandoned half-done at shutdown. It is
// safe to call more than once, and safe to call on an Engine that was never
// started.
//
// Stop does not close the database: an Engine can still be used to enqueue
// and inspect jobs afterwards. Call Close to release it.
func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.started || e.stopped {
		e.mu.Unlock()
		return
	}
	e.stopped = true
	reaper, sched, cancel := e.reaper, e.scheduler, e.cancel
	e.mu.Unlock()

	// The scheduler stops first so no new firing lands in the queue
	// while the pool is draining the ones already there.
	if sched != nil {
		sched.Stop()
	}
	if reaper != nil {
		reaper.Stop()
	}
	e.pool.Stop()
	if cancel != nil {
		cancel()
	}
}

// Close stops the Engine if it is running and releases the database. It is
// safe to call more than once.
func (e *Engine) Close() error {
	e.Stop()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true

	// Both stores are closed even if the first fails, so a database is
	// never left held open by an error closing the other one.
	var errs []error
	if err := e.queue.Close(); err != nil {
		errs = append(errs, fmt.Errorf("batchjobs: close %s: %w", e.cfg.DB, err))
	}
	if e.schedules != nil {
		if err := e.schedules.Close(); err != nil {
			errs = append(errs, fmt.Errorf("batchjobs: close schedules %s: %w", e.cfg.schedulesPath(), err))
		}
	}
	return errors.Join(errs...)
}

// MetricsHandler serves the Engine's Prometheus collectors in the text
// exposure format, for mounting at a scrape path such as "/metrics".
//
// An Engine given its own Config.Registry does not need this: those
// collectors are already on the caller's registry, alongside whatever else
// it exposes.
func (e *Engine) MetricsHandler() http.Handler { return e.metrics.Handler() }

// APIHandler returns the REST API for this Engine's queue -- the same
// routes cmd/batch-jobs-server serves -- so an application can expose its
// queue to producers in other processes, or to the dashboard, on a listener
// it already has:
//
//	mux.Handle("/jobs/", http.StripPrefix("/jobs", engine.APIHandler(batchjobs.APIOptions{
//	    AuthToken: os.Getenv("JOBS_API_KEY"),
//	    Dashboard: true,
//	})))
//
// The sibling client package talks to it.
func (e *Engine) APIHandler(opts ...APIOptions) http.Handler {
	var o APIOptions
	if len(opts) > 0 {
		o = opts[0]
	}

	srv := &api.Server{
		Store:     e.queue,
		Logger:    e.logger,
		Metrics:   e.metrics,
		AuthToken: o.AuthToken,
		RateLimit: api.RateLimit{RPS: o.RateLimitRPS, Burst: o.RateLimitBurst},
	}
	// Left nil on an Engine without scheduling, which is what makes the
	// schedule routes answer 501 rather than pretending to work.
	if e.scheduleMgr != nil {
		srv.ScheduleStore = e.scheduleMgr
	}
	if o.Dashboard {
		srv.Assets = dashboard.FS
	}
	return srv.Handler()
}

// APIOptions configures the handler APIHandler returns. The zero value
// serves the API with no authentication and no dashboard, which is
// appropriate for a listener that is not reachable from outside the host.
type APIOptions struct {
	// AuthToken, if set, is required as the X-API-Key header on every
	// request except /healthz.
	AuthToken string

	// RateLimitRPS and RateLimitBurst cap requests per client IP. Leaving
	// RPS at zero disables rate limiting.
	RateLimitRPS   float64
	RateLimitBurst int

	// Dashboard serves the web UI at the handler's root, alongside the
	// API's own routes.
	Dashboard bool
}
