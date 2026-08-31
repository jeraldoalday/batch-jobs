// Command batch-jobs-server runs the batch-jobs HTTP API against a
// SQLite-backed queue, standalone -- useful for local development,
// exercising the API by hand, or as a starting point for embedding the
// server into a larger binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeraldoalday/batch-jobs/internal/api"
	"github.com/jeraldoalday/batch-jobs/internal/dashboard"
	"github.com/jeraldoalday/batch-jobs/internal/metrics"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
	"github.com/jeraldoalday/batch-jobs/internal/schedule"
	schedulesqlite "github.com/jeraldoalday/batch-jobs/internal/schedule/sqlite"
	"github.com/jeraldoalday/batch-jobs/internal/scheduler"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitemigrate"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitex"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dbPath := flag.String("db", "batch-jobs.db", `SQLite database path (":memory:" for ephemeral)`)
	authToken := flag.String("auth-token", "", "if set, required as the X-API-Key header on every request except /healthz")
	rateRPS := flag.Float64("rate-rps", 0, "requests per second allowed per client IP (0 disables rate limiting)")
	rateBurst := flag.Int("rate-burst", 20, "burst size for per-client rate limiting")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, or error")
	enableDashboard := flag.Bool("dashboard", true, "serve the web dashboard at /")
	leaseTTL := flag.Duration("lease-ttl", sqlite.DefaultLeaseTTL, "how long a claimed job stays claimed without a heartbeat before it can be recovered (negative disables leasing)")
	reapInterval := flag.Duration("reap-interval", sqlite.DefaultReapInterval, "how often to reclaim jobs whose worker died holding them (0 disables the reaper)")
	retainSucceeded := flag.Duration("retain-succeeded", 0, "how long to keep jobs that ran cleanly (0 keeps them forever)")
	retainFailed := flag.Duration("retain-failed", 0, "how long to keep permanently failed jobs (0 keeps them forever)")
	retainDead := flag.Duration("retain-dead", 0, "how long to keep dead-lettered jobs (0 keeps them forever, which is the recommendation: they are work still to be done)")
	retainCancelled := flag.Duration("retain-cancelled", 0, "how long to keep cancelled jobs (0 keeps them forever)")
	pruneInterval := flag.Duration("prune-interval", sqlite.DefaultPruneInterval, "how often to delete finished jobs the retention flags have expired (negative leaves pruning to POST /v1/prune)")
	pruneBatch := flag.Int("prune-batch", 0, "how many rows one prune statement deletes, bounding how long it holds the write lock (0 uses the default)")
	enableSchedules := flag.Bool("schedules", false, "serve the /v1/schedules API and run the cron scheduler")
	schedulesDB := flag.String("schedules-db", "", `SQLite database for schedule definitions (default: the -db path with ".schedules" before the extension)`)
	schedulePoll := flag.Duration("schedule-poll", scheduler.DefaultPollInterval, "how often to check for schedules that have come due")
	autoMigrate := flag.Bool("migrate", true, "apply pending schema migrations on startup; with -migrate=false, refuse to start against a database that needs them")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "how long to wait for in-flight requests to finish on SIGINT/SIGTERM before forcing shutdown")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(logger)

	retention := queue.Retention{
		Succeeded: *retainSucceeded,
		Failed:    *retainFailed,
		Dead:      *retainDead,
		Cancelled: *retainCancelled,
		BatchSize: *pruneBatch,
	}
	if err := retention.Validate(); err != nil {
		logger.Error("invalid retention flags", "error", err)
		os.Exit(1)
	}

	if !*autoMigrate {
		// Deployments that migrate deliberately (with batch-jobs-migrate,
		// in a step of their own) want a server that starts against the
		// schema it expects or not at all, rather than one that quietly
		// changes the database as a side effect of booting.
		if err := requireMigratedSchema(*dbPath); err != nil {
			logger.Error("database schema is not up to date", "path", *dbPath, "error", err)
			os.Exit(1)
		}
	}

	q, err := sqlite.Open(*dbPath, sqlite.Options{LeaseTTL: *leaseTTL})
	if err != nil {
		logger.Error("failed to open database", "path", *dbPath, "error", err)
		os.Exit(1)
	}
	defer q.Close()

	m := metrics.New(prometheus.NewRegistry())
	if err := m.RegisterQueueDepth("default", q.Len); err != nil {
		logger.Warn("failed to register queue depth gauge", "error", err)
	}
	if err := m.RegisterExpiredLeases("default", func() int {
		n, err := q.ExpiredLeases()
		if err != nil {
			// A scrape is not the place to surface a database error;
			// reporting zero keeps the endpoint serving, and the
			// reaper logs the same failure where it can be acted on.
			logger.Debug("failed to count expired leases", "error", err)
			return 0
		}
		return n
	}); err != nil {
		logger.Warn("failed to register expired lease gauge", "error", err)
	}

	if err := m.RegisterRetainedJobs("default", func() int {
		n, err := q.Retained()
		if err != nil {
			logger.Debug("failed to count retained jobs", "error", err)
			return 0
		}
		return n
	}); err != nil {
		logger.Warn("failed to register retained jobs gauge", "error", err)
	}
	if err := m.RegisterOldestRetainedAge("default", func() float64 {
		oldest, err := q.OldestRetained()
		if err != nil {
			logger.Debug("failed to find the oldest retained job", "error", err)
			return 0
		}
		if oldest.IsZero() {
			return 0
		}
		return time.Since(oldest).Seconds()
	}); err != nil {
		logger.Warn("failed to register oldest retained job gauge", "error", err)
	}

	var scheduleMgr *schedule.Manager
	if *enableSchedules {
		path := *schedulesDB
		if path == "" {
			path = sqlitex.SiblingPath(*dbPath, ".schedules")
		}
		store, err := schedulesqlite.Open(path)
		if err != nil {
			logger.Error("failed to open schedules database", "path", path, "error", err)
			os.Exit(1)
		}
		defer store.Close()
		scheduleMgr = &schedule.Manager{Store: store, Queue: q}
		logger.Info("batch-jobs-server: scheduling enabled", "schedules_db", path, "poll", *schedulePoll)
	}

	srv := &api.Server{
		Store:     q,
		Logger:    logger,
		Metrics:   m,
		AuthToken: *authToken,
		RateLimit: api.RateLimit{RPS: *rateRPS, Burst: *rateBurst},
		Retention: retention,
	}
	// Left nil without -schedules, which is what makes the schedule
	// routes answer 501 rather than pretending to work.
	if scheduleMgr != nil {
		srv.ScheduleStore = scheduleMgr
	}
	if *enableDashboard {
		srv.Assets = dashboard.FS
	}

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The reaper's first sweep runs as part of Start, which is also the
	// startup recovery pass: if this process is replacing one that
	// crashed, its jobs are orphaned right now.
	if *reapInterval > 0 && q.LeaseTTL() > 0 {
		reaper := &sqlite.Reaper{Queue: q, Interval: *reapInterval, Logger: logger, Metrics: m}
		reaper.Start(ctx)
		defer reaper.Stop()
	}

	// Unlike the reaper, the janitor's first pass waits out an interval:
	// nothing about deleting old rows is urgent, and startup is when the
	// process has the most competition for its single write lock.
	if retention.Enabled() && *pruneInterval > 0 {
		janitor := &sqlite.Janitor{
			Queue:     q,
			Retention: retention,
			Interval:  *pruneInterval,
			Logger:    logger,
			Metrics:   m,
		}
		if err := janitor.Start(ctx); err != nil {
			logger.Error("failed to start the janitor", "error", err)
			os.Exit(1)
		}
		defer janitor.Stop()
		logger.Info("batch-jobs-server: retention enabled", "policy", retention.String(), "interval", *pruneInterval)
	}

	if scheduleMgr != nil {
		sched := &scheduler.Scheduler{
			Store:        scheduleMgr.Store,
			Queue:        q,
			PollInterval: *schedulePoll,
			Logger:       logger,
			Metrics:      m,
		}
		sched.Start(ctx)
		defer sched.Stop()
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("batch-jobs-server: listening", "addr", *addr, "db", *dbPath)
		serveErr <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server exited", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		stop()
		logger.Info("batch-jobs-server: shutting down", "timeout", *shutdownTimeout)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed, forcing close", "error", err)
			httpSrv.Close()
		}
	}

	logger.Info("batch-jobs-server: stopped")
}

// requireMigratedSchema reports an error unless path's schema matches this
// binary's migrations exactly: nothing pending, nothing drifted, nothing
// applied that this build does not know about.
func requireMigratedSchema(path string) error {
	db, err := sqlitex.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()

	records, err := sqlitemigrate.Status(db, sqlite.Migrations(), sqlite.MigrationsDir)
	if err != nil {
		return err
	}

	pending := 0
	for _, r := range records {
		if r.Pending() {
			pending++
		}
	}
	if pending > 0 {
		return fmt.Errorf("%d migration(s) pending; run: batch-jobs-migrate -db %s up", pending, path)
	}
	return sqlitemigrate.Verify(db, sqlite.Migrations(), sqlite.MigrationsDir)
}

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid -log-level %q, defaulting to info\n", s)
		return slog.LevelInfo
	}
	return l
}
