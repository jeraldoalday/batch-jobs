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
	"github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dbPath := flag.String("db", "batch-jobs.db", `SQLite database path (":memory:" for ephemeral)`)
	authToken := flag.String("auth-token", "", "if set, required as the X-API-Key header on every request except /healthz")
	rateRPS := flag.Float64("rate-rps", 0, "requests per second allowed per client IP (0 disables rate limiting)")
	rateBurst := flag.Int("rate-burst", 20, "burst size for per-client rate limiting")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, or error")
	enableDashboard := flag.Bool("dashboard", true, "serve the web dashboard at /")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "how long to wait for in-flight requests to finish on SIGINT/SIGTERM before forcing shutdown")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(logger)

	q, err := sqlite.Open(*dbPath)
	if err != nil {
		logger.Error("failed to open database", "path", *dbPath, "error", err)
		os.Exit(1)
	}
	defer q.Close()

	m := metrics.New(prometheus.NewRegistry())
	if err := m.RegisterQueueDepth("default", q.Len); err != nil {
		logger.Warn("failed to register queue depth gauge", "error", err)
	}

	srv := &api.Server{
		Store:     q,
		Logger:    logger,
		Metrics:   m,
		AuthToken: *authToken,
		RateLimit: api.RateLimit{RPS: *rateRPS, Burst: *rateBurst},
	}
	if *enableDashboard {
		srv.Assets = dashboard.FS
	}

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid -log-level %q, defaulting to info\n", s)
		return slog.LevelInfo
	}
	return l
}
