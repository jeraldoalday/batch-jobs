// Command batch-jobs-bench drives synthetic load at a batch-jobs engine
// and reports what came back.
//
// It answers the questions the microbenchmarks in this module cannot. A
// benchmark times one operation in isolation on an idle machine; this runs
// producers and workers together, at a sustained arrival rate, against a
// real database file, which is where queueing effects, write-lock
// contention, and the tail of the latency distribution actually appear.
//
// Two latencies are reported, and the difference between them is usually
// the interesting part:
//
//   - enqueue latency, how long a producer waited for the queue to accept
//     a job;
//   - end-to-end latency, how long a job waited from being enqueued to
//     being finished by a worker, which includes every second it spent
//     sitting in the backlog.
//
// A run that keeps up has an end-to-end distribution close to the handler
// duration. A run that does not has an end-to-end tail that grows for as
// long as the run continues, which is the signature of an arrival rate
// above the service rate rather than of any single slow operation.
//
// Examples:
//
//	# Maximum sustainable enqueue throughput, no workers.
//	batch-jobs-bench -jobs 20000 -workers 0
//
//	# 500 jobs/s for a minute through 8 workers, each job taking 5ms.
//	batch-jobs-bench -rate 500 -duration 1m -workers 8 -work 5ms
//
//	# The same, submitted in batches of 100.
//	batch-jobs-bench -rate 500 -duration 1m -workers 8 -work 5ms -batch 100
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	batchjobs "github.com/jeraldoalday/batch-jobs"
	"github.com/jeraldoalday/batch-jobs/internal/loadgen"
)

// benchQueue is the queue name every job in a run goes on.
const benchQueue = "bench"

func main() {
	cfg := parseFlags()

	if err := run(cfg, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "batch-jobs-bench: %v\n", err)
		os.Exit(1)
	}
}

// config is the parsed command line.
type config struct {
	db        string
	rate      float64
	duration  time.Duration
	jobs      int
	producers int
	batch     int
	workers   int
	work      time.Duration
	payload   int
	warmup    time.Duration
	drainWait time.Duration
	keepDB    bool
	verbose   bool
}

func parseFlags() config {
	var cfg config

	flag.StringVar(&cfg.db, "db", "", "SQLite database path (default: a temporary file removed at the end)")
	flag.Float64Var(&cfg.rate, "rate", 0, "target enqueue rate in jobs per second (0 submits as fast as the queue will accept)")
	flag.DurationVar(&cfg.duration, "duration", 0, "how long to measure for, after any warmup (0 means run until -jobs is reached)")
	flag.IntVar(&cfg.jobs, "jobs", 0, "how many jobs to submit in total (0 means run until -duration elapses)")
	flag.IntVar(&cfg.producers, "producers", loadgen.DefaultConcurrency, "number of producer goroutines")
	flag.IntVar(&cfg.batch, "batch", 1, "jobs per enqueue call; above 1 uses the batch API")
	flag.IntVar(&cfg.workers, "workers", 4, "number of workers consuming the queue (0 enqueues without consuming)")
	flag.DurationVar(&cfg.work, "work", 0, "how long each job's handler takes")
	flag.IntVar(&cfg.payload, "payload", 64, "payload size in bytes")
	flag.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "discard everything observed in this long at the start")
	flag.DurationVar(&cfg.drainWait, "drain", 30*time.Second, "how long to wait after the run for in-flight jobs to finish (0 does not wait)")
	flag.BoolVar(&cfg.keepDB, "keep-db", false, "keep the database file after the run instead of removing it")
	flag.BoolVar(&cfg.verbose, "v", false, "log engine activity to stderr")
	flag.Parse()

	return cfg
}

// run performs one load test end to end and writes the report to out.
func run(cfg config, out io.Writer) error {
	if cfg.duration <= 0 && cfg.jobs <= 0 {
		return errors.New("a run needs -duration, -jobs, or both")
	}
	if cfg.batch < 1 {
		return fmt.Errorf("-batch must be at least 1, got %d", cfg.batch)
	}
	if cfg.producers < 1 {
		return fmt.Errorf("-producers must be at least 1, got %d", cfg.producers)
	}

	dbPath, cleanup, err := databasePath(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	// A run is interruptible, and an interrupted run still reports: the
	// numbers gathered before Ctrl-C are the numbers, and throwing them
	// away would make a long run all-or-nothing.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	engine, latency, err := startEngine(ctx, cfg, dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	if cfg.workers > 0 {
		defer engine.Stop()
	}

	fmt.Fprintf(out, "batch-jobs-bench: %s\n\n", describe(cfg, dbPath))

	result, runErr := loadgen.Run(ctx, loadgen.Config{
		Rate:        cfg.rate / float64(cfg.batch),
		Duration:    cfg.duration,
		Jobs:        callCount(cfg.jobs, cfg.batch),
		Concurrency: cfg.producers,
		Warmup:      cfg.warmup,
	}, submitter(engine, cfg))

	if err := result.Report(out, "enqueue"); err != nil {
		return err
	}

	if cfg.workers > 0 {
		drained := drain(ctx, engine, cfg.drainWait)
		fmt.Fprintln(out)
		if err := loadgen.ReportHistogram(out, "end-to-end", latency.Snapshot()); err != nil {
			return err
		}
		if !drained {
			fmt.Fprintf(out, "\n  NOTE: %d jobs were still waiting when the run ended.\n", engine.Len())
			fmt.Fprintln(out, "  End-to-end latencies above cover only the jobs that finished, so they")
			fmt.Fprintln(out, "  understate a backlog that was still growing. The queue did not keep up.")
		}
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

// callCount converts a job count into a number of enqueue calls, since
// that is what the load generator counts.
func callCount(jobs, batch int) int {
	if jobs <= 0 {
		return 0
	}
	calls := jobs / batch
	if jobs%batch != 0 {
		calls++
	}
	return calls
}

// databasePath resolves where the run's database lives, returning a
// cleanup that removes it unless the caller asked to keep it.
func databasePath(cfg config) (string, func(), error) {
	if cfg.db != "" {
		return cfg.db, func() {}, nil
	}

	dir, err := os.MkdirTemp("", "batch-jobs-bench-")
	if err != nil {
		return "", nil, fmt.Errorf("create temporary directory: %w", err)
	}
	path := filepath.Join(dir, "bench.db")
	if cfg.keepDB {
		return path, func() { fmt.Fprintf(os.Stderr, "database kept at %s\n", path) }, nil
	}
	return path, func() { os.RemoveAll(dir) }, nil
}

// startEngine opens the engine, registers the handler, and starts the
// workers. It returns the recorder the handler reports end-to-end latency
// into.
func startEngine(ctx context.Context, cfg config, dbPath string) (*batchjobs.Engine, *loadgen.Recorder, error) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if cfg.verbose {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	engine, err := batchjobs.Open(batchjobs.Config{
		DB:      dbPath,
		Workers: max(cfg.workers, 1),
		Logger:  logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open engine: %w", err)
	}

	// End-to-end latency is measured from the job's CreatedAt, so it
	// covers the whole time the job existed -- including everything it
	// spent waiting in the backlog, which is the part that grows when the
	// queue cannot keep up and the part a producer-side measurement
	// cannot see.
	latency := loadgen.NewRecorder()
	engine.Handle(benchQueue, func(_ context.Context, j *batchjobs.Job) error {
		if cfg.work > 0 {
			time.Sleep(cfg.work)
		}
		latency.Record(time.Since(j.CreatedAt))
		return nil
	})

	if cfg.workers == 0 {
		// Nothing consumes the queue: the run measures the producer side
		// alone, which is how the enqueue path's own ceiling is found.
		return engine, latency, nil
	}

	if err := engine.Start(ctx); err != nil {
		engine.Close()
		return nil, nil, fmt.Errorf("start engine: %w", err)
	}
	return engine, latency, nil
}

// submitter returns the SubmitFunc for the run: one enqueue per call, or
// one batch per call when -batch is above one.
func submitter(engine *batchjobs.Engine, cfg config) loadgen.SubmitFunc {
	payload := make([]byte, cfg.payload)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	if cfg.batch == 1 {
		return func(ctx context.Context, _ int) error {
			_, err := engine.Enqueue(ctx, benchQueue, payload)
			return err
		}
	}

	// Each producer builds its own submissions, so the slice is not
	// shared; the payload is, which is safe because nothing writes to it.
	return func(ctx context.Context, _ int) error {
		subs := make([]batchjobs.Submission, cfg.batch)
		for i := range subs {
			subs[i] = batchjobs.Submission{Queue: benchQueue, Payload: payload}
		}
		_, err := engine.EnqueueBatch(ctx, subs)
		return err
	}
}

// drain waits for the queue to empty, reporting whether it did within
// wait. A run whose queue never empties is the interesting failure, so
// this reports rather than blocks forever.
func drain(ctx context.Context, engine *batchjobs.Engine, wait time.Duration) bool {
	if wait <= 0 {
		return engine.Len() == 0
	}

	deadline := time.After(wait)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		if engine.Len() == 0 {
			// Len counts waiting jobs, not running ones, so give the
			// workers holding the last few a moment to finish.
			time.Sleep(50 * time.Millisecond)
			return engine.Len() == 0
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}

// describe summarises the run's configuration in one line.
func describe(cfg config, dbPath string) string {
	offered := "unpaced"
	if cfg.rate > 0 {
		offered = fmt.Sprintf("%.0f jobs/s", cfg.rate)
	}
	return fmt.Sprintf("%s, %d producers, batch %d, %d workers, %s per job, %s",
		offered, cfg.producers, cfg.batch, cfg.workers, cfg.work, dbPath)
}
