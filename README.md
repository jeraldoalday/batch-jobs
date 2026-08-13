# batch-jobs

A self-hosted, embeddable job queue and task scheduler for Go.

`batch-jobs` lets a Go application enqueue units of work and process them with
a pool of workers, with support for retries, priorities, delayed/scheduled
execution, and persistence. It is designed to be embedded directly into a
service (no external broker required) while still scaling to a durable,
disk-backed queue when needed.

## Status

Early development. The core queue and job model, both storage backends,
scheduling, and observability are in place; the HTTP API and dashboard are
being built out incrementally. See commit history for progress.

## Goals

- Small, dependency-light core
- Pluggable storage backends (in-memory, SQLite)
- At-least-once delivery with configurable retry/backoff
- Observable: structured logs, metrics, and a simple dashboard

## Storage backends

- `internal/queue.Memory` — in-process, no persistence. State is lost on
  restart; useful for tests and workloads that don't need durability.
- `internal/queue/sqlite.Queue` — backed by a SQLite database file via the
  pure-Go, no-CGO [modernc.org/sqlite](https://modernc.org/sqlite) driver.
  Jobs survive process restarts, and multiple `Queue` instances (in the same
  process or different ones) may safely share one database file.

  ```go
  q, err := sqlite.Open("jobs.db")
  if err != nil {
      log.Fatal(err)
  }
  defer q.Close()
  ```

  Both backends implement the same `queue.Queue` interface, so a `worker.Pool`
  can use either interchangeably.

## Scheduling

`internal/schedule` adds recurring, cron-driven jobs on top of the queue: a
`Schedule` is a job template (queue, payload, priority, retry policy) plus a
standard 5-field cron expression (minute hour day-of-month month
day-of-week — wildcards, lists, ranges, and `/step` are all supported).
`internal/schedule/sqlite.Store` persists schedules the same way
`queue/sqlite.Queue` persists jobs, and `internal/scheduler.Scheduler` is the
loop that polls a `Store` for due schedules, enqueues each one's job onto a
`queue.Queue`, and advances it to its next fire time:

```go
store, err := sqlitestore.Open("schedules.db")
if err != nil {
    log.Fatal(err)
}
defer store.Close()

sch, err := schedule.New("reports", "0 9 * * 1-5", []byte("daily report"))
if err != nil {
    log.Fatal(err)
}
if err := store.Create(sch); err != nil {
    log.Fatal(err)
}

s := &scheduler.Scheduler{Store: store, Queue: q}
s.Start(ctx)
defer s.Stop()
```

Firing a schedule enqueues its job and then advances `NextRunAt` as two
separate steps, so it is at-least-once: a crash between the two re-fires the
schedule on the next poll. Handlers for schedule-produced jobs should be
idempotent, the same requirement any at-least-once queue places on handlers
in general.

## Observability

`worker.Pool` and `scheduler.Scheduler` each take optional `Logger` and
`Metrics` fields; both are safe to leave unset.

- `Logger` is a `*slog.Logger` (defaulting to `slog.Default()`). Lifecycle
  events (pool/scheduler started and stopped) are logged at info level;
  per-job outcomes and per-fire successes at debug level; failures
  (reschedule errors, dead-lettering, disabled schedules) at warn/error
  level.
- `Metrics` is a `*internal/metrics.Metrics` (defaulting to `nil`, which
  disables instrumentation — every method on a nil `*Metrics` is a no-op).
  It records Prometheus counters and histograms:

  - `batchjobs_jobs_processed_total{queue,status}` — jobs processed, by
    queue and terminal status (`succeeded`, `retrying`, `dead`)
  - `batchjobs_job_duration_seconds{queue,status}` — handler execution time
  - `batchjobs_schedule_fires_total` / `batchjobs_schedule_errors_total` —
    schedule fire outcomes

  ```go
  m := metrics.New(prometheus.NewRegistry())

  pool := &worker.Pool{Queue: q, Handler: handle, Metrics: m}
  sched := &scheduler.Scheduler{Store: store, Queue: q, Metrics: m}

  http.Handle("/metrics", m.Handler())
  ```

## License

MIT — see [LICENSE](./LICENSE).
