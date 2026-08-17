# batch-jobs

A self-hosted, embeddable job queue and task scheduler for Go.

`batch-jobs` lets a Go application enqueue units of work and process them with
a pool of workers, with support for retries, priorities, delayed/scheduled
execution, and persistence. It is designed to be embedded directly into a
service (no external broker required) while still scaling to a durable,
disk-backed queue when needed.

## Status

Early development. The core queue and job model, both storage backends,
scheduling, observability, and a first HTTP API are in place; the dashboard
is being built out incrementally. See commit history for progress.

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

## Reliability

- **Per-job timeouts.** `job.WithTimeout(d)` bounds a single execution
  attempt: `worker.Pool` derives a `context.WithTimeout` from it before
  calling the handler, so a well-behaved handler that respects `ctx.Done()`
  is actually interrupted rather than left to run indefinitely. Persisted by
  `queue/sqlite.Queue` and exposed as `timeout_seconds` over the HTTP API.
- **Permanent failures.** Most handler errors are transient and go through
  the normal retry/backoff/dead-letter path, but some errors (a malformed
  payload, for example) can never succeed on retry. Wrapping one with
  `worker.Permanent(err)` marks the job `StatusFailed` immediately, skipping
  both retry scheduling and dead-lettering.
- **Idempotency keys.** `job.WithIdempotencyKey(key)` lets a producer safely
  retry an `Enqueue` call (e.g. after a network error) without risking a
  duplicate: both `queue.Memory` and `queue/sqlite.Queue` treat a second
  `Enqueue` with the same `(Queue, IdempotencyKey)` pair as a no-op that
  copies the original job's state into the caller's `*job.Job` instead of
  creating a new one. `queue/sqlite.Queue` enforces this with a database
  unique index, so it holds even across process restarts and concurrent
  writers; `queue.Memory` scopes it to jobs still waiting (not yet claimed by
  a worker), since it keeps no record of a job once dequeued.
- **Graceful shutdown.** `cmd/batch-jobs-server` listens for SIGINT/SIGTERM
  and calls `http.Server.Shutdown`, giving in-flight requests up to
  `-shutdown-timeout` (default 10s) to finish before the listener is forced
  closed.

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
  - `batchjobs_jobs_in_flight{queue}` — jobs currently executing in a handler
  - `batchjobs_schedule_fires_total` / `batchjobs_schedule_errors_total` —
    schedule fire outcomes
  - `batchjobs_scheduler_tick_duration_seconds` — time per scheduler poll
  - `batchjobs_queue_depth{queue}` — a pull-based gauge for a `Queue`'s
    current backlog (pending + retrying jobs), registered separately via
    `Metrics.RegisterQueueDepth` since it reads `Queue.Len` on every scrape
    rather than being updated as events happen

  ```go
  m := metrics.New(prometheus.NewRegistry())
  m.RegisterQueueDepth("emails", q.Len)

  pool := &worker.Pool{Queue: q, Handler: handle, Metrics: m}
  sched := &scheduler.Scheduler{Store: store, Queue: q, Metrics: m}

  http.Handle("/metrics", m.Handler())
  ```

## HTTP API

`internal/api.Server` exposes a `Store` (a superset of `queue.Queue` that
adds `Get`/`List`/`Cancel`/`Stats` — `queue/sqlite.Queue` satisfies it
directly) over REST:

| Method | Path                   | Description                        |
| ------ | ---------------------- | ----------------------------------- |
| POST   | `/v1/jobs`              | Enqueue a job (200 instead of 201 if `idempotency_key` deduped it) |
| GET    | `/v1/jobs/{id}`         | Get a job by ID                    |
| GET    | `/v1/jobs?status=&limit=` | List jobs by status              |
| POST   | `/v1/jobs/{id}/cancel`  | Cancel a pending/retrying job      |
| POST   | `/v1/jobs/{id}/retry`   | Retry a dead/failed job             |
| GET    | `/v1/stats`             | Job counts by status               |
| GET    | `/v1/events`            | SSE stream of live stats           |
| GET    | `/healthz`              | Liveness probe (never requires auth) |
| GET    | `/metrics`              | Prometheus scrape, if `Metrics` is set |
| GET    | `/`                     | Dashboard, if `Assets` is set       |

```go
srv := &api.Server{
    Store:     sqliteQueue,
    Metrics:   m,
    AuthToken: os.Getenv("BATCH_JOBS_API_TOKEN"), // empty disables auth
    RateLimit: api.RateLimit{RPS: 10, Burst: 20},   // zero value disables
}
http.ListenAndServe(":8080", srv.Handler())
```

Auth is a static `X-API-Key` header check, and rate limiting is a small
hand-rolled per-client-IP token bucket — both are stubs meant as a starting
point, not a production-grade auth/throttling layer. The full contract is
documented in [`docs/openapi.yaml`](./docs/openapi.yaml). A standalone
binary wiring this up against a SQLite-backed queue lives in
`cmd/batch-jobs-server`:

```sh
go run ./cmd/batch-jobs-server -addr :8080 -db jobs.db
```

## Dashboard

`internal/dashboard` embeds a minimal web UI — stat tiles for the job-count
breakdown, and a status-filterable job table with cancel/retry actions — as a
static asset tree with no build step. Mount it on a `Server` by setting
`Assets`:

```go
srv := &api.Server{Store: sqliteQueue, Assets: dashboard.FS}
```

`cmd/batch-jobs-server` does this by default; pass `-dashboard=false` to
disable it. Stat tiles update live over `/v1/events` (Server-Sent Events),
falling back to polling `/v1/stats` every 5s if the connection drops; the job
table itself polls `/v1/jobs` on the same interval.

## License

MIT — see [LICENSE](./LICENSE).
