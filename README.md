# batch-jobs

A self-hosted, embeddable job queue and task scheduler for Go.

`batch-jobs` lets a Go application enqueue units of work and process them with
a pool of workers, with support for retries, priorities, delayed/scheduled
execution, and persistence. It is designed to be embedded directly into a
service (no external broker required) while still scaling to a durable,
disk-backed queue when needed.

## Status

Early development. The core queue and job model, both storage backends,
scheduling, observability, an HTTP API, a dashboard, the first scaling
controls (multi-queue routing, pool autoscaling, backpressure), and
persistence hardening (job leases, crash recovery, migration tooling) are
in place. See commit history for progress.

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
  `schedule.WithTimeout(d)` applies the same bound to every job a recurring
  schedule produces.
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
- **Crash recovery.** A worker claims a job under a time-bounded *lease*
  (`sqlite.Options{LeaseTTL: ...}`, default 60s) and `worker.Pool` extends
  that lease by heartbeating while the handler runs, so a slow job keeps its
  claim for as long as the worker holding it is alive. If the process dies
  instead, nothing renews the lease, and `Queue.Recover` hands the job back:
  requeued for another attempt, or dead-lettered if its retries are already
  spent. Recovery spends an attempt, so a job that kills whichever worker
  picks it up eventually lands in the dead-letter state rather than crashing
  workers forever. `sqlite.Reaper` runs that sweep on an interval (and once
  immediately on start, for the process replacing a crashed peer), and
  `POST /v1/recover` triggers it on demand.

  ```go
  q, _ := sqlite.Open("jobs.db", sqlite.Options{LeaseTTL: 30 * time.Second})

  reaper := &sqlite.Reaper{Queue: q, Interval: 10 * time.Second, Metrics: m}
  reaper.Start(ctx)
  defer reaper.Stop()

  // The pool heartbeats automatically for any queue that leases.
  pool := &worker.Pool{Queue: q, Handler: handle, Metrics: m}
  ```

  A worker that loses its lease mid-job (because recovery reclaimed it) has
  its handler context cancelled and its outcome discarded, so two workers
  never race to report on the same attempt.
- **Outcome write-back.** `worker.Pool` reports terminal outcomes to queues
  implementing `queue.Completer` (`queue/sqlite.Queue` does), so a finished
  job's stored row leaves `running` and releases its lease. Retries go back
  through `Enqueue` as before; `queue.Memory`, which drops a job the moment
  it is claimed, is unaffected.
- **Graceful shutdown.** `cmd/batch-jobs-server` listens for SIGINT/SIGTERM
  and calls `http.Server.Shutdown`, giving in-flight requests up to
  `-shutdown-timeout` (default 10s) to finish before the listener is forced
  closed.

## Scaling

Three things stop a queue from keeping up: work arriving faster than the
backlog can absorb, a pool sized for the wrong load, and one busy queue
crowding out every other one. Each has a knob.

### Multi-queue routing

`queue.Router` is a `Queue` that fans `Enqueue` out to a backing queue per
logical queue name and fans `Dequeue` back in across all of them, so a
single `worker.Pool` can serve several queues without either splitting into
one pool per queue or letting the busiest producer monopolise a shared one.

Each route carries a `Weight`: the number of jobs it may serve in a row
before the router moves on. A weight is a share, not a cap — an idle
sibling never stops a busy route from using the whole pool.

```go
r, err := queue.NewRouter([]queue.Route{
    {Name: "payments", Queue: payments, Weight: 3},
    {Name: "emails", Queue: emails, Weight: 1},
})
if err != nil {
    log.Fatal(err)
}
pool := &worker.Pool{Queue: r, Handler: handle}
```

While both queues are busy this alternates three payments to one email;
when payments is empty, emails takes every worker. A job whose queue name
matches no route is `ErrNoRoute`, unless `queue.WithDefaultRoute(q)`
supplies a catch-all. `Close` shuts down the router only: the backing
queues belong to whoever created them.

With the SQLite backend, `Queue.Bind(name)` returns a view restricted to one
logical queue — it claims and counts only that queue's jobs — so every route
can share one database file, one schema, and one connection pool:

```go
q, err := sqlite.Open("jobs.db")
if err != nil {
    log.Fatal(err)
}
defer q.Close()

r, err := queue.NewRouter([]queue.Route{
    {Name: "payments", Queue: q.Bind("payments"), Weight: 3},
    {Name: "emails", Queue: q.Bind("emails"), Weight: 1},
})
```

### Dynamic pool sizing

A fixed `Concurrency` means paying for peak capacity all day or watching a
backlog build during a burst. Setting `Pool.Scale.Max` enables autoscaling:
the pool samples its own workload every `ScaleInterval` (default 1s) and
resizes itself within `[Min, Max]`.

```go
pool := &worker.Pool{
    Queue:       q,
    Handler:     handle,
    Concurrency: 2, // starting size, clamped into [Min, Max]
    Scale: worker.Policy{
        Min:            2,
        Max:            32,
        JobsPerWorker:  10, // one extra worker per 10 waiting jobs
        ScaleDownAfter: 3,  // idle samples before retiring a worker
    },
}
```

The target size is every busy worker plus enough additional workers to cover
the backlog at `JobsPerWorker` each. Growth is applied immediately — a
backlog is work already waiting — while shrinking retires one worker per
decision and only after `ScaleDownAfter` consecutive samples agree the pool
is oversized, so a bursty workload does not thrash goroutines up and down.

Retiring a worker stops it pulling new jobs but never cancels the job in its
hands: each worker's `Dequeue` runs under its own context, separate from the
pool-wide context handlers run under. `Max` is deliberately required rather
than inferred, since pool concurrency is what decides how hard handlers hit
whatever they talk to.

`Pool.Workers()` and `Pool.Busy()` report the current numbers, and the pool
exports `batchjobs_pool_workers` and `batchjobs_pool_scale_events_total`
(labeled `up`/`down`) when `Metrics` is set.

### Backpressure

An unbounded in-memory queue turns a producer that outruns its workers into
an out-of-memory crash. `queue.NewMemory(queue.WithCapacity(n))` bounds the
backlog, returning `queue.ErrFull` once it is reached:

```go
q := queue.NewMemory(queue.WithCapacity(10_000))
```

Admission control applies only to newly submitted jobs. A retry coming back
around, or a job being dead-lettered, was already admitted and counted once,
so it is always let through — a bounded queue can exceed its capacity
briefly, by at most the number of jobs in flight, rather than dropping work
it already accepted. Over HTTP, `POST /v1/jobs` answers a full queue with
`503` and a `Retry-After` header (configurable via `Server.RetryAfter`)
instead of `500`, since the request was well-formed and will likely succeed
once the backlog drains.

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
  - `batchjobs_pool_workers{pool}` — worker goroutines currently running
  - `batchjobs_pool_scale_events_total{pool,direction}` — autoscaling
    decisions applied, by direction (`up`, `down`)
  - `batchjobs_jobs_recovered_total{outcome}` — orphaned jobs reclaimed
    after a worker died holding them, by outcome (`requeued`, `dead`)
  - `batchjobs_expired_leases{queue}` — a pull-based gauge of claims that
    have lapsed but not yet been recovered, registered via
    `Metrics.RegisterExpiredLeases`; it stays above zero when a reaper has
    stopped running
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
| POST   | `/v1/recover`           | Reclaim jobs orphaned by a dead worker (501 if the store doesn't lease) |
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

Lease and recovery behaviour are flags too: `-lease-ttl` (negative disables
leasing), `-reap-interval` (0 disables the reaper), and `-migrate=false` to
refuse to start against a database whose schema this binary did not expect.

## Schema migrations

Migrations are numbered `.sql` files embedded in each store
(`internal/queue/sqlite/migrations`, `internal/schedule/sqlite/migrations`)
and applied by `internal/sqlitemigrate`, which records each one in a
`schema_migrations` table along with a SHA-256 of its contents. That
checksum is the point: editing a migration after it has been applied would
otherwise leave the running database and the migration set describing
different schemas, silently. Applying is safe from several processes at
once — the bookkeeping row is written first, so whichever process loses the
race skips the migration rather than half-applying it.

`cmd/batch-jobs-migrate` separates the schema decision from the deploy:

```sh
go run ./cmd/batch-jobs-migrate -db jobs.db status   # what would run, and what already has
go run ./cmd/batch-jobs-migrate -db jobs.db up       # apply everything pending
go run ./cmd/batch-jobs-migrate -db jobs.db verify   # deploy gate: non-zero on drift
go run ./cmd/batch-jobs-migrate -db jobs.db version  # highest applied version
```

`status` reports each migration as `applied`, `pending`, `drifted` (its file
changed after it was applied), or `missing` (the database was migrated by a
newer build than the one running). Each store keeps its own database file:
pointing two migration sets at one file is rejected, since they number
migrations independently and would collide in a shared `schema_migrations`
table.

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
