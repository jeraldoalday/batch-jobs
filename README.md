# batch-jobs

A self-hosted, embeddable job queue and task scheduler for Go.

`batch-jobs` lets a Go application enqueue units of work and process them with
a pool of workers, with support for retries, priorities, delayed/scheduled
execution, and persistence. It is designed to be embedded directly into a
service (no external broker required) while still scaling to a durable,
disk-backed queue when needed.

> **How this project is built:** I develop this library with AI coding agents. I decide how jobs should be queued, retried, and scheduled, review each change and its tests, and merge what holds up.

## Status

v0.1.0. The core queue and job model, both storage backends, cron
scheduling, observability, an HTTP API, a dashboard, the scaling controls
(multi-queue routing, pool autoscaling, backpressure), persistence
hardening (job leases, crash recovery, migration tooling), and a public Go
API (the `batchjobs` engine and the `client` package) are in place.

## Goals

- Small, dependency-light core
- Pluggable storage backends (in-memory, SQLite)
- At-least-once delivery with configurable retry/backoff
- Observable: structured logs, metrics, and a simple dashboard

## Quick start

The `batchjobs` package is the embedded engine: a durable queue, a pool of
workers, and the recovery sweep that reclaims jobs whose worker died holding
them. Producing and consuming happen in the same process, so there is no
broker to run.

```go
engine, err := batchjobs.Open(batchjobs.Config{DB: "jobs.db", Workers: 8})
if err != nil {
    log.Fatal(err)
}
defer engine.Close()

engine.Handle("emails", func(ctx context.Context, j *batchjobs.Job) error {
    var msg Message
    if err := j.Decode(&msg); err != nil {
        // A payload that will not parse now will not parse on a retry.
        return batchjobs.Permanent(err)
    }
    return send(ctx, msg)
})

if err := engine.Start(ctx); err != nil {
    log.Fatal(err)
}
defer engine.Stop()

_, err = engine.EnqueueJSON(ctx, "emails", Message{To: "a@example.com"},
    batchjobs.WithMaxRetries(3),
    batchjobs.WithTimeout(30*time.Second),
)
```

Handlers are registered per queue; a job whose queue has no handler fails
permanently rather than burning its retries on code that does not exist.
Jobs are durable the moment `Enqueue` returns, so an `Engine` opened against
an existing database picks up where the last process left off.

The lifecycle is deliberately split. `Open` returns a usable engine before
`Start`, since a producer-only process needs to enqueue and inspect without
running workers at all. `Stop` drains in-flight handlers without closing the
database, so jobs can still be recorded after the workers are gone. `Close`
releases it.

### Inspecting and steering work

```go
stats, err := engine.Stats(ctx)          // per-status counts
fmt.Println(stats.Backlog(), stats.Failing())

dead, err := engine.List(ctx, batchjobs.StatusDead, 100)
for _, j := range dead {
    log.Printf("%s failed after %d attempts: %s", j.ID, j.Attempt(), j.LastError)
    engine.RetryJob(ctx, j.ID)           // back to pending, counters cleared
}

engine.Cancel(ctx, id)                   // a job a worker has not claimed yet
engine.Recover(ctx)                      // reclaim a dead host's jobs now
```

`Permanent(err)` is the one piece of handler vocabulary worth learning: it
separates *this job is wrong* (`StatusFailed`, worth a human reading) from
*we could not get through* (`StatusDead`, worth retrying once whatever broke
is fixed). Without it, a malformed payload spends every retry it has before
anyone hears about it.

### Serving the queue over HTTP

`Engine.APIHandler` mounts the same REST API and dashboard on a listener the
application already has, so producers in other processes reach the same jobs
the embedded workers are running:

```go
mux.Handle("/jobs/", http.StripPrefix("/jobs", engine.APIHandler(batchjobs.APIOptions{
    AuthToken: os.Getenv("JOBS_API_KEY"),
    Dashboard: true,
})))
mux.Handle("/metrics", engine.MetricsHandler())
```

## Go client

The `client` package talks to any batch-jobs server — one run by
`cmd/batch-jobs-server`, or an application's own `Engine.APIHandler`. It
shares no types with the engine, so importing it costs nothing but the
standard library.

```go
c, err := client.New("https://jobs.internal", client.WithAPIKey(key))
if err != nil {
    log.Fatal(err)
}

j, err := c.EnqueueJSON(ctx, "emails", msg,
    client.WithMaxRetries(3),
    client.WithIdempotencyKey(orderID),
)
```

Failures are an unwrappable `*client.Error` carrying the request, status,
the server's message, and its `Retry-After` hint, with sentinels for the
outcomes worth branching on (`ErrNotFound`, `ErrConflict`, `ErrQueueFull`,
`ErrRateLimited`, `ErrUnauthorized`, `ErrInvalidRequest`, `ErrUnsupported`,
`ErrServer`). `client.Temporary(err)` draws the line between a request worth
repeating unchanged and an answer:

```go
if _, err := c.Enqueue(ctx, "emails", payload); err != nil {
    if !client.Temporary(err) {
        return err                       // 400, 404, 501: an answer
    }
    time.Sleep(client.RetryAfter(err, time.Second))
}
```

Operational calls are there too: `Recover` reclaims jobs orphaned by a dead
worker, and `Prune` runs the server's retention policy now rather than at
its next scheduled pass. Neither takes parameters — a client free to name
its own retention could delete every finished job on the server by asking
for a retention of zero — and both answer `ErrUnsupported` on a server that
does not do them.

`Client.Watch` follows the server's live stats stream — the same SSE feed
the dashboard's tiles are driven by — so a monitor does not have to poll:

```go
err := c.Watch(ctx, func(stats client.Stats) error {
    log.Printf("%s (backlog %d)", stats, stats.Backlog())
    return nil
})
```

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

Recurring work is a cron expression plus the job template every firing is
stamped from. Turn it on with `Config.Schedules` and the engine opens a
second database for the definitions and runs the loop that enqueues each
one as it comes due:

```go
engine, err := batchjobs.Open(batchjobs.Config{
    DB:        "jobs.db",
    Workers:   4,
    Schedules: true, // schedule definitions land in jobs.schedules.db
})
...
engine.Handle("reports", buildReport)
engine.Start(ctx)

// 3am on weekdays.
s, err := engine.AddScheduleJSON(ctx, "reports", "0 3 * * 1-5",
    Request{Window: "daily"},
    batchjobs.WithMaxRetries(3),
    batchjobs.WithTimeout(10*time.Minute),
)
```

A scheduled job is an ordinary job by the time a worker sees it, so the
handler needs to know nothing about schedules.

The expression is the standard five fields — minute, hour, day of month,
month, day of week — with wildcards, lists, ranges, and `/step`.
`ValidateCron` checks one before you offer to save it, and
`Schedule.Upcoming(n)` answers the question the expression itself does not:

```go
times, _ := s.Upcoming(3) // when does this actually run?
```

The rest of the surface is `GetSchedule`, `ListSchedules`,
`UpdateSchedule`, `PauseSchedule`, `ResumeSchedule`, `DeleteSchedule`, and
`TriggerSchedule` — mirrored by the `client` package and by
`/v1/schedules` over HTTP. Three of them behave in ways worth knowing:

- **`UpdateSchedule` takes an edit function** and moves the next fire time
  only when the expression actually changed, so adjusting a retry budget
  cannot silently postpone tonight's run. A new expression is validated
  before anything is written: an edit that will not parse leaves the stored
  schedule exactly as it was.
- **`ResumeSchedule` recomputes the next fire time from now.** Without
  that, a schedule paused for a week comes back a week overdue and fires
  the instant it is resumed — exactly the surprise pausing it was meant to
  avoid. Pausing accumulates no missed firings; the time it was paused for
  simply does not happen.
- **`TriggerSchedule` runs a schedule now** without disturbing its
  timetable: the next fire time does not move and the run is not recorded
  as a firing. It works on a paused schedule, since pausing stops the
  timetable rather than the operator.

Schedules live in their own database because the jobs schema and the
schedules schema have separately versioned migration sets, and one
`schema_migrations` table cannot track both. The path is derived from
`Config.DB` — `jobs.db` gives `jobs.schedules.db` — or named outright with
`Config.SchedulesDB`, which on its own is enough to turn the feature on.
`Config.SchedulePoll` (default 1s) bounds how late a firing can be.

Under the public surface, `internal/schedule` holds the cron parser and the
`Schedule` template, `internal/schedule/sqlite.Store` persists them the way
`queue/sqlite.Queue` persists jobs, `internal/schedule.Manager` holds the
editing rules shared by the engine and the HTTP API, and
`internal/scheduler.Scheduler` is the polling loop.

Firing enqueues the job and then advances `NextRunAt` as two separate
steps, so it is at-least-once: a crash between the two re-fires the
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

## Retention

A durable queue does not forget anything on its own. Every job it has ever
run is still a row in the `jobs` table, so the database grows for as long
as the queue is used, and the indexes that make dequeue fast grow with it.
Retention is the policy that says what may be deleted.

```go
engine, err := batchjobs.Open(batchjobs.Config{
    DB: "jobs.db",
    Retention: batchjobs.Retention{
        Succeeded: 48 * time.Hour,
        Cancelled: 48 * time.Hour,
        Failed:    30 * 24 * time.Hour,
        // Dead is left at zero: forever.
    },
    PruneInterval: time.Hour,
})
```

The durations are per terminal status because the four mean different
things. A succeeded job is an audit trail whose value decays in hours. A
dead-lettered job is a *work list* — deleting one discards the only record
that the work was ever meant to happen — which is why `Dead` stays at
"forever" even in `DefaultRetention()`.

**The zero value keeps everything**, so an existing deployment behaves
exactly as it did until it asks for something else. A negative duration is
rejected rather than clamped, at `Open` rather than at the first sweep: its
two readings ("forever" and "immediately") are opposites, and a policy that
would delete the wrong thing should stop the program starting rather than
surface an hour later in a log line nobody is reading.

- **Pruning is batched.** SQLite has a single writer, so one unbounded
  `DELETE` would hold the write lock for as long as it took to remove the
  whole history, and every enqueue, claim, and completion in every process
  sharing the file would block behind it. A pass issues bounded deletes
  (`BatchSize`, default 1000) until nothing eligible is left, giving the
  lock up between rounds — so pruning a large backlog slows the queue
  instead of stopping it. Each batch takes the oldest rows first.
- **Only finished jobs are touched**, however old a pending one is, and
  age is measured from when a job *finished* (its `updated_at`), not from
  when it was created.
- **`sqlite.Janitor`** runs the pass on `PruneInterval` (default 1h).
  Unlike `sqlite.Reaper`, its first sweep waits out an interval rather than
  running at startup: nothing about deleting month-old rows is urgent, and
  startup is when the process has the most competition for its write lock.
- **A fleet should prune from one place.** Several janitors against one
  database are correct but pointless — one pass already removes everything
  eligible, and the rest only duplicate the write-lock pressure. Set
  `PruneInterval` negative to keep the policy and run the passes yourself,
  via `Engine.Prune` or `POST /v1/prune`.

`Engine.Retained()` and `Engine.OldestRetained()` are the two numbers that
say whether retention is keeping up, and they say different things: the
count can sit still while the history quietly ages past its policy, on a
queue whose pruning stopped and which has since gone quiet.

```sh
# The same policy on the standalone server.
go run ./cmd/batch-jobs-server -db jobs.db \
    -retain-succeeded 48h -retain-cancelled 48h -retain-failed 720h \
    -prune-interval 1h
```

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

### Per-queue handlers

`worker.Mux` is the consumer-side counterpart to `queue.Router`: it
dispatches each job to the handler registered for its queue, so one pool can
serve several logical queues without a pool (and its idle goroutines) per
queue, or a hand-written switch on `j.Queue`.

```go
mux := worker.NewMux()
mux.Handle("emails", sendEmail)
mux.Handle("reports", buildReport)
mux.Fallback(park)                       // optional catch-all

pool := &worker.Pool{Queue: q, Handler: mux.Serve}
```

Duplicate registration panics, the way `net/http`'s does: two packages both
claiming `"emails"` is a bug worth surfacing at startup rather than as jobs
quietly running the wrong code. A job whose queue has neither a handler nor a
fallback fails with `ErrNoHandler` wrapped in `Permanent` — retrying cannot
make code appear, so the attempts would only delay someone noticing.
`Engine.Handle` is this, wired up for you.

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

## Performance

Full numbers, methodology, and tuning advice are in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md). The short version:

**Batch your producers.** A durable enqueue's cost is dominated by the
transaction around the write, not by the row. Submitting a hundred jobs one
at a time costs about 83µs each; in one batch, about 28µs.

```go
subs := make([]batchjobs.Submission, 0, len(recipients))
for _, to := range recipients {
    sub, err := batchjobs.JSON("emails", Message{To: to},
        batchjobs.WithIdempotencyKey("welcome:"+to))
    if err != nil {
        log.Fatal(err)
    }
    subs = append(subs, sub)
}

jobs, err := engine.EnqueueBatch(ctx, subs)
```

The batch is atomic — either every job is stored or none is — so a caller
that gets an error can retry the whole thing without working out which half
went through. Idempotency keys work across a batch and within one, which is
what makes that retry safe.

**Dequeue is flat in the backlog.** Claiming a job costs about 240µs
whether 1,000 or 50,000 jobs are waiting. That took a second index
(`idx_jobs_dequeue_order`), a claim query split into one branch per status
so the index can supply the ordering, and maintained table statistics so
SQLite chooses between the two indexes correctly. Before, a claim against a
50,000-job backlog took 13.7ms — and every worker runs that query in a
loop, so the queue got slower exactly as it filled up.

**Load-test the whole thing, not one call.**

```sh
# 500 jobs/s for a minute through 8 workers, each job taking 5ms.
go run ./cmd/batch-jobs-bench -rate 500 -duration 1m -workers 8 -work 5ms
```

`batch-jobs-bench` reports enqueue latency and end-to-end latency
separately, and the gap between them is the diagnosis. A queue that has
fallen behind still *accepts* jobs in microseconds — only the end-to-end
distribution shows the backlog, and it keeps growing for as long as the run
lasts. The harness paces open-loop and corrects for coordinated omission,
so a run that could not offer the load it was asked for says so rather than
reporting a comfortable latency for a system in trouble.

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
  - `batchjobs_jobs_pruned_total{status}` — finished jobs deleted by a
    retention pass, by the terminal status they were in; a rising
    `status="dead"` means work is being discarded before anyone looked at it
  - `batchjobs_retained_jobs{queue}` and
    `batchjobs_oldest_retained_job_age_seconds{queue}` — pull-based gauges
    for the size and age of the kept history, registered via
    `Metrics.RegisterRetainedJobs` and `Metrics.RegisterOldestRetainedAge`.
    Both are registered whether or not a policy is set, because the queue
    with no policy is the one whose history grows without bound
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
| POST   | `/v1/schedules`         | Create a recurring schedule         |
| GET    | `/v1/schedules`         | List every schedule                 |
| GET    | `/v1/schedules/{id}`    | Get a schedule by ID                |
| PATCH  | `/v1/schedules/{id}`    | Edit a schedule (omitted fields are left alone) |
| DELETE | `/v1/schedules/{id}`    | Delete a schedule (204 even if absent) |
| POST   | `/v1/schedules/{id}/pause`   | Stop a schedule firing         |
| POST   | `/v1/schedules/{id}/resume`  | Re-enable it, clearing an overdue fire time |
| POST   | `/v1/schedules/{id}/trigger` | Run it now, leaving its timetable alone |
| POST   | `/v1/recover`           | Reclaim jobs orphaned by a dead worker (501 if the store doesn't lease) |
| POST   | `/v1/prune`             | Delete finished jobs the server's retention policy has expired (501 if it has none) |
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
Retention is flags too: `-retain-succeeded`, `-retain-failed`,
`-retain-dead`, and `-retain-cancelled` (all zero — keep forever — by
default), with `-prune-interval` and `-prune-batch` beside them; a negative
`-prune-interval` keeps the policy while leaving the passes to
`POST /v1/prune`, which is what a fleet sharing one database file wants.
`-schedules` serves the schedule routes and runs the cron loop, with
`-schedules-db` and `-schedule-poll` beside it; without the flag those
routes stay mounted and answer `501`, so a client is told the server does
not schedule rather than handed a `404` it cannot tell from a typo.

The schedule routes need a `ScheduleStore`, which `schedule.Manager`
satisfies — `Engine.APIHandler` wires it for you on an engine opened with
`Config.Schedules`.

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
breakdown, a status-filterable job table with cancel/retry actions, and a
schedules panel — as a static asset tree with no build step. Mount it on a
`Server` by setting `Assets`:

```go
srv := &api.Server{Store: sqliteQueue, Assets: dashboard.FS}
```

`cmd/batch-jobs-server` does this by default; pass `-dashboard=false` to
disable it. Stat tiles update live over `/v1/events` (Server-Sent Events),
falling back to polling `/v1/stats` every 5s if the connection drops; the job
table itself polls `/v1/jobs` on the same interval.

The schedules panel lists every schedule with its expression, next run, and
last run, and offers Run now / Pause / Resume / Delete plus a small form to
add one. It stays hidden unless the server answers `/v1/schedules`, since a
panel whose every button returns "not implemented" is worse than no panel.
An enabled schedule whose next run has already passed is flagged: that is
the visible symptom of a scheduler that is not running, or not keeping up.

## License

MIT — see [LICENSE](./LICENSE).
