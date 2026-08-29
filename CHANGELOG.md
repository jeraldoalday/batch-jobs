# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Entries describe behaviour a user can observe, not the commits that produced
it.

## [0.1.0] - 2026-08-30

The first released version. Everything below is what the public API offers
on day one, rather than a diff against something earlier: before v0.1.0
there was no released version and nothing under `internal/` was ever part
of the contract.

### Added

#### Public API

- `batchjobs`: an embeddable engine — a durable queue, a worker pool
  running handlers registered per queue, and the recovery sweep that
  reclaims jobs whose worker died holding them — wired together by one
  `Open` call. `Config` covers pool size, autoscaling bounds, retry
  backoff, lease TTL, sweep cadence, logger, and metrics registry, and
  rejects unworkable combinations at `Open`.
- `batchjobs`: `Enqueue`, `EnqueueJSON`, `EnqueueResult` (which reports
  whether an idempotency key deduplicated the call), `Get`, `List`,
  `Cancel`, `RetryJob`, `Stats`, `Len`, `Recover`, and `ExpiredLeases`.
- `batchjobs`: `Permanent` and `IsPermanent`, separating a job that is
  wrong (`StatusFailed`) from one that could not get through
  (`StatusDead`), plus the error sentinels callers branch on.
- `batchjobs`: `Engine.APIHandler` mounts the REST API and dashboard on a
  listener the application already has; `Engine.MetricsHandler` serves its
  Prometheus collectors.
- `client`: a Go client for the HTTP API — enqueue (with idempotency keys),
  get, list, cancel, retry, stats, recovery, health, and `Events`/`Watch`
  for the live SSE stats stream. It shares no types with the engine, so it
  costs nothing but the standard library to import.
- `client`: `*Error` carrying the request, status, server message, and
  `Retry-After` hint, unwrapping to sentinels (`ErrNotFound`,
  `ErrConflict`, `ErrQueueFull`, `ErrRateLimited`, `ErrUnauthorized`,
  `ErrInvalidRequest`, `ErrUnsupported`, `ErrServer`), plus `Temporary` and
  `RetryAfter` helpers.

#### Scheduling

- `batchjobs`: cron-driven recurring jobs reach the public API.
  `Config.Schedules` opens a schedule store and runs the loop that enqueues
  each schedule's job as it comes due; `AddSchedule`, `AddScheduleJSON`,
  `GetSchedule`, `ListSchedules`, `UpdateSchedule`, `PauseSchedule`,
  `ResumeSchedule`, `DeleteSchedule` and `TriggerSchedule` manage them. A
  scheduled job is an ordinary job by the time a worker sees it, so
  handlers need to know nothing about schedules.
- `batchjobs`: `Schedule` with `Upcoming` (the next fire times, which is
  the question a cron expression by itself does not answer), `Overdue`, and
  `Decode`; `ValidateCron` for checking an expression before offering to
  save it. Enqueue's own options describe the job template, and the three
  that describe a single submission — `WithDelay`, `WithRunAt`,
  `WithIdempotencyKey` — are rejected rather than silently ignored.
- `batchjobs`: `UpdateSchedule` takes an edit function and moves the next
  fire time only when the expression actually changed, so adjusting a retry
  budget cannot postpone tonight's run. A new expression is validated
  before anything is written, so a rejected edit leaves the stored schedule
  untouched.
- `batchjobs`: `ResumeSchedule` recomputes the next fire time from now, so
  a schedule paused for a week does not fire the instant it comes back.
  `TriggerSchedule` runs one on demand without moving its next fire time or
  recording the run as a firing, and works on a paused schedule.
- `batchjobs`: `Config.SchedulesDB` and `Config.SchedulePoll`, and the
  `ErrScheduleNotFound` and `ErrSchedulingDisabled` sentinels. Schedules
  live in their own database — derived from `Config.DB`, so `jobs.db` gives
  `jobs.schedules.db` — because the two schemas have separately versioned
  migration sets that one `schema_migrations` table cannot track.
- `client`: `AddSchedule`, `AddScheduleJSON`, `GetSchedule`,
  `ListSchedules`, `UpdateSchedule`, `PauseSchedule`, `ResumeSchedule`,
  `DeleteSchedule` and `TriggerSchedule`, with `ScheduleUpdate` as a patch
  whose chainable setters name only the fields being changed. `Schedule`
  exposes the server's projected fire times, and `Fired`/`Overdue` for
  reading its state.
- HTTP: `POST`/`GET /v1/schedules`, `GET`/`PATCH`/`DELETE
  /v1/schedules/{id}`, and `POST /v1/schedules/{id}/pause`, `/resume` and
  `/trigger`. A `PATCH` leaves omitted fields alone; a `DELETE` of an
  absent schedule still answers 204, so the call is safe to retry. A server
  without a schedule store answers 501 on all of them rather than 404, so a
  client can tell "this server does not schedule" from a bad path.
- `cmd/batch-jobs-server`: `-schedules`, `-schedules-db` and
  `-schedule-poll`.
- Dashboard: a schedules panel listing every schedule with its expression,
  next run and last run, with Run now / Pause / Resume / Delete and a form
  to add one. It hides itself on a server that does not schedule, and flags
  an enabled schedule whose next run has passed — the visible symptom of a
  scheduler that is not running or not keeping up.

#### Engine

- `worker.Mux`: dispatches each job to the handler registered for its
  queue, so one pool can serve several logical queues. A queue with neither
  a handler nor a fallback fails permanently with `ErrNoHandler`; duplicate
  registration panics.
- `worker.IsPermanent`: exported so handler middleware can tell a permanent
  failure from a transient one.

#### Performance

- `batchjobs`: `Engine.EnqueueBatch`, `EnqueueBatchJSON`, `Submission`, and
  `JSON` submit many jobs in one transaction, atomically. A durable
  enqueue's cost is dominated by the transaction around the write rather
  than the row, so per-job cost falls from about 83µs one at a time to
  about 28µs in batches of a hundred. Idempotency keys work across a batch
  and within one, so a repeat adopts the original job even when the
  duplicate is another entry in the same call.
- `queue.BatchEnqueuer` and `queue.EnqueueBatch`: the optional-capability
  interface backing the above, and the helper that uses it when a queue has
  it and falls back to a loop when it does not.
- `sqlite.Queue.Optimize`: refreshes the table statistics SQLite's planner
  needs to choose a dequeue index. Seeded at `Open` for a database that
  already holds jobs, and refreshed by `sqlite.Reaper` on its own interval
  (`Reaper.OptimizeInterval`, five minutes by default; negative disables).
- `sqlitex.Options`: connection tuning — `Synchronous`, `CacheSizeKB`,
  `BusyTimeout`, `MaxOpenConns`, and `ConnectRetry` — with `OpenWith` for
  callers that need to deviate from the defaults.
- `cmd/batch-jobs-bench`: a load-test harness that drives producers and
  workers against a real database and reports enqueue latency, end-to-end
  latency, and whether the run kept to its target rate. It paces open-loop
  and corrects for coordinated omission, so a run that could not offer the
  load it was asked for says so instead of reporting a comfortable latency
  for a system in trouble.

### Changed

- Dequeue no longer costs time proportional to the backlog. Migration
  `0004` adds `idx_jobs_dequeue_order`, and the claim query takes each
  status's best row from its own branch so that index can supply the
  ordering rather than SQLite sorting every ready job to take one of them.
  A claim against 50,000 waiting jobs goes from 13.7ms to 245µs, and from
  8.7ms to 241µs when bound to one named queue. Existing databases pick the
  index up on their next migration; no application change is needed.
- SQLite connections now run at `synchronous = NORMAL` rather than `FULL`,
  with an 8MB page cache and a bounded pool whose idle cap matches its open
  cap. Enqueue plus dequeue goes from 325µs to 227µs, and 452µs to 215µs
  under concurrency. In WAL mode a crashing *process* still loses nothing;
  `NORMAL` gives up only the last few committed transactions in a power cut
  or kernel panic. Set `sqlitex.Options.Synchronous` to `SynchronousFull`
  to restore the previous behaviour.

### Fixed

- `sqlitex`: an in-memory database no longer splits across the connection
  pool. SQLite gives every connection opened against `":memory:"` a
  database of its own, so a second connection found an empty one — which
  surfaced as an intermittent "no such table: jobs" from a background sweep
  while foreground work carried on fine. This reached the public API:
  `batchjobs.MemoryDB` was process-local only as long as nothing concurrent
  touched it. Such databases are now pinned to a single connection.
- `sqlitex`: opening several pools against the same fresh database file at
  once no longer fails with `SQLITE_BUSY`. Putting a database into WAL mode
  needs a brief exclusive lock that SQLite does not run the busy handler
  for, so a server starting alongside its migrate step, or two workers
  starting together, could have one fail outright. Connections now retry a
  busy connect.
- `sqlite`: an enqueue carrying an idempotency key no longer reads the
  whole jobs table. `idx_jobs_idempotency` is a partial index, and SQLite
  will only use one for a query whose `WHERE` clause provably implies the
  index's — which `idempotency_key = ?` against a bound parameter does not.
  The query now restates the predicate: 220µs to 48µs per job inside a
  batch, and the gain grows with the table, since the scan was proportional
  to it.
- `api`: the per-client rate limiter no longer grows without bound. Buckets
  are evicted once they have sat idle long enough to have refilled, which
  is indistinguishable from keeping them; previously a server facing many
  distinct client IPs accumulated one live bucket per address for the
  lifetime of the process.
- `client`: `EventStream.Close` is safe to call from another goroutine to
  unblock a parked `Next`, as documented — the closed flag was a plain bool
  read and written concurrently.

### Documentation

- README: quick start for the embedded engine, a Go client section with the
  error model spelled out, and `worker.Mux`.
- Runnable godoc examples for both public packages, including batch
  enqueue.
- `docs/PERFORMANCE.md`: how to measure (microbenchmark versus load test),
  how to read the load report, what coordinated omission is and why the
  send-lag distribution is printed alongside the latency one, every tuning
  change with the numbers behind it, and tuning advice in the order worth
  trying.
- README: a Performance section.
- `make bench`, `make bench-queue`, and `make load`.
- README: a Scheduling section written around the public API rather than
  the internal packages, plus the schedule routes, server flags, and
  dashboard panel.
- `docs/openapi.yaml`: the schedule endpoints, both request bodies, the
  `Schedule` and `Cron` schemas, and the 501 a server without scheduling
  returns.
- Runnable godoc examples for schedules in both public packages.

### Internal

- `schedule.Store.Update` edits a stored schedule in place, leaving
  `enabled` and the firing history to `SetEnabled` and `MarkFired`;
  `Schedule.Reschedule` validates a new expression before applying it.
- `schedule.Manager` holds the editing rules — validation, when an edit may
  move the next fire time, clearing an overdue one on resume, producing a
  job on demand — so the embedded engine and the HTTP API share one
  implementation of them rather than each growing their own.
- `sqlitex.SiblingPath` derives a second database path from the first,
  inserting a suffix before the extension.
- `api.ScheduleStore` is the interface the schedule handlers sit behind,
  satisfied by `schedule.Manager`.

## Earlier work

Before the public API existed, everything lived under `internal/`. In
summary, by day of the project's plan:

- **Day 1** — job model and lifecycle, the `Queue` interface, an in-memory
  queue with priority and delayed heaps, and a worker pool with retries,
  backoff, dead-lettering, and panic recovery.
- **Day 2** — SQLite persistence via the pure-Go `modernc.org/sqlite`
  driver, with embedded migrations applied on open.
- **Day 3** — a hand-rolled cron parser, durable schedule storage, and the
  polling scheduler that enqueues due schedules.
- **Day 4** — structured logging and Prometheus-compatible metrics for job
  outcomes, durations, in-flight counts, and queue depth.
- **Days 5–6** — the REST API (enqueue, get, list, cancel, stats, health),
  API-key auth, per-client rate limiting, request logging, and
  `cmd/batch-jobs-server`.
- **Days 7–8** — the embedded web dashboard, job retry, and the SSE stats
  stream driving it.
- **Day 9** — per-attempt timeouts, permanent errors, idempotency keys, and
  graceful shutdown.
- **Day 10** — multi-queue routing, autoscaling pools, and backpressure via
  bounded queues.
- **Day 11** — job leases, crash recovery and the reaper, terminal-outcome
  write-back, migration checksums and drift detection, and
  `cmd/batch-jobs-migrate`.
