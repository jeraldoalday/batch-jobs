# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until v0.1.0 there is no released version, so everything below sits under
Unreleased. Entries describe behaviour a user can observe, not the commits
that produced it.

## [Unreleased]

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

#### Engine

- `worker.Mux`: dispatches each job to the handler registered for its
  queue, so one pool can serve several logical queues. A queue with neither
  a handler nor a fallback fails permanently with `ErrNoHandler`; duplicate
  registration panics.
- `worker.IsPermanent`: exported so handler middleware can tell a permanent
  failure from a transient one.

### Fixed

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
- Runnable godoc examples for both public packages.

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
