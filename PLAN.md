# 14-Day Plan

## Day 1 — Core engine (done)
- Job/Status model with lifecycle transitions
- Queue interface; in-memory queue with priority + delayed heaps
- Worker Pool: retry, backoff (constant/exponential), dead-letter, panic recovery
- Unit + concurrency tests, Makefile, CI (vet/fmt/race)

## Day 2 — Persistence (done)
- SQLite-backed `Queue` implementation (`internal/queue/sqlite`, satisfies
  existing `queue.Queue` interface) via modernc.org/sqlite (pure Go, no CGO)
- Embedded schema migrations, applied idempotently on `Open`
- List/Stats helpers for inspecting terminal jobs (Memory drops these)
- Durability tests (restart-and-recover), concurrency tests, benchmarks vs
  in-memory queue

## Day 3 — Scheduling (done)
- `internal/schedule`: hand-rolled 5-field cron parser (lists, ranges,
  steps, wildcards) and `Cron.Next`; `Schedule` job template + `Store`
  interface
- `internal/schedule/sqlite`: durable `Store` implementation, sharing the
  connection setup and migration runner extracted from Day 2's
  `queue/sqlite` into `internal/sqlitex` / `internal/sqlitemigrate`
- `internal/scheduler`: polling loop that enqueues due schedules' jobs and
  advances them, mirroring `worker.Pool`'s Start/Stop shape
- Delayed-execution polish: `worker.WithJitter` to decorrelate synchronized
  retries

## Day 4 — Observability (done)
- Structured logging (`slog`): lifecycle (start/stop) at info level,
  per-job and per-fire outcomes at debug level, across `worker.Pool` and
  `scheduler.Scheduler`
- `internal/metrics`: Prometheus-compatible counters, histograms, and gauges
  for job outcomes/duration/in-flight count, schedule fires/errors/tick
  duration, and a pull-based queue depth gauge; wired into `Pool.Metrics`
  and `Scheduler.Metrics` as optional (nil-safe) fields, plus a scrape
  `Handler()` for exposing them

## Day 5–6 — HTTP API (done)
- `internal/api`: `Store` interface (`Enqueue`/`Get`/`List`/`Cancel`/`Stats`)
  satisfied by `queue/sqlite.Queue`, plus new `Queue.Get` and `Queue.Cancel`
  methods (and a `job.StatusCancelled` lifecycle state) added to support it
- REST endpoints: `POST /v1/jobs` (enqueue), `GET /v1/jobs/{id}` (status),
  `GET /v1/jobs?status=&limit=` (list), `POST /v1/jobs/{id}/cancel`,
  `GET /v1/stats`, `GET /healthz`, optional `GET /metrics`
- Middleware: static `X-API-Key` auth stub (off by default, always exempts
  `/healthz`), a hand-rolled per-client-IP token-bucket rate limiter, and
  request logging
- `cmd/batch-jobs-server`: standalone binary wiring the API to a SQLite
  queue, manually verified end to end with curl
- Handler unit tests (fake in-memory `Store`) plus sqlite-backed
  integration tests served over a real `httptest.Server`; OpenAPI spec at
  `docs/openapi.yaml`

## Day 7–8 — Dashboard (done)
- `internal/dashboard`: embedded static web UI (`go:embed`), no build step —
  stat tiles for the per-status job-count breakdown and a status-filterable
  job table
- `internal/queue/sqlite.Queue.Retry` and `POST /v1/jobs/{id}/retry`: resets
  a dead/failed job back to pending, clearing retry count and last error
- `GET /v1/events`: Server-Sent Events stream of live `/v1/stats` snapshots,
  powering the dashboard's stat tiles without polling; job table polls
  `/v1/jobs` every 5s and both paths expose cancel/retry actions
- `api.Server.Assets` (optional `fs.FS`) mounts static files at `/` behind
  the API's own routes; wired into `cmd/batch-jobs-server` by default
  (`-dashboard=false` to disable)
- Bug fix: logging middleware's `statusRecorder` now forwards `Flush()`, so
  streaming handlers work when wrapped by `withLogging`

## Day 9 — Reliability (done)
- `job.Timeout` / `job.WithTimeout`: `worker.Pool` derives a per-attempt
  `context.WithTimeout` from it, so a handler that respects `ctx.Done()` is
  actually interrupted rather than left to run indefinitely; persisted by
  `queue/sqlite.Queue` (migration 0002) and exposed as `timeout_seconds`
  over the HTTP API
- `worker.Permanent(err)`: lets a handler mark an error non-retryable,
  transitioning the job straight to `StatusFailed` and skipping both retry
  scheduling and dead-lettering — the first code path that actually
  produces `StatusFailed`, previously unreachable
- `job.IdempotencyKey` / `job.WithIdempotencyKey`: both `queue.Memory` and
  `queue/sqlite.Queue` dedupe a repeated `Enqueue` for the same `(Queue,
  IdempotencyKey)` pair into the original job rather than creating a
  duplicate — a unique index for `queue/sqlite.Queue` (durable, race-safe
  across concurrent writers), a waiting-jobs index scoped to not-yet-claimed
  jobs for `queue.Memory`; exposed as `idempotency_key` over the HTTP API,
  with `POST /v1/jobs` responding 200 (not 201) on a dedupe
- Graceful shutdown for `cmd/batch-jobs-server`: SIGINT/SIGTERM triggers
  `http.Server.Shutdown` with a configurable `-shutdown-timeout` (default
  10s) instead of the process dying mid-request

## Day 10 — Scaling (done)
- `queue.Router`: a `Queue` that fans `Enqueue` out by job queue name and
  fans `Dequeue` back in across its routes in weighted round-robin order,
  so one `worker.Pool` can serve several logical queues with tunable
  shares; `WithDefaultRoute` catch-all, `ErrNoRoute` otherwise
- `queue/sqlite.Queue.Bind(name)`: a view of the durable queue restricted
  to one logical queue, so every route can share a single database file,
  schema, and connection pool
- Dynamic pool sizing: `worker.Policy`/`worker.Scaler` (target = busy
  workers + backlog/JobsPerWorker, clamped to `[Min, Max]`; immediate
  growth, hysteretic one-at-a-time shrink) driven by an autoscaler
  goroutine that `Pool.Scale.Max` enables. Workers get a separate context
  for `Dequeue` so retiring one never cancels the job it is running
- Backpressure: `queue.WithCapacity` bounds `Memory` and returns
  `queue.ErrFull` (retries and dead letters bypass the check, since they
  were already admitted); `POST /v1/jobs` maps it to `503` +
  `Retry-After`
- `batchjobs_pool_workers` and `batchjobs_pool_scale_events_total`
  metrics; router fan-out/sweep benchmarks

## Day 11 — Persistence hardening (done)
- Job leases: `queue/sqlite` migration 0003 adds `owner` and
  `lease_expires_at`, stamped by the same UPDATE that claims a job, so
  there is no window where a job is running unleased. `Queue.Heartbeat`
  extends a claim; `Queue.Recover` reclaims claims whose lease lapsed
  (requeue, or dead-letter when retries are spent, so a worker-killing job
  cannot cycle forever); `sqlite.Reaper` runs that sweep on an interval and
  once at startup
- `worker.Pool` heartbeats any queue implementing `Heartbeater` while a
  handler runs, deriving its interval from the queue's TTL, and cancels the
  handler if the lease is lost mid-flight; `queue.Router` forwards
  heartbeats to whichever route holds the job and reports the shortest TTL
- Bug fix: terminal outcomes were never written back, so a finished job's
  row stayed `running` forever — a reporting problem before leases and a
  duplicate-execution one after. `queue.Completer` / `Queue.Complete` and
  `Pool.complete` close it
- `internal/sqlitemigrate` hardening: per-migration SHA-256 checksums with
  drift detection, duplicate-version and cross-set collision checks,
  concurrent-`Apply` safety (bookkeeping row written first), and read-only
  `Status`/`Verify`/`Version`
- `cmd/batch-jobs-migrate` (status/up/verify/version) plus
  `-migrate=false`, `-lease-ttl`, and `-reap-interval` on the server;
  `POST /v1/recover`; `batchjobs_jobs_recovered_total` and
  `batchjobs_expired_leases` metrics

## Day 12 — API polish
- Public API ergonomics, examples, godoc

## Day 13 — Performance
- Load testing, benchmarking, tuning

## Day 14 — Release
- Docs, CHANGELOG, v0.1.0 tag
