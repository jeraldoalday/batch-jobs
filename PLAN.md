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

## Day 7–8 — Dashboard
- Minimal web UI: job/queue views
- Live updates (SSE/polling), retry/cancel actions

## Day 9 — Reliability
- Graceful shutdown
- Job timeouts / context cancellation
- Idempotency keys

## Day 10 — Scaling
- Multi-queue/priority routing
- Dynamic worker pool sizing, backpressure

## Day 11 — Persistence hardening
- Crash recovery tests
- Migration tooling

## Day 12 — API polish
- Public API ergonomics, examples, godoc

## Day 13 — Performance
- Load testing, benchmarking, tuning

## Day 14 — Release
- Docs, CHANGELOG, v0.1.0 tag
