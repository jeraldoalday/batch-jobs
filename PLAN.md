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

## Day 4 — Observability
- Structured logging (`slog`)
- Metrics hooks (Prometheus-compatible)

## Day 5–6 — HTTP API
- REST endpoints: enqueue, status, list/cancel
- Middleware (auth stub, rate limiting)
- Integration tests, OpenAPI spec

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
