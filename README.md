# batch-jobs

A self-hosted, embeddable job queue and task scheduler for Go.

`batch-jobs` lets a Go application enqueue units of work and process them with
a pool of workers, with support for retries, priorities, delayed/scheduled
execution, and persistence. It is designed to be embedded directly into a
service (no external broker required) while still scaling to a durable,
disk-backed queue when needed.

## Status

Early development. The core in-memory queue and job model are in place;
persistence, scheduling, the HTTP API, and the dashboard are being built out
incrementally. See commit history for progress.

## Goals

- Small, dependency-light core
- Pluggable storage backends (in-memory now, SQLite next)
- At-least-once delivery with configurable retry/backoff
- Observable: structured logs, metrics, and a simple dashboard

## License

MIT — see [LICENSE](./LICENSE).
