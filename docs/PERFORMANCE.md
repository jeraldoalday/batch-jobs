# Performance

Everything below was measured on an Apple M1 Pro (8 cores, laptop SSD)
with Go 1.26 and `modernc.org/sqlite` v1.56. Absolute numbers will differ
on your hardware; the ratios and the *shapes* are the part worth carrying
across.

## How to measure

Two tools, answering different questions.

```sh
make bench          # microbenchmarks: one operation, in isolation
make load           # a load test: producers and workers, together
```

A microbenchmark tells you what one call costs on an idle machine. That
is the right question for "did this change make enqueue slower?" and the
wrong one for "will this hold 2,000 jobs a second?" — because the answer
to the second is decided by queueing, lock contention, and the tail of
the distribution, none of which appear when one operation runs alone.

Compare benchmark runs with `benchstat` rather than reading a single run:

```sh
make bench > before.txt
# ... change something ...
make bench > after.txt
benchstat before.txt after.txt
```

For load, `batch-jobs-bench` runs producers and workers against a real
database file:

```sh
# The producer side's ceiling: nothing consumes the queue.
go run ./cmd/batch-jobs-bench -jobs 20000 -workers 0

# 500 jobs/s for a minute through 8 workers, each job taking 5ms.
go run ./cmd/batch-jobs-bench -rate 500 -duration 1m -workers 8 -work 5ms

# The same load, submitted in batches of 100.
go run ./cmd/batch-jobs-bench -rate 500 -duration 1m -workers 8 -work 5ms -batch 100
```

### Reading the report

It prints two distributions, and their divergence is the diagnosis.

- **enqueue latency** — how long a producer waited for the queue to accept
  a job.
- **end-to-end latency** — how long a job took from being created to being
  finished, measured inside the handler. This includes every second the
  job spent sitting in the backlog.

A system that is keeping up has an end-to-end distribution close to the
handler's own duration:

```
  latency      min 91.58µs  mean 234.68µs  max 8.52ms
               p50     192.51µs
               p99     1.51ms

  end-to-end   min 5.27ms  mean 5.78ms  max 13.91ms
               p50     5.9ms
               p99     7.08ms
```

That is 400 jobs/s through 8 workers with a 5ms handler: end-to-end p50 of
5.9ms against 5ms of actual work, so jobs are barely waiting.

A system that is not keeping up looks like this — same tool, 2,000 jobs/s
through 4 workers with a 20ms handler:

```
  latency      p50 200.7µs   p99 18.87ms
  end-to-end   p50 3.83s     p99 7.78s

  NOTE: 9529 jobs were still waiting when the run ended.
```

Note what enqueue latency does: almost nothing. It stays in the hundreds
of microseconds throughout, because accepting a job is still fast — the
queue is doing its job perfectly, and the *arrival rate exceeds the
service rate*. Only the end-to-end number shows it, and it will keep
growing for as long as the run lasts, because it is measuring a backlog
rather than an operation. This is why a producer-side measurement alone
can be actively misleading.

### Coordinated omission

The load generator paces open-loop by default: each job is pinned to an
intended send time, and its latency is measured from *that* time rather
than from when a producer actually got to it.

This matters more than it sounds. If you measure from the actual send, a
system that has fallen behind reports a comfortable latency — the queueing
has moved into your generator, where you are not timing it. The report
prints a **send lag** distribution alongside, so the two failures are
distinguishable:

| enqueue latency | send lag | meaning |
|---|---|---|
| high | low | the system is genuinely slow |
| high | high | the generator could not offer the load; the numbers describe a lighter load than you asked for |

A run that missed its target rate by more than 5% says so in the report,
because that fact invalidates every latency printed above it.

## What the tuning did

Day 13's changes, with the measurements that motivated each.

### Dequeue no longer scales with the backlog

This was the big one. Claiming a job cost time proportional to *every job
waiting*:

| ready backlog | before | after |
|---|---|---|
| 1,000 | 450µs | 238µs |
| 10,000 | 2.9ms | 236µs |
| 50,000 | 13.7ms | 245µs |
| 50,000, bound to one queue | 8.7ms | 241µs |

Every worker runs this query in a loop, so the queue got slower exactly as
it filled up — the worst possible shape for a backlog to have.

Two causes. `idx_jobs_dequeue` leads with `status` then `run_at`, so it
narrows the claim to ready jobs but cannot produce them *in priority
order*: `run_at` is an inequality sitting between the status equality and
the `ORDER BY` columns, which cuts off the index's usable sort. SQLite
therefore materialised every ready job into a temporary B-tree and sorted
it, to take the single row `LIMIT 1` asked for.

Migration `0004` adds `idx_jobs_dequeue_order`, which puts the sort columns
directly after the status equality. That index is still unusable while the
query says `status IN ('pending', 'retrying')`, because two disjoint index
ranges cannot be walked as one sorted stream — so the claim now takes each
status's best row from its own branch and picks between the two winners.
The remaining sort is over two rows.

### Statistics decide which index

The two dequeue indexes suit opposite backlogs: the new one wins when most
waiting jobs are runnable now, `idx_jobs_dequeue` when the queue is mostly
delayed jobs and the ready ones are a thin slice. SQLite chooses correctly
per query *given table statistics*, and chooses the slow one without them.

`Queue.Optimize` maintains them — seeded at `Open` for a populated
database, refreshed every five minutes by the reaper. Two details are
load-bearing:

- The pragma is `PRAGMA optimize(0x10002)`, not plain `PRAGMA optimize`.
  The `0x10000` bit makes SQLite consider every table rather than only
  those the *current connection* has touched. Without it the call is
  effectively a no-op, since `database/sql` hands out pooled connections
  and the one servicing the call has rarely written enough rows to trip
  the heuristic.
- No `analysis_limit` is set. The usual advice is to cap analysis at a few
  hundred rows per index to keep it quick; measured at 400, 2,000, and
  10,000 rows, the planner still chose the slow index. Only a full
  analysis produced the fast plan. It costs roughly a millisecond per
  thousand jobs, on a background schedule.

### Idempotency lookups use their index

`idx_jobs_idempotency` is a *partial* index (`WHERE idempotency_key != ''`,
so the many jobs opting out of dedupe do not all collide on the empty
string). SQLite only uses a partial index for a query whose own `WHERE`
clause provably implies the index's — and `idempotency_key = ?` against a
bound parameter does not, because the parameter could be `''`. The planner
fell back to a full table scan on every idempotent enqueue.

Restating the predicate in the query costs nothing and fixed it: 220µs to
48µs per job inside a batch, and unlike the benchmark suggests, the real
gain grows with the table, since a scan was proportional to it.

### Connection settings

| setting | was | now | why |
|---|---|---|---|
| `synchronous` | `FULL` | `NORMAL` | 4× on writes (110µs → 25µs for a bare INSERT). In WAL mode a crashing *process* still loses nothing; `NORMAL` risks only the last few committed transactions in a power cut or kernel panic. A job queue's promise is "the worker died", not "the datacenter did". |
| page cache | 2MB | 8MB | A queue with a few hundred thousand rows spilled out of cache on every index scan. |
| pool | unbounded, 2 idle | 4 open, 4 idle | SQLite serialises writers, so a bigger pool buys no write throughput — it converts waiting inside the driver into `SQLITE_BUSY` between connections. Raising the idle cap to match stops the pool re-opening the file and re-running every pragma on each burst. |

Set `Options.Synchronous = SynchronousFull` if losing the last few
committed transactions to a power cut is unacceptable, and expect writes
to cost several times as much.

Round trip, enqueue plus dequeue: 325µs → 227µs. Concurrent: 452µs → 215µs.

### Batch enqueue

A durable enqueue's cost is dominated by the transaction around the write,
not the row:

| batch size | per job |
|---|---|
| 1 (`Enqueue`) | 83µs |
| 10 | 38µs |
| 100 | 31µs |
| 1,000 | 28µs |

`Engine.EnqueueBatch` submits many jobs in one transaction, atomically.
Idempotency keys work across a batch and within one, so a key already on
file comes back describing the original — including when the duplicate is
another entry in the same call. Because the batch is all-or-nothing, a
caller that gets an error can retry the whole thing without working out
which half went through.

## Tuning your own deployment

Roughly in the order worth trying.

1. **Batch your producers.** If jobs arrive in groups — a fan-out, an
   import, a schedule expansion — `EnqueueBatch` is a 3× cut in per-job
   cost for no change in semantics.
2. **Size the pool to the work, not the machine.** Workers spend their
   time in your handler, not in SQLite. If handlers are IO-bound, more
   workers than cores is right; if CPU-bound, it is not. Autoscaling
   (`Config.MinWorkers` / `MaxWorkers`) tracks the backlog for you.
3. **Watch the end-to-end distribution, not the enqueue one.** The queue
   accepting jobs quickly says nothing about whether they are being run.
   `batchjobs_job_duration_seconds` and the queue-depth gauge are the two
   to alarm on.
4. **Leave the statistics maintenance on.** A process that disables the
   reaper (`ReapInterval` negative) also disables the statistics refresh,
   and a long-running queue whose backlog shape drifts will eventually be
   planned against stale numbers.
5. **Only then reach for `synchronous`.** `SynchronousOff` is faster
   again and can corrupt the database on a power cut. It is for benchmarks
   and throwaway databases, not for production.
