package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/job"
	"github.com/jeraldoalday/batch-jobs/internal/queue"
)

var _ queue.BatchEnqueuer = (*Queue)(nil)

// EnqueueBatch implements queue.BatchEnqueuer: it admits every job in one
// transaction, so a producer with a fan-out in hand pays for one commit
// rather than one per job. On a durable backend that is most of the cost,
// since a commit is a WAL write and (at higher synchronous levels) an
// fsync, while the row itself is a few hundred bytes.
//
// Either all of the jobs are admitted or none are. Each *job.Job is
// updated in place exactly as Enqueue updates it, so a job that deduped
// against an existing idempotency key comes back describing the original.
func (q *Queue) EnqueueBatch(jobs []*job.Job) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return queue.ErrClosed
	}

	if len(jobs) == 0 {
		return nil
	}
	for i, j := range jobs {
		if j == nil {
			return fmt.Errorf("sqlite: enqueue batch: job %d is nil", i)
		}
	}

	if err := q.runBatch(jobs); err != nil {
		return err
	}

	q.mu.Lock()
	q.notifyLocked()
	q.mu.Unlock()
	return nil
}

// runBatch performs the inserts inside one write transaction.
//
// The transaction is opened with an explicit BEGIN IMMEDIATE on a
// dedicated connection rather than with db.Begin, which issues a deferred
// BEGIN. The difference matters here because the batch reads before it
// writes: a job carrying an idempotency key is looked up first so a repeat
// can adopt the original. Under a deferred transaction that read takes a
// shared lock and the following insert has to upgrade it, and SQLite
// refuses that upgrade outright -- without consulting the busy timeout --
// if another connection took the write lock in between. Taking the write
// lock up front turns that failure into ordinary waiting, and as a bonus
// makes the read-then-write pair genuinely atomic: no other writer can
// slip a conflicting idempotency key in between.
func (q *Queue) runBatch(jobs []*job.Job) error {
	ctx := context.Background()

	conn, err := q.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sqlite: enqueue batch: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("sqlite: enqueue batch: begin: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			// Best-effort: the connection is about to be returned to the
			// pool, and returning it inside an open transaction would
			// hand the next caller a connection holding the write lock.
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	insert, err := conn.PrepareContext(ctx, upsertQuery)
	if err != nil {
		return fmt.Errorf("sqlite: enqueue batch: prepare insert: %w", err)
	}
	defer insert.Close()

	// The lookup statement is prepared lazily: most batches carry no
	// idempotency keys at all, and preparing a statement they will never
	// use is pure overhead on the common path.
	var lookup *sql.Stmt
	defer func() {
		if lookup != nil {
			lookup.Close()
		}
	}()

	for i, j := range jobs {
		if j.IdempotencyKey != "" {
			if lookup == nil {
				if lookup, err = conn.PrepareContext(ctx, idempotentMatchQuery); err != nil {
					return fmt.Errorf("sqlite: enqueue batch: prepare lookup: %w", err)
				}
			}
			// Reads inside the transaction see rows inserted earlier in
			// the same batch, so two jobs sharing a key within one call
			// dedupe against each other exactly as they would across two.
			existing, err := scanJob(lookup.QueryRowContext(ctx, j.Queue, j.IdempotencyKey))
			if err == nil {
				*j = *existing
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("sqlite: enqueue batch: job %d (%s): %w", i, j.ID, err)
			}
		}

		if _, err := insert.ExecContext(ctx, insertArgs(j)...); err != nil {
			return fmt.Errorf("sqlite: enqueue batch: job %d (%s): %w", i, j.ID, err)
		}
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("sqlite: enqueue batch: commit: %w", err)
	}
	committed = true
	return nil
}
