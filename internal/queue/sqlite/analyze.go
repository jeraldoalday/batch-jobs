package sqlite

import (
	"fmt"
	"time"
)

// DefaultOptimizeInterval is how often maintained statistics are refreshed
// when a Reaper is running. It is deliberately much longer than the reap
// interval: what the planner needs is a rough idea of the backlog's shape,
// which changes over minutes, not the exact row counts of a moment ago.
const DefaultOptimizeInterval = 5 * time.Minute

// Optimize refreshes the table statistics SQLite's query planner uses to
// choose between the two dequeue indexes.
//
// Those two indexes suit opposite backlogs. idx_jobs_dequeue_order can
// hand back ready jobs already in priority order, which is enormously
// faster when most waiting jobs are runnable now -- a claim against a
// 50,000-job ready backlog drops from about 14ms to under 100us. But when
// the queue is mostly *delayed* jobs and the ready ones are a thin slice,
// idx_jobs_dequeue's run_at range is the more selective way in, and
// walking priority order looking for the few ready rows would be the
// slower plan. There is no static answer: it depends on data the schema
// cannot know.
//
// SQLite decides per query, correctly, as long as it has statistics. With
// none it falls back on generic guesses and picks the wrong index for the
// common case. PRAGMA optimize is the maintained form of ANALYZE: it
// re-analyzes only the tables whose contents have moved enough since last
// time to matter, so calling it on a schedule costs nothing when nothing
// has changed.
//
// Optimize is safe to call from several processes at once and while
// workers are running. It is idempotent, and a failure is not worth
// escalating: stale statistics make queries slower, not wrong.
func (q *Queue) Optimize() error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return nil
	}

	if _, err := q.db.Exec(optimizePragma); err != nil {
		return fmt.Errorf("sqlite: optimize: %w", err)
	}
	return nil
}

// optimizePragma is the exact form of PRAGMA optimize this package needs,
// and both of its details are load-bearing.
//
// The 0x10000 bit asks SQLite to consider every table rather than only
// those the *current connection* has touched. Without it the pragma is
// effectively a no-op here: database/sql hands out pooled connections, so
// the connection that services this call is rarely one that has written
// enough jobs to trip the heuristic, and the jobs table simply never gets
// analyzed. That was the difference between the dequeue plan being fixed
// and appearing to be fixed.
//
// No analysis_limit is set, deliberately. SQLite's usual advice is to cap
// analysis at a few hundred rows per index to keep it quick, but sampling
// that shallowly cannot tell these two dequeue indexes apart -- measured
// at 400, 2000, and 10000 rows, the planner still chose the slow one, and
// only a full analysis produced the fast plan. The cost is roughly a
// millisecond per thousand jobs, paid on a background schedule, against a
// dequeue that is otherwise proportional to the entire backlog every time
// a worker asks for work.
const optimizePragma = "PRAGMA optimize(0x10002)"

// analyzeOnOpen seeds statistics for a database that already holds jobs.
//
// PRAGMA optimize will not analyze a table it has no record of having
// changed, and a freshly opened connection has no such record -- so a
// process starting against an existing, populated queue would run on
// generic guesses until enough jobs passed through it to trip the
// heuristic. That is exactly the wrong moment to have the slow plan: a
// restart after a crash faces the largest backlog the queue ever has.
// Forcing one analysis at open, only when there is something to analyze,
// closes that window.
func analyzeOnOpen(q *Queue) error {
	var rows int
	// A cheap bounded existence check: counting the whole table would
	// itself be proportional to a backlog that might be large.
	if err := q.db.QueryRow("SELECT EXISTS (SELECT 1 FROM jobs LIMIT 1)").Scan(&rows); err != nil {
		return fmt.Errorf("sqlite: analyze on open: %w", err)
	}
	if rows == 0 {
		return nil
	}
	if err := q.Optimize(); err != nil {
		return fmt.Errorf("sqlite: analyze on open: %w", err)
	}
	return nil
}
