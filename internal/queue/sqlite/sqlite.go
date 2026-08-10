package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// DefaultPollInterval bounds how long Dequeue can go between checks of the
// database when it has no local job to wait on. It exists so a job written
// by another process sharing this database file (rather than through this
// Queue's own Enqueue) is still noticed in a bounded amount of time.
const DefaultPollInterval = 500 * time.Millisecond

// Queue is a SQLite-backed, durable implementation of queue.Queue. Jobs
// survive process restarts; multiple Queue instances (in the same process
// or different ones) may safely share the same database file.
type Queue struct {
	db *sql.DB

	mu      sync.Mutex
	closed  bool
	changed chan struct{}

	pollInterval time.Duration
}

// Options configures Open.
type Options struct {
	// PollInterval overrides DefaultPollInterval.
	PollInterval time.Duration
}

// Open opens (creating if necessary) the SQLite database at path, applies
// any pending schema migrations, and returns a ready-to-use Queue. Use
// ":memory:" for an ephemeral, process-local database useful in tests.
func Open(path string, opts ...Options) (*Queue, error) {
	db, err := sql.Open("sqlite", withPragmas(path))
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: migrate %s: %w", path, err)
	}

	pollInterval := DefaultPollInterval
	if len(opts) > 0 && opts[0].PollInterval > 0 {
		pollInterval = opts[0].PollInterval
	}

	return &Queue{
		db:           db,
		changed:      make(chan struct{}),
		pollInterval: pollInterval,
	}, nil
}

// pragmaDSN carries the connection settings the Queue relies on for
// correctness under concurrent access: WAL so readers don't block the
// writer, a busy timeout so a writer contending with another connection
// retries instead of immediately failing with SQLITE_BUSY, and foreign
// keys on for completeness.
//
// These are passed as modernc.org/sqlite DSN query parameters rather than
// issued as PRAGMA statements after Open, because database/sql maintains a
// pool of connections opened lazily on demand: a one-off Exec only
// configures whichever single connection happens to service it, leaving
// every connection the pool opens afterwards unconfigured. DSN parameters
// are applied by the driver to every connection it opens.
const pragmaDSN = "_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on"

// withPragmas appends pragmaDSN to path as query parameters, joining with
// "&" if path already has a query string (as the "file:name?mode=memory"
// URIs used in tests do) or "?" otherwise.
func withPragmas(path string) string {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + pragmaDSN
}

// notifyLocked wakes any goroutine blocked in Dequeue waiting on the
// current changed channel. Callers must hold q.mu.
func (q *Queue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}
