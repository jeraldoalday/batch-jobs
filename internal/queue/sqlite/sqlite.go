package sqlite

import (
	"database/sql"
	"fmt"
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
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}

	if err := configure(db); err != nil {
		db.Close()
		return nil, err
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

// configure applies the PRAGMAs the Queue relies on for correctness under
// concurrent access: WAL so readers don't block the writer, and a busy
// timeout so a writer contending with another connection retries instead
// of immediately failing with SQLITE_BUSY.
func configure(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("sqlite: %s: %w", p, err)
		}
	}
	return nil
}

// notifyLocked wakes any goroutine blocked in Dequeue waiting on the
// current changed channel. Callers must hold q.mu.
func (q *Queue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}
