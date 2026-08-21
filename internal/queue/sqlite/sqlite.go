package sqlite

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jeraldoalday/batch-jobs/internal/sqlitex"
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
	leaseTTL     time.Duration
	owner        string
}

// Options configures Open.
type Options struct {
	// PollInterval overrides DefaultPollInterval.
	PollInterval time.Duration

	// LeaseTTL overrides DefaultLeaseTTL: how long a claimed job stays
	// claimed without a heartbeat before Recover may hand it to another
	// worker. A negative value disables leasing, so claims never expire
	// and a crashed worker's jobs stay 'running' until an operator
	// intervenes.
	LeaseTTL time.Duration

	// Owner overrides the identifier stamped on jobs this Queue claims,
	// which defaults to "hostname/pid". It is diagnostic only: recovery
	// keys on lease expiry, never on who holds the claim.
	Owner string
}

// Open opens (creating if necessary) the SQLite database at path, applies
// any pending schema migrations, and returns a ready-to-use Queue. Use
// ":memory:" for an ephemeral, process-local database useful in tests.
func Open(path string, opts ...Options) (*Queue, error) {
	db, err := sqlitex.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: migrate %s: %w", path, err)
	}

	pollInterval := DefaultPollInterval
	leaseTTL := DefaultLeaseTTL
	owner := defaultOwner()
	if len(opts) > 0 {
		if opts[0].PollInterval > 0 {
			pollInterval = opts[0].PollInterval
		}
		if opts[0].LeaseTTL != 0 {
			leaseTTL = opts[0].LeaseTTL
		}
		if opts[0].Owner != "" {
			owner = opts[0].Owner
		}
	}

	return &Queue{
		db:           db,
		changed:      make(chan struct{}),
		pollInterval: pollInterval,
		leaseTTL:     leaseTTL,
		owner:        owner,
	}, nil
}

// LeaseTTL returns how long claims made by this Queue are good for without
// a heartbeat, or 0 if leasing is disabled. Workers use it to pick a
// heartbeat interval comfortably shorter than the expiry.
func (q *Queue) LeaseTTL() time.Duration {
	if q.leaseTTL <= 0 {
		return 0
	}
	return q.leaseTTL
}

// Owner returns the identifier this Queue stamps on the jobs it claims.
func (q *Queue) Owner() string { return q.owner }

// notifyLocked wakes any goroutine blocked in Dequeue waiting on the
// current changed channel. Callers must hold q.mu.
func (q *Queue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}
