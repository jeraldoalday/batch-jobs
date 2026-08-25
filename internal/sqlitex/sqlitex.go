// Package sqlitex holds the SQLite connection setup shared by every
// sqlite-backed store in this module (internal/queue/sqlite,
// internal/schedule/sqlite, ...), so each one doesn't have to re-derive the
// same pragma DSN, pool sizing, and driver import.
package sqlitex

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"modernc.org/sqlite"
)

// Synchronous levels, as accepted by Options.Synchronous.
const (
	// SynchronousFull fsyncs the write-ahead log at every commit. It
	// survives an operating-system crash or a power cut with no
	// transaction loss, and costs roughly four times as much per write as
	// SynchronousNormal on a laptop SSD.
	SynchronousFull = "FULL"

	// SynchronousNormal fsyncs only at WAL checkpoints. A crashing
	// *process* still loses nothing -- committed transactions are in the
	// WAL, and another process or a restart reads them back -- so it is
	// the right default for a job queue, whose whole durability story is
	// "the worker died, not the machine". A power cut or kernel panic can
	// lose the most recently committed transactions, which for a queue
	// means a few jobs enqueued in the last instant.
	SynchronousNormal = "NORMAL"

	// SynchronousOff never fsyncs and can corrupt the database on a power
	// cut. It exists for benchmarks and throwaway test databases.
	SynchronousOff = "OFF"
)

// Defaults for Options. They are exported so tooling and tests can state
// what they are deviating from.
const (
	// DefaultBusyTimeout is how long a connection contending for the
	// write lock retries before giving up with SQLITE_BUSY. SQLite allows
	// exactly one writer at a time, so under any real concurrency some
	// waiting is normal rather than exceptional.
	DefaultBusyTimeout = 5 * time.Second

	// DefaultCacheSizeKB is the per-connection page cache. SQLite's own
	// default is 2MB, which a queue with a few hundred thousand rows
	// spills out of on every index scan; 8MB holds the hot part of the
	// dequeue index in memory for the price of a rounding error in RSS.
	DefaultCacheSizeKB = 8 * 1024

	// DefaultMaxOpenConns bounds the connection pool. It is deliberately
	// small: SQLite serializes writers, so a larger pool does not buy
	// write throughput, it just converts waiting inside the driver into
	// SQLITE_BUSY retries between connections. Four leaves room for
	// concurrent readers (WAL does not block them) without that.
	DefaultMaxOpenConns = 4
)

// Options tunes the connections Open hands out. The zero value is the
// configuration every store in this module wants; each field documents
// what deviating from it costs.
type Options struct {
	// BusyTimeout overrides DefaultBusyTimeout.
	BusyTimeout time.Duration

	// Synchronous overrides SynchronousNormal. Use SynchronousFull when
	// losing the last few committed transactions to a power cut is not
	// acceptable, and expect writes to cost several times as much.
	Synchronous string

	// CacheSizeKB overrides DefaultCacheSizeKB. It is per connection, so
	// the process-wide cost is this times MaxOpenConns.
	CacheSizeKB int

	// MaxOpenConns overrides DefaultMaxOpenConns. A negative value leaves
	// the pool unbounded, which is almost never what you want against
	// SQLite.
	MaxOpenConns int

	// ConnectRetry overrides DefaultConnectRetry: how long opening a
	// single connection retries a SQLITE_BUSY before giving up. See
	// retryConnector for what that covers and why the busy timeout does
	// not.
	ConnectRetry time.Duration
}

// resolve fills in the defaults, so the rest of the package can work with
// a fully-specified Options.
func (o Options) resolve() Options {
	if o.BusyTimeout <= 0 {
		o.BusyTimeout = DefaultBusyTimeout
	}
	if o.Synchronous == "" {
		o.Synchronous = SynchronousNormal
	}
	if o.CacheSizeKB <= 0 {
		o.CacheSizeKB = DefaultCacheSizeKB
	}
	if o.MaxOpenConns == 0 {
		o.MaxOpenConns = DefaultMaxOpenConns
	}
	if o.ConnectRetry <= 0 {
		o.ConnectRetry = DefaultConnectRetry
	}
	return o
}

// DSN returns the query parameters o asks the driver for, in a stable
// order.
//
// These are passed as modernc.org/sqlite DSN query parameters rather than
// issued as PRAGMA statements after Open, because database/sql maintains a
// pool of connections opened lazily on demand: a one-off Exec only
// configures whichever single connection happens to service it, leaving
// every connection the pool opens afterwards unconfigured. DSN parameters
// are applied by the driver to every connection it opens.
func (o Options) DSN() string {
	o = o.resolve()
	params := url.Values{}
	// WAL so readers never block the writer, and a busy timeout so a
	// writer contending with another connection retries rather than
	// failing immediately.
	params.Set("_journal_mode", "WAL")
	params.Set("_busy_timeout", strconv.FormatInt(o.BusyTimeout.Milliseconds(), 10))
	params.Set("_foreign_keys", "on")
	params.Set("_synchronous", o.Synchronous)
	// A negative cache_size is read by SQLite as a size in kibibytes
	// rather than a count of pages, which is the unit worth configuring:
	// it does not silently change meaning if the page size does.
	params.Set("_pragma", "cache_size(-"+strconv.Itoa(o.CacheSizeKB)+")")
	return params.Encode()
}

// PragmaDSN is the default connection settings, kept as a constant-shaped
// value for callers that build their own DSN string.
var PragmaDSN = Options{}.DSN()

// WithPragmas appends the default settings to path as query parameters,
// joining with "&" if path already has a query string (as the
// "file:name?mode=memory" URIs used in tests do) or "?" otherwise.
func WithPragmas(path string) string {
	return Options{}.apply(path)
}

// apply appends o's DSN parameters to path.
func (o Options) apply(path string) string {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + o.DSN()
}

// Open opens (creating if necessary) the SQLite database at path with the
// default Options applied. Use ":memory:" for an ephemeral, process-local
// database useful in tests.
func Open(path string) (*sql.DB, error) {
	return OpenWith(path, Options{})
}

// OpenWith is Open with explicit tuning.
func OpenWith(path string, opts Options) (*sql.DB, error) {
	opts = opts.resolve()

	connector, err := sqlite.NewConnector(opts.apply(path))
	if err != nil {
		return nil, fmt.Errorf("sqlitex: open %s: %w", path, err)
	}
	db := sql.OpenDB(&retryConnector{inner: connector, timeout: opts.ConnectRetry})

	if opts.MaxOpenConns > 0 {
		db.SetMaxOpenConns(opts.MaxOpenConns)
		// Idle connections are capped at two by default, so a pool that
		// is allowed more than two open connections spends the surplus
		// re-opening the database file and re-running every pragma above
		// on each burst of concurrency. Matching the idle cap to the open
		// cap makes the pool stable once it has warmed up.
		db.SetMaxIdleConns(opts.MaxOpenConns)
	}
	// Connections are never retired on age. A SQLite connection is a file
	// handle, not a network socket: there is no idle timeout on the far
	// side to race, and recycling one only pays the pragma setup again.
	db.SetConnMaxLifetime(0)

	return db, nil
}
