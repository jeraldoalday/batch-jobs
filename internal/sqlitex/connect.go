package sqlitex

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"modernc.org/sqlite"
)

// sqliteBusy is SQLITE_BUSY, the result code SQLite returns when it cannot
// take a lock it needs. It is spelled out rather than imported because the
// constant lives in the driver's internal C-translation package.
const sqliteBusy = 5

// DefaultConnectRetry bounds how long opening a single connection retries
// a SQLITE_BUSY before giving up. It is generous because the situation it
// covers is short-lived by nature: the loser only has to wait for whoever
// is doing the one-time journal-mode transition to finish it.
const DefaultConnectRetry = 5 * time.Second

// retryConnector wraps the driver's connector so that establishing a
// connection retries when SQLite reports the database as busy.
//
// This exists because of a race that the busy timeout does not cover.
// Applying "PRAGMA journal_mode = WAL" to a database that is not yet in
// WAL mode needs a brief exclusive lock on the file, and SQLite does not
// run the busy handler for that acquisition: it fails immediately with
// SQLITE_BUSY. Every connection this package opens applies that pragma, so
// two processes opening the same fresh database file at the same instant
// -- a server and its migrate step, two workers starting together, a test
// spinning up four pools at once -- could have one of them fail outright
// on a database that was merely a few microseconds from being ready.
//
// Retrying at connection level rather than warming a single connection at
// Open covers the whole pool: database/sql opens connections lazily, so a
// second or third connection can meet the same transition long after Open
// returned. Once the file is in WAL mode the pragma is a no-op that takes
// no exclusive lock, so this costs nothing in the steady state.
type retryConnector struct {
	inner   driver.Connector
	timeout time.Duration
}

// Connect implements driver.Connector.
func (c *retryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	deadline := time.Now().Add(c.timeout)

	// Start well below the expected wait and back off, so the common case
	// (a transition already almost finished) is not made to wait a fixed
	// slot, while a genuinely locked database is not spun on.
	delay := 250 * time.Microsecond
	const maxDelay = 25 * time.Millisecond

	for {
		conn, err := c.inner.Connect(ctx)
		if err == nil || !isBusy(err) {
			return conn, err
		}
		if time.Now().After(deadline) {
			return nil, err
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if delay *= 2; delay > maxDelay {
			delay = maxDelay
		}
	}
}

// Driver implements driver.Connector.
func (c *retryConnector) Driver() driver.Driver { return c.inner.Driver() }

// isBusy reports whether err is SQLITE_BUSY, including the extended codes
// (SQLITE_BUSY_SNAPSHOT and friends) that carry it in their low byte.
func isBusy(err error) bool {
	var serr *sqlite.Error
	if !errors.As(err, &serr) {
		return false
	}
	return serr.Code()&0xff == sqliteBusy
}
