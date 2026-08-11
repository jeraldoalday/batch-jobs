// Package sqlitex holds the SQLite connection setup shared by every
// sqlite-backed store in this module (internal/queue/sqlite,
// internal/schedule/sqlite, ...), so each one doesn't have to re-derive the
// same pragma DSN and driver import.
package sqlitex

import (
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// PragmaDSN carries the connection settings every store in this module
// relies on for correctness under concurrent access: WAL so readers don't
// block the writer, a busy timeout so a writer contending with another
// connection retries instead of immediately failing with SQLITE_BUSY, and
// foreign keys on for completeness.
//
// These are passed as modernc.org/sqlite DSN query parameters rather than
// issued as PRAGMA statements after Open, because database/sql maintains a
// pool of connections opened lazily on demand: a one-off Exec only
// configures whichever single connection happens to service it, leaving
// every connection the pool opens afterwards unconfigured. DSN parameters
// are applied by the driver to every connection it opens.
const PragmaDSN = "_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on"

// WithPragmas appends PragmaDSN to path as query parameters, joining with
// "&" if path already has a query string (as the "file:name?mode=memory"
// URIs used in tests do) or "?" otherwise.
func WithPragmas(path string) string {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + PragmaDSN
}

// Open opens (creating if necessary) the SQLite database at path with
// PragmaDSN applied. Use ":memory:" for an ephemeral, process-local
// database useful in tests.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", WithPragmas(path))
	if err != nil {
		return nil, fmt.Errorf("sqlitex: open %s: %w", path, err)
	}
	return db, nil
}
