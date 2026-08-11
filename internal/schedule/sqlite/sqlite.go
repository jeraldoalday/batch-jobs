// Package sqlite implements schedule.Store on top of a SQLite database
// file, giving recurring schedules the same durability across process
// restarts that internal/queue/sqlite gives jobs.
package sqlite

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/jeraldoalday/batch-jobs/internal/sqlitemigrate"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitex"
)

// migrationsFS embeds the numbered .sql migration files applied by Open to
// bring a database up to the current schema.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store is a SQLite-backed, durable implementation of schedule.Store.
// Multiple Store instances (in the same process or different ones) may
// safely share the same database file.
type Store struct {
	db *sql.DB
}

// Open opens (creating if necessary) the SQLite database at path, applies
// any pending schema migrations, and returns a ready-to-use Store. Use
// ":memory:" for an ephemeral, process-local database useful in tests.
func Open(path string) (*Store, error) {
	db, err := sqlitex.Open(path)
	if err != nil {
		return nil, fmt.Errorf("schedule/sqlite: open %s: %w", path, err)
	}

	if err := sqlitemigrate.Apply(db, migrationsFS, "migrations"); err != nil {
		db.Close()
		return nil, fmt.Errorf("schedule/sqlite: migrate %s: %w", path, err)
	}

	return &Store{db: db}, nil
}

// Close implements schedule.Store.
func (s *Store) Close() error {
	return s.db.Close()
}
