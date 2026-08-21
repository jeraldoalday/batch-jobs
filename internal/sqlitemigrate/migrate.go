// Package sqlitemigrate applies embedded, numbered .sql migration files to
// a SQLite database, tracking which have already run so Apply is safe to
// call every time a store is opened. It is shared by every sqlite-backed
// store in this module so each one only has to embed its own migrations/
// directory and call Apply.
package sqlitemigrate

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrDuplicateVersion is returned by Apply when two migration files in the
// same directory share a version number. Applying both would leave the
// schema in an order-dependent state and only one of them could ever be
// recorded in schema_migrations, so it is rejected up front rather than
// half-applied.
var ErrDuplicateVersion = errors.New("sqlitemigrate: duplicate migration version")

// ErrVersionNameMismatch is returned when a version recorded in the
// database was applied from a differently named file than the one the
// binary holds. The usual cause is two migration sets -- the jobs schema
// and the schedules schema, say -- pointed at the same database file,
// where they share one schema_migrations table and silently claim each
// other's version numbers.
var ErrVersionNameMismatch = errors.New("sqlitemigrate: migration version applied under a different name")

// migrationsTable tracks which migrations have already been applied so
// Apply is safe to run every time a store is opened.
const migrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);`

// migration is one parsed, embedded .sql file.
type migration struct {
	version int
	name    string
	sql     string
}

// load reads every *.sql file in dir (within fsys) and orders them by the
// numeric prefix in their filename (e.g. "0001_init.sql" -> version 1).
func load(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}

	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		version, err := versionFromFilename(entry.Name())
		if err != nil {
			return nil, err
		}

		contents, err := fs.ReadFile(fsys, path.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}

		migrations = append(migrations, migration{
			version: version,
			name:    entry.Name(),
			sql:     string(contents),
		})
	}

	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })

	if err := checkDuplicates(migrations); err != nil {
		return nil, err
	}
	return migrations, nil
}

// checkDuplicates reports an error if two migrations in the (already
// sorted) slice share a version number. A directory holding both
// "0003_leases.sql" and "0003_owners.sql" is a merge accident: whichever
// one loses the race is silently skipped forever afterwards, because
// schema_migrations only records the version.
func checkDuplicates(migrations []migration) error {
	for i := 1; i < len(migrations); i++ {
		if migrations[i].version == migrations[i-1].version {
			return fmt.Errorf("%w: %d shared by %s and %s",
				ErrDuplicateVersion, migrations[i].version, migrations[i-1].name, migrations[i].name)
		}
	}
	return nil
}

// versionFromFilename extracts the leading integer from a migration
// filename, e.g. "0001_init.sql" -> 1.
func versionFromFilename(name string) (int, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("migration %q missing '_' separator between version and name", name)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("migration %q has non-numeric version prefix: %w", name, err)
	}
	return version, nil
}

// Apply applies every *.sql file in dir (within fsys) that has not already
// been recorded in schema_migrations, each inside its own transaction, in
// ascending version order.
//
// Migrations that were already applied are checked against the checksum
// recorded at the time: if one of their files has since been edited, Apply
// fails with ErrChecksumMismatch rather than carrying on against a schema
// it can no longer describe.
func Apply(db *sql.DB, fsys fs.FS, dir string) error {
	if err := ensureTable(db); err != nil {
		return err
	}

	applied, err := appliedRows(db)
	if err != nil {
		return err
	}

	migrations, err := load(fsys, dir)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if recorded, ok := applied[m.version]; ok {
			if err := checkName(m, recorded); err != nil {
				return err
			}
			if err := verifyChecksum(db, m, recorded.checksum); err != nil {
				return err
			}
			continue
		}
		if err := applyOne(db, m); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
	}
	return nil
}

// ensureTable creates schema_migrations if needed and brings an
// older database's copy of it up to the current shape.
func ensureTable(db *sql.DB) error {
	if _, err := db.Exec(migrationsTable); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return ensureChecksumColumn(db)
}

// checkName reports whether a recorded version was applied from the file
// the binary is now holding. Catching this before the checksum comparison
// matters because the checksum error ("this file changed") sends an
// operator looking for an edit that never happened, when the real problem
// is that something else is using the same database.
func checkName(m migration, recorded appliedRow) error {
	if recorded.name == m.name {
		return nil
	}
	return fmt.Errorf("%w: version %d was applied as %s, but this set has %s "+
		"(are two migration sets sharing one database file?)",
		ErrVersionNameMismatch, m.version, recorded.name, m.name)
}

// applyOne runs one migration's statements and records it, atomically.
//
// The bookkeeping INSERT deliberately comes first, before any of the
// migration's own statements. It is what makes concurrent Apply calls safe:
// two processes starting against the same database file both read an empty
// schema_migrations, so both try to apply migration N. The INSERT takes
// SQLite's single write lock, so the loser blocks until the winner commits
// and then fails on the primary key — at which point it can tell it lost
// and skip the migration entirely. Running the statements first would mean
// the loser executes them too, and statements like ALTER TABLE ADD COLUMN
// (which have no IF NOT EXISTS form) fail outright rather than yielding to
// the winner.
func applyOne(db *sql.DB, m migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, name, applied_at, checksum) VALUES (?, ?, ?, ?)`,
		m.version, m.name, time.Now().UTC().UnixNano(), checksum(m.sql),
	); err != nil {
		if isAlreadyRecorded(err) {
			// Another process applied this migration while we were
			// deciding to: its transaction won the write lock and
			// committed. Nothing left for us to do.
			return nil
		}
		return fmt.Errorf("record migration: %w", err)
	}

	for _, stmt := range splitStatements(m.sql) {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("exec statement: %w", err)
		}
	}

	return tx.Commit()
}

// isAlreadyRecorded reports whether err is the primary-key violation raised
// when schema_migrations already holds a row for this version. It is
// matched on the driver's message so this package need not depend on
// modernc.org/sqlite's concrete error type.
func isAlreadyRecorded(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "PRIMARY KEY constraint failed")
}

// splitStatements splits a migration file into individual statements on
// ';' so each can be sent to the driver separately; the modernc.org/sqlite
// driver executes one statement per call. Comment-only and blank
// statements are dropped.
func splitStatements(sqlText string) []string {
	raw := strings.Split(sqlText, ";")
	statements := make([]string, 0, len(raw))
	for _, s := range raw {
		if isMeaningfulStatement(s) {
			statements = append(statements, strings.TrimSpace(s))
		}
	}
	return statements
}

// isMeaningfulStatement reports whether s contains anything other than
// whitespace and "--" line comments.
func isMeaningfulStatement(s string) bool {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			return true
		}
	}
	return false
}
