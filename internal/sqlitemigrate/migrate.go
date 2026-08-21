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
func Apply(db *sql.DB, fsys fs.FS, dir string) error {
	if _, err := db.Exec(migrationsTable); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := make(map[int]bool)
	rows, err := db.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read applied migrations: %w", err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("scan applied migration: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	migrations, err := load(fsys, dir)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		if err := applyOne(db, m); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
	}
	return nil
}

func applyOne(db *sql.DB, m migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, stmt := range splitStatements(m.sql) {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("exec statement: %w", err)
		}
	}

	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
		m.version, m.name, time.Now().UTC().UnixNano(),
	); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}

	return tx.Commit()
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
