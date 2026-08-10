package sqlite

import (
	"database/sql"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// migrationsTable tracks which migrations have already been applied so
// migrate is safe to run every time Open is called.
const migrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);`

type migration struct {
	version int
	name    string
	sql     string
}

// loadMigrations reads every embedded *.sql file and orders them by the
// numeric prefix in their filename (e.g. "0001_init.sql" -> version 1).
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
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

		contents, err := migrationsFS.ReadFile(path.Join("migrations", entry.Name()))
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
	return migrations, nil
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

// migrate applies every embedded migration that has not already been
// recorded in schema_migrations, each inside its own transaction.
func migrate(db *sql.DB) error {
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

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		if err := applyMigration(db, m); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
	}
	return nil
}

func applyMigration(db *sql.DB, m migration) error {
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
