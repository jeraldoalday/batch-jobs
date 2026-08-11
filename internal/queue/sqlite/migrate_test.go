package sqlite

import (
	"database/sql"
	"testing"
)

func openRaw(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMigrateCreatesJobsTable(t *testing.T) {
	db := openRaw(t)
	if err := migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs'`).Scan(&name)
	if err != nil {
		t.Fatalf("jobs table not found: %v", err)
	}
}

func TestMigrateRecordsAppliedVersions(t *testing.T) {
	db := openRaw(t)
	if err := migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if count == 0 {
		t.Fatal("schema_migrations has no rows after migrate")
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := openRaw(t)
	if err := migrate(db); err != nil {
		t.Fatalf("first migrate: %v", err)
	}

	var firstCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&firstCount); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}

	if err := migrate(db); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	var secondCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&secondCount); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if secondCount != firstCount {
		t.Fatalf("schema_migrations has %d rows after re-running migrate, want %d (should not duplicate records)", secondCount, firstCount)
	}
}
