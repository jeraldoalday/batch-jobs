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
	if err := migrate(db); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if count != len(mustLoadMigrations(t)) {
		t.Fatalf("schema_migrations has %d rows, want %d (re-running migrate should not duplicate records)", count, len(mustLoadMigrations(t)))
	}
}

func TestVersionFromFilename(t *testing.T) {
	tests := []struct {
		name    string
		want    int
		wantErr bool
	}{
		{name: "0001_init.sql", want: 1},
		{name: "0042_add_index.sql", want: 42},
		{name: "no_version.sql", wantErr: true},
		{name: "abc_init.sql", wantErr: true},
	}

	for _, tt := range tests {
		got, err := versionFromFilename(tt.name)
		if tt.wantErr {
			if err == nil {
				t.Errorf("versionFromFilename(%q): want error, got version %d", tt.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("versionFromFilename(%q): unexpected error: %v", tt.name, err)
			continue
		}
		if got != tt.want {
			t.Errorf("versionFromFilename(%q) = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func mustLoadMigrations(t *testing.T) []migration {
	t.Helper()
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	return migrations
}
