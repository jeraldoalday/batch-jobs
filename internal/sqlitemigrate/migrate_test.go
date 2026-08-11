package sqlitemigrate

import (
	"database/sql"
	"embed"
	"testing"

	_ "modernc.org/sqlite"
)

//go:embed testdata/migrations/*.sql
var testMigrationsFS embed.FS

const testMigrationsDir = "testdata/migrations"

func openRaw(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestApplyCreatesTables(t *testing.T) {
	db := openRaw(t)
	if err := Apply(db, testMigrationsFS, testMigrationsDir); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'widgets'`).Scan(&name)
	if err != nil {
		t.Fatalf("widgets table not found: %v", err)
	}
}

func TestApplyRecordsEachMigration(t *testing.T) {
	db := openRaw(t)
	if err := Apply(db, testMigrationsFS, testMigrationsDir); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema_migrations has %d rows, want 2 (one per fixture migration)", count)
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	db := openRaw(t)
	if err := Apply(db, testMigrationsFS, testMigrationsDir); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := Apply(db, testMigrationsFS, testMigrationsDir); err != nil {
		t.Fatalf("second Apply: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema_migrations has %d rows after re-running Apply, want 2 (should not duplicate records)", count)
	}
}

func TestLoadOrdersByNumericVersion(t *testing.T) {
	migrations, err := load(testMigrationsFS, testMigrationsDir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("load returned %d migrations, want 2", len(migrations))
	}
	if migrations[0].version != 1 || migrations[1].version != 2 {
		t.Fatalf("load order = %d, %d, want 1, 2", migrations[0].version, migrations[1].version)
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
