package sqlitemigrate

import (
	"errors"
	"testing"
	"testing/fstest"
)

// mapMigrations builds an in-memory migrations directory, so a test can
// edit a migration's contents between Apply calls -- something an embedded
// testdata fixture cannot do.
func mapMigrations(files map[string]string) fstest.MapFS {
	fsys := make(fstest.MapFS, len(files))
	for name, contents := range files {
		fsys["migrations/"+name] = &fstest.MapFile{Data: []byte(contents)}
	}
	return fsys
}

const widgetsSQL = `CREATE TABLE IF NOT EXISTS widgets (id INTEGER PRIMARY KEY);`

func TestApplyDetectsEditedMigration(t *testing.T) {
	db := openRaw(t)

	before := mapMigrations(map[string]string{"0001_widgets.sql": widgetsSQL})
	if err := Apply(db, before, "migrations"); err != nil {
		t.Fatalf("first Apply: %v", err)
	}

	after := mapMigrations(map[string]string{
		"0001_widgets.sql": widgetsSQL + "\nALTER TABLE widgets ADD COLUMN name TEXT;",
	})
	err := Apply(db, after, "migrations")
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Apply after editing an applied migration = %v, want ErrChecksumMismatch", err)
	}
}

func TestApplyAcceptsUnchangedMigration(t *testing.T) {
	db := openRaw(t)
	fsys := mapMigrations(map[string]string{"0001_widgets.sql": widgetsSQL})

	if err := Apply(db, fsys, "migrations"); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := Apply(db, fsys, "migrations"); err != nil {
		t.Fatalf("second Apply with identical files: %v", err)
	}
}

func TestApplyBackfillsMissingChecksum(t *testing.T) {
	db := openRaw(t)
	fsys := mapMigrations(map[string]string{"0001_widgets.sql": widgetsSQL})
	if err := Apply(db, fsys, "migrations"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Simulate a row written by a build from before checksums existed.
	if _, err := db.Exec(`UPDATE schema_migrations SET checksum = ''`); err != nil {
		t.Fatalf("clear checksums: %v", err)
	}

	if err := Apply(db, fsys, "migrations"); err != nil {
		t.Fatalf("Apply over checksum-less rows: %v", err)
	}

	var sum string
	if err := db.QueryRow(`SELECT checksum FROM schema_migrations WHERE version = 1`).Scan(&sum); err != nil {
		t.Fatalf("read checksum: %v", err)
	}
	if sum != checksum(widgetsSQL) {
		t.Fatalf("checksum = %q, want it backfilled to %q", sum, checksum(widgetsSQL))
	}
}

func TestEnsureChecksumColumnUpgradesLegacyTable(t *testing.T) {
	db := openRaw(t)

	// The pre-checksum shape of the table, as an older binary created it.
	_, err := db.Exec(`CREATE TABLE schema_migrations (
        version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL)`)
	if err != nil {
		t.Fatalf("create legacy table: %v", err)
	}

	if err := Apply(db, mapMigrations(map[string]string{"0001_widgets.sql": widgetsSQL}), "migrations"); err != nil {
		t.Fatalf("Apply against legacy schema_migrations: %v", err)
	}

	has, err := hasColumn(db, "schema_migrations", "checksum")
	if err != nil {
		t.Fatalf("hasColumn: %v", err)
	}
	if !has {
		t.Fatal("schema_migrations.checksum was not added to the legacy table")
	}
}

func TestHasColumnReportsUnknownColumn(t *testing.T) {
	db := openRaw(t)
	if err := Apply(db, testMigrationsFS, testMigrationsDir); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	has, err := hasColumn(db, "widgets", "nonexistent")
	if err != nil {
		t.Fatalf("hasColumn: %v", err)
	}
	if has {
		t.Fatal("hasColumn reported a column that does not exist")
	}
}

func TestApplyRejectsDuplicateVersions(t *testing.T) {
	db := openRaw(t)
	fsys := mapMigrations(map[string]string{
		"0001_widgets.sql": widgetsSQL,
		"0001_gadgets.sql": `CREATE TABLE IF NOT EXISTS gadgets (id INTEGER PRIMARY KEY);`,
	})

	err := Apply(db, fsys, "migrations")
	if !errors.Is(err, ErrDuplicateVersion) {
		t.Fatalf("Apply with two version-1 migrations = %v, want ErrDuplicateVersion", err)
	}

	// Nothing should have been applied: the check runs before any
	// statement does.
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('widgets', 'gadgets')`,
	).Scan(&count); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if count != 0 {
		t.Fatalf("%d table(s) created despite the duplicate-version error, want 0", count)
	}
}
