package sqlitemigrate

import (
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeraldoalday/batch-jobs/internal/sqlitex"
)

// twoMigrations is a migration set whose second file uses ALTER TABLE ADD
// COLUMN, which has no IF NOT EXISTS form: applying it twice is an error,
// which is exactly what makes it a good probe for concurrent Apply.
func twoMigrations() map[string]string {
	return map[string]string{
		"0001_widgets.sql": widgetsSQL,
		"0002_name.sql":    `ALTER TABLE widgets ADD COLUMN name TEXT NOT NULL DEFAULT '';`,
	}
}

func TestStatusReportsPendingBeforeApply(t *testing.T) {
	db := openRaw(t)

	records, err := Status(db, mapMigrations(twoMigrations()), "migrations")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("Status returned %d records, want 2", len(records))
	}
	for _, r := range records {
		if r.State != StatePending || !r.Pending() {
			t.Errorf("migration %d state = %q, want pending", r.Version, r.State)
		}
	}

	// Status must not itself apply anything.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if count != 0 {
		t.Fatalf("Status applied %d migration(s), want 0", count)
	}
}

func TestStatusReportsAppliedWithTimestamp(t *testing.T) {
	db := openRaw(t)
	fsys := mapMigrations(twoMigrations())
	if err := Apply(db, fsys, "migrations"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	records, err := Status(db, fsys, "migrations")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	for _, r := range records {
		if r.State != StateApplied {
			t.Errorf("migration %d state = %q, want applied", r.Version, r.State)
		}
		if r.AppliedAt.IsZero() {
			t.Errorf("migration %d has no AppliedAt timestamp", r.Version)
		}
	}
	if records[0].Version != 1 || records[1].Version != 2 {
		t.Fatalf("Status order = %d, %d, want ascending 1, 2", records[0].Version, records[1].Version)
	}
}

func TestStatusReportsDriftWithoutFailing(t *testing.T) {
	db := openRaw(t)
	if err := Apply(db, mapMigrations(twoMigrations()), "migrations"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	edited := twoMigrations()
	edited["0001_widgets.sql"] = widgetsSQL + "\n-- edited after the fact"

	records, err := Status(db, mapMigrations(edited), "migrations")
	if err != nil {
		t.Fatalf("Status over drifted migrations: %v", err)
	}
	if records[0].State != StateDrifted {
		t.Fatalf("migration 1 state = %q, want drifted", records[0].State)
	}
	if records[1].State != StateApplied {
		t.Fatalf("migration 2 state = %q, want applied (its file is untouched)", records[1].State)
	}

	if err := Verify(db, mapMigrations(edited), "migrations"); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Verify over drifted migrations = %v, want ErrChecksumMismatch", err)
	}
}

func TestStatusReportsMigrationsMissingFromDisk(t *testing.T) {
	db := openRaw(t)
	fsys := mapMigrations(map[string]string{"0001_widgets.sql": widgetsSQL})
	if err := Apply(db, fsys, "migrations"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// A newer build migrated this database and left behind a version this
	// binary knows nothing about.
	if _, err := db.Exec(
		`INSERT INTO schema_migrations (version, name, applied_at, checksum) VALUES (7, '0007_future.sql', 1, 'deadbeef')`,
	); err != nil {
		t.Fatalf("insert future migration: %v", err)
	}

	records, err := Status(db, fsys, "migrations")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("Status returned %d records, want 2 (one on disk, one only in the database)", len(records))
	}
	if records[1].State != StateMissing || records[1].Version != 7 {
		t.Fatalf("second record = version %d/%q, want version 7 missing", records[1].Version, records[1].State)
	}

	if err := Verify(db, fsys, "migrations"); !errors.Is(err, ErrMissingMigration) {
		t.Fatalf("Verify with a database ahead of the binary = %v, want ErrMissingMigration", err)
	}
}

func TestVerifyAcceptsPendingMigrations(t *testing.T) {
	db := openRaw(t)
	// Nothing applied yet: pending work is not drift.
	if err := Verify(db, mapMigrations(twoMigrations()), "migrations"); err != nil {
		t.Fatalf("Verify with only pending migrations: %v", err)
	}
}

func TestVersionTracksHighestApplied(t *testing.T) {
	db := openRaw(t)

	got, err := Version(db)
	if err != nil {
		t.Fatalf("Version on a fresh database: %v", err)
	}
	if got != 0 {
		t.Fatalf("Version on a fresh database = %d, want 0", got)
	}

	if err := Apply(db, mapMigrations(twoMigrations()), "migrations"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, err = Version(db); err != nil || got != 2 {
		t.Fatalf("Version after Apply = %d (err %v), want 2", got, err)
	}
}

func TestApplyIsSafeFromConcurrentProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migrate.db")
	fsys := mapMigrations(twoMigrations())

	const processes = 4
	dbs := make([]*sql.DB, processes)
	for i := range dbs {
		// A separate *sql.DB per goroutine stands in for a separate
		// process: nothing is shared but the file itself.
		db, err := sqlitex.Open(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		t.Cleanup(func() { db.Close() })
		dbs[i] = db
	}

	errs := make([]error, processes)
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = Apply(db, fsys, "migrations")
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Apply %d: %v", i, err)
		}
	}

	var count int
	if err := dbs[0].QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema_migrations has %d rows after %d concurrent Apply calls, want 2", count, processes)
	}
}
