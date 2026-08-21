package sqlitemigrate

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"
)

// ErrMissingMigration is returned by Verify when the database records a
// migration that the binary's embedded set does not contain -- the database
// was migrated by a newer build and this one cannot describe it.
var ErrMissingMigration = errors.New("sqlitemigrate: applied migration not found on disk")

// State is where a migration stands relative to a particular database.
type State string

const (
	// StateApplied means the migration has run against this database and
	// its file still matches the checksum recorded at the time.
	StateApplied State = "applied"

	// StatePending means the migration exists on disk but has not run
	// against this database yet.
	StatePending State = "pending"

	// StateDrifted means the migration has run, but its file has changed
	// since. The database's schema and the file no longer agree.
	StateDrifted State = "drifted"

	// StateMissing means the database records a migration that the
	// embedded set does not contain, i.e. it was migrated by a newer
	// build.
	StateMissing State = "missing"
)

// Record describes one migration's standing against a database: the
// per-migration rows a "migrate status" command prints.
type Record struct {
	// Version is the numeric prefix of the migration's filename.
	Version int

	// Name is the migration's filename, or the name recorded in
	// schema_migrations for a StateMissing record.
	Name string

	// State is where the migration stands: applied, pending, drifted, or
	// missing.
	State State

	// AppliedAt is when the migration ran, zero unless State is applied,
	// drifted, or missing.
	AppliedAt time.Time
}

// Pending reports whether the migration still needs to run.
func (r Record) Pending() bool { return r.State == StatePending }

// Status reports the standing of every migration in dir against db, in
// ascending version order, without changing anything: a database that has
// never been migrated reports every migration as pending rather than
// failing, and a checksum mismatch is reported as a StateDrifted record
// rather than an error.
//
// It is the read-only counterpart to Apply, meant for tooling that shows an
// operator what a migration run would do before they authorize it.
func Status(db *sql.DB, fsys fs.FS, dir string) ([]Record, error) {
	if err := ensureTable(db); err != nil {
		return nil, err
	}

	migrations, err := load(fsys, dir)
	if err != nil {
		return nil, err
	}

	applied, err := appliedRows(db)
	if err != nil {
		return nil, err
	}

	records := make([]Record, 0, len(migrations)+len(applied))
	onDisk := make(map[int]bool, len(migrations))
	for _, m := range migrations {
		onDisk[m.version] = true

		row, ok := applied[m.version]
		if !ok {
			records = append(records, Record{Version: m.version, Name: m.name, State: StatePending})
			continue
		}

		state := StateApplied
		if row.name != m.name || (row.checksum != "" && row.checksum != checksum(m.sql)) {
			state = StateDrifted
		}
		records = append(records, Record{
			Version:   m.version,
			Name:      m.name,
			State:     state,
			AppliedAt: row.appliedAt,
		})
	}

	// Rows recorded in the database with no matching file: the database is
	// ahead of this binary. Reporting them is the whole point of a status
	// command -- silently omitting them would make a newer database look
	// fully migrated.
	for version, row := range applied {
		if onDisk[version] {
			continue
		}
		records = append(records, Record{
			Version:   version,
			Name:      row.name,
			State:     StateMissing,
			AppliedAt: row.appliedAt,
		})
	}

	sort.Slice(records, func(i, j int) bool { return records[i].Version < records[j].Version })
	return records, nil
}

// Verify checks that db's schema is exactly what dir describes: no drift in
// what has been applied, and nothing applied that the binary does not know
// about. Pending migrations are not an error -- they are what Apply is for.
func Verify(db *sql.DB, fsys fs.FS, dir string) error {
	records, err := Status(db, fsys, dir)
	if err != nil {
		return err
	}

	for _, r := range records {
		switch r.State {
		case StateDrifted:
			return fmt.Errorf("%w: %s changed after it was applied", ErrChecksumMismatch, r.Name)
		case StateMissing:
			return fmt.Errorf("%w: version %d (%s)", ErrMissingMigration, r.Version, r.Name)
		}
	}
	return nil
}

// Version returns the highest migration version applied to db, or 0 if none
// have been. It is the single number a deployment check or a log line wants
// when the full Status listing is more than it needs.
func Version(db *sql.DB) (int, error) {
	if err := ensureTable(db); err != nil {
		return 0, err
	}

	var version sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	if !version.Valid {
		return 0, nil
	}
	return int(version.Int64), nil
}

// appliedRow is one schema_migrations row.
type appliedRow struct {
	name      string
	appliedAt time.Time
	checksum  string
}

// appliedRows reads schema_migrations keyed by version.
func appliedRows(db *sql.DB) (map[int]appliedRow, error) {
	rows, err := db.Query(`SELECT version, name, applied_at, checksum FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]appliedRow)
	for rows.Next() {
		var (
			version   int
			name      string
			appliedAt int64
			sum       string
		)
		if err := rows.Scan(&version, &name, &appliedAt, &sum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = appliedRow{
			name:      name,
			appliedAt: time.Unix(0, appliedAt).UTC(),
			checksum:  sum,
		}
	}
	return applied, rows.Err()
}
