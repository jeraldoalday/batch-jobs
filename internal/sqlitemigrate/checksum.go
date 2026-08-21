package sqlitemigrate

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrChecksumMismatch is returned by Apply and Verify when a migration file
// on disk no longer matches what was recorded when it was applied. The
// database is not necessarily broken, but it is no longer described by the
// migration set in the binary, and every environment that applies the
// edited file from scratch will end up with a different schema.
var ErrChecksumMismatch = errors.New("sqlitemigrate: migration checksum mismatch")

// checksum returns the hex-encoded SHA-256 of a migration's contents. It is
// computed over the raw file bytes, not the split statements, so a
// whitespace or comment edit counts as drift too: the point is to detect
// that the file changed at all, not to guess whether the change was
// semantically meaningful.
func checksum(sqlText string) string {
	sum := sha256.Sum256([]byte(sqlText))
	return hex.EncodeToString(sum[:])
}

// ensureChecksumColumn adds schema_migrations.checksum to databases created
// by an older version of this package, which tracked only (version, name,
// applied_at). It is a no-op once the column exists.
//
// This is the one piece of schema in the module that cannot itself be a
// numbered migration, since it is part of the machinery that runs them.
func ensureChecksumColumn(db *sql.DB) error {
	has, err := hasColumn(db, "schema_migrations", "checksum")
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	if _, err := db.Exec(`ALTER TABLE schema_migrations ADD COLUMN checksum TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("add schema_migrations.checksum: %w", err)
	}
	return nil
}

// hasColumn reports whether table already has a column named column.
func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%q)`, table))
	if err != nil {
		return false, fmt.Errorf("table_info(%s): %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			typ        string
			notNull    int
			dflt       sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &primaryKey); err != nil {
			return false, fmt.Errorf("table_info(%s): scan: %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

// verifyChecksum compares a loaded migration against the checksum recorded
// when it was applied. A row recorded before checksums existed (or written
// by an older binary) has an empty checksum: those are backfilled from the
// file rather than reported as drift, since there is nothing to compare
// against.
func verifyChecksum(db *sql.DB, m migration, recorded string) error {
	want := checksum(m.sql)
	if recorded == "" {
		_, err := db.Exec(`UPDATE schema_migrations SET checksum = ? WHERE version = ?`, want, m.version)
		if err != nil {
			return fmt.Errorf("backfill checksum for %s: %w", m.name, err)
		}
		return nil
	}
	if recorded != want {
		return fmt.Errorf("%w: %s was applied as %s but is now %s",
			ErrChecksumMismatch, m.name, short(recorded), short(want))
	}
	return nil
}

// short truncates a hex checksum for error messages, where the first few
// bytes are enough to tell two versions of a file apart.
func short(sum string) string {
	if len(sum) <= 12 {
		return sum
	}
	return sum[:12]
}
