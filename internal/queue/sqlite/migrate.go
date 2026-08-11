package sqlite

import (
	"database/sql"

	"github.com/jeraldoalday/batch-jobs/internal/sqlitemigrate"
)

// migrate applies every embedded migration that has not already been
// recorded in schema_migrations.
func migrate(db *sql.DB) error {
	return sqlitemigrate.Apply(db, migrationsFS, "migrations")
}
