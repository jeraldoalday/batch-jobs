// Package sqlite implements queue.Queue on top of a SQLite database file,
// giving jobs durability across process restarts while preserving the same
// priority- and delay-aware dequeue semantics as queue.Memory.
package sqlite

import (
	"embed"
	"io/fs"
)

// migrationsFS embeds the numbered .sql migration files applied by migrate
// to bring a database up to the current schema.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrationsDir is the path, within the FS returned by Migrations, holding
// this package's migration files.
const MigrationsDir = "migrations"

// Migrations exposes the embedded migration set so tooling outside this
// package -- the migrate command, above all -- can inspect and apply
// exactly what Open would apply, rather than re-embedding a second copy
// that could drift from it.
func Migrations() fs.FS { return migrationsFS }
