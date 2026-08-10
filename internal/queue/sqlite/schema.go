// Package sqlite implements queue.Queue on top of a SQLite database file,
// giving jobs durability across process restarts while preserving the same
// priority- and delay-aware dequeue semantics as queue.Memory.
package sqlite

import "embed"

// migrationsFS embeds the numbered .sql migration files applied by migrate
// to bring a database up to the current schema.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS
