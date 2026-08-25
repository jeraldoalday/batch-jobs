package sqlitex

import "strings"

// MemoryPath is the conventional path for an ephemeral, process-local
// database held entirely in RAM.
const MemoryPath = ":memory:"

// isPrivateMemory reports whether path names an in-memory database that is
// private to a single connection.
//
// SQLite gives every connection opened against ":memory:" -- or against a
// URI with mode=memory and no shared cache -- a database of its own. That
// is invisible until something opens a second connection, at which point
// it finds an empty database: a table created through the first connection
// does not exist on the second. database/sql opens connections lazily and
// on demand, so which calls hit which database is a matter of timing,
// and the symptom is an intermittent "no such table" from a background
// sweep or a second goroutine rather than an outright failure at startup.
//
// A URI carrying cache=shared is excluded, since that is precisely the
// arrangement under which several connections do share one in-memory
// database.
func isPrivateMemory(path string) bool {
	if path == MemoryPath || path == "" {
		return true
	}
	if !strings.Contains(path, "mode=memory") {
		return false
	}
	return !strings.Contains(path, "cache=shared")
}

// memoryPoolSize is the only pool size a private in-memory database can
// safely have.
//
// The alternative -- rewriting the path to a uniquely named shared-cache
// URI so the pool's connections meet in one database -- would allow real
// concurrency, but SQLite's shared-cache mode locks at table granularity
// and reports contention as SQLITE_LOCKED, which the busy timeout does not
// retry. Trading a documented serialization for an undocumented class of
// intermittent error is a bad bargain for what is, by definition, an
// ephemeral database used by tests and small workloads.
const memoryPoolSize = 1
