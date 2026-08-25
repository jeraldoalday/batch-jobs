package sqlitex

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestOptionsDSNDefaults(t *testing.T) {
	dsn := Options{}.DSN()

	for _, want := range []string{
		"_journal_mode=WAL",
		"_busy_timeout=5000",
		"_foreign_keys=on",
		"_synchronous=NORMAL",
		"cache_size%28-8192%29", // URL-encoded cache_size(-8192)
	} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN() = %q, want it to contain %q", dsn, want)
		}
	}
}

func TestOptionsDSNOverrides(t *testing.T) {
	dsn := Options{
		BusyTimeout: 250 * time.Millisecond,
		Synchronous: SynchronousFull,
		CacheSizeKB: 64,
	}.DSN()

	for _, want := range []string{"_busy_timeout=250", "_synchronous=FULL", "cache_size%28-64%29"} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN() = %q, want it to contain %q", dsn, want)
		}
	}
}

// TestOptionsDSNIsStable matters because the DSN is what identifies a
// connection's configuration in logs and in bug reports; an unstable
// ordering makes two identical configurations look different.
func TestOptionsDSNIsStable(t *testing.T) {
	var opts Options
	first := opts.DSN()
	for i := 0; i < 10; i++ {
		if got := opts.DSN(); got != first {
			t.Fatalf("DSN() = %q on call %d, was %q", got, i, first)
		}
	}
}

func TestWithPragmasJoinsCorrectly(t *testing.T) {
	if got := WithPragmas("/tmp/x.db"); !strings.HasPrefix(got, "/tmp/x.db?") {
		t.Errorf("WithPragmas(%q) = %q, want a '?' separator", "/tmp/x.db", got)
	}
	if got := WithPragmas("file:x?mode=memory"); !strings.HasPrefix(got, "file:x?mode=memory&") {
		t.Errorf("WithPragmas on a path with a query = %q, want an '&' separator", got)
	}
}

func TestOpenAppliesPragmas(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pragmas.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tests := []struct {
		pragma string
		want   string
	}{
		{"journal_mode", "wal"},
		{"synchronous", "1"}, // 1 == NORMAL
		{"foreign_keys", "1"},
	}
	for _, tc := range tests {
		var got string
		if err := db.QueryRow("PRAGMA " + tc.pragma).Scan(&got); err != nil {
			t.Fatalf("PRAGMA %s: %v", tc.pragma, err)
		}
		if strings.ToLower(got) != tc.want {
			t.Errorf("PRAGMA %s = %q, want %q", tc.pragma, got, tc.want)
		}
	}

	var cacheSize int
	if err := db.QueryRow("PRAGMA cache_size").Scan(&cacheSize); err != nil {
		t.Fatalf("PRAGMA cache_size: %v", err)
	}
	if cacheSize != -DefaultCacheSizeKB {
		t.Errorf("PRAGMA cache_size = %d, want %d", cacheSize, -DefaultCacheSizeKB)
	}
}

// TestOpenAppliesPragmasToEveryPooledConnection is the reason the settings
// live in the DSN rather than in a PRAGMA issued after Open: database/sql
// opens connections lazily, so a one-off Exec would configure only
// whichever one happened to serve it.
func TestOpenAppliesPragmasToEveryPooledConnection(t *testing.T) {
	db, err := OpenWith(filepath.Join(t.TempDir(), "pool.db"), Options{MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	defer db.Close()

	// Hold several connections open at once, so the pool is forced to
	// open more than one, and check each reports the configured mode.
	const conns = 4
	var wg sync.WaitGroup
	release := make(chan struct{})
	errs := make([]error, conns)
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := db.Conn(t.Context())
			if err != nil {
				errs[i] = err
				return
			}
			defer c.Close()

			var mode string
			if err := c.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&mode); err != nil {
				errs[i] = err
				return
			}
			if strings.ToLower(mode) != "wal" {
				errs[i] = fmt.Errorf("connection %d reports journal_mode %q, want wal", i, mode)
			}
			<-release
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("connection %d: %v", i, err)
		}
	}
}

func TestOpenSetsPoolLimits(t *testing.T) {
	db, err := OpenWith(filepath.Join(t.TempDir(), "limits.db"), Options{MaxOpenConns: 3})
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if stats.MaxOpenConnections != 3 {
		t.Errorf("MaxOpenConnections = %d, want 3", stats.MaxOpenConnections)
	}
}

// TestOpenSurvivesConcurrentFirstConnections is the regression test for
// the journal-mode race. Applying "PRAGMA journal_mode = WAL" to a
// database that is not yet in WAL mode needs a brief exclusive lock that
// SQLite does *not* run the busy handler for, so before the retrying
// connector, several pools opening one fresh file at the same instant
// would reliably leave one of them failing outright with SQLITE_BUSY.
func TestOpenSurvivesConcurrentFirstConnections(t *testing.T) {
	const rounds = 25
	const pools = 4

	for round := 0; round < rounds; round++ {
		path := filepath.Join(t.TempDir(), "race.db")

		dbs := make([]*sql.DB, pools)
		for i := range dbs {
			db, err := Open(path)
			if err != nil {
				t.Fatalf("round %d: Open %d: %v", round, i, err)
			}
			dbs[i] = db
		}

		errs := make([]error, pools)
		var wg sync.WaitGroup
		for i, db := range dbs {
			wg.Add(1)
			go func(i int, db *sql.DB) {
				defer wg.Done()
				// Ping is what actually opens the connection, and so
				// what applies the pragmas.
				errs[i] = db.Ping()
			}(i, db)
		}
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Errorf("round %d: pool %d failed to connect: %v", round, i, err)
			}
		}
		for _, db := range dbs {
			db.Close()
		}
	}
}

func TestIsBusy(t *testing.T) {
	if isBusy(nil) {
		t.Error("isBusy(nil) = true")
	}
	if isBusy(errors.New("some other failure")) {
		t.Error("isBusy on an unrelated error = true")
	}

	// A real SQLITE_BUSY, produced by holding an exclusive lock on one
	// connection while another with no busy timeout tries to write.
	path := filepath.Join(t.TempDir(), "busy.db")
	holder, err := OpenWith(path, Options{MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	defer holder.Close()
	if _, err := holder.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	tx, err := holder.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	other, err := OpenWith(path, Options{MaxOpenConns: 1, BusyTimeout: time.Millisecond})
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	defer other.Close()

	_, err = other.Exec(`INSERT INTO t (id) VALUES (2)`)
	if err == nil {
		t.Skip("the second writer was not blocked; nothing to classify")
	}
	var serr *sqlite.Error
	if !errors.As(err, &serr) {
		t.Skipf("second writer failed with a non-driver error: %v", err)
	}
	if !isBusy(err) {
		t.Errorf("isBusy(%v) = false, want true for code %d", err, serr.Code())
	}
}
