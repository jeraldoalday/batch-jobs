// Command batch-jobs-migrate inspects and applies the schema migrations
// behind batch-jobs' SQLite stores.
//
// The server applies pending migrations itself on startup, which is the
// right default for a single process on a laptop and the wrong one for a
// deployment: several instances rolling at once, a schema change that
// wants a maintenance window, or an operator who simply wants to know what
// is about to run before it runs. This command separates that decision
// from the deploy.
//
//	batch-jobs-migrate -db jobs.db status    # what would run, and what already has
//	batch-jobs-migrate -db jobs.db up        # apply everything pending
//	batch-jobs-migrate -db jobs.db verify    # fail if the database and the binary disagree
//	batch-jobs-migrate -db jobs.db version   # highest applied version, for scripts
//
// Each store keeps its own database file, since they share a
// schema_migrations table if they do not and their version numbers
// collide. Pass -schedules-db to act on the schedules store as well:
//
//	batch-jobs-migrate -db jobs.db -schedules-db schedules.db status
package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"text/tabwriter"
	"time"

	queuesqlite "github.com/jeraldoalday/batch-jobs/internal/queue/sqlite"
	schedulesqlite "github.com/jeraldoalday/batch-jobs/internal/schedule/sqlite"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitemigrate"
	"github.com/jeraldoalday/batch-jobs/internal/sqlitex"
)

// target is one migration set and the database it belongs to, so the same
// four commands work across stores without either one knowing about the
// other.
type target struct {
	name string
	path string
	fsys fs.FS
	dir  string
}

func main() {
	dbPath := flag.String("db", "batch-jobs.db", "SQLite database path for the jobs store")
	schedulesPath := flag.String("schedules-db", "", "SQLite database path for the schedules store (empty: skip it)")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	targets, err := buildTargets(*dbPath, *schedulesPath)
	if err != nil {
		fail(err)
	}

	if err := run(flag.Arg(0), targets); err != nil {
		fail(err)
	}
}

// buildTargets turns the -db and -schedules-db flags into the targets a run
// acts on, rejecting the one combination that is invalid on its face: the
// two stores number their migrations independently, so pointing both flags
// at the same file would collide in one schema_migrations table.
func buildTargets(dbPath, schedulesPath string) ([]target, error) {
	targets := []target{
		{name: "jobs", path: dbPath, fsys: queuesqlite.Migrations(), dir: queuesqlite.MigrationsDir},
	}
	if schedulesPath == "" {
		return targets, nil
	}
	if schedulesPath == dbPath {
		return nil, errors.New("-schedules-db must differ from -db: the two stores number their " +
			"migrations independently and would collide in one schema_migrations table")
	}
	return append(targets, target{
		name: "schedules", path: schedulesPath,
		fsys: schedulesqlite.Migrations(), dir: schedulesqlite.MigrationsDir,
	}), nil
}

// run dispatches one command across every target, opening each database in
// turn and closing it before moving on, so a failure against the first
// store does not leave a connection open to the second.
func run(command string, targets []target) error {
	for _, t := range targets {
		if err := runOne(command, t); err != nil {
			return fmt.Errorf("%s (%s): %w", t.name, t.path, err)
		}
	}
	return nil
}

func runOne(command string, t target) error {
	db, err := sqlitex.Open(t.path)
	if err != nil {
		return err
	}
	defer db.Close()

	switch command {
	case "status":
		return status(db, t)
	case "up":
		return up(db, t)
	case "verify":
		return verify(db, t)
	case "version":
		return version(db, t)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

// status prints every migration and where it stands, without changing
// anything -- the command an operator runs before deciding to run "up".
func status(db *sql.DB, t target) error {
	records, err := sqlitemigrate.Status(db, t.fsys, t.dir)
	if err != nil {
		return err
	}

	fmt.Printf("%s: %s\n", t.name, t.path)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  VERSION\tSTATE\tAPPLIED AT\tNAME")

	pending := 0
	for _, r := range records {
		if r.Pending() {
			pending++
		}
		fmt.Fprintf(w, "  %d\t%s\t%s\t%s\n", r.Version, r.State, appliedAt(r), r.Name)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("  %d of %d migration(s) pending\n\n", pending, len(records))
	return nil
}

// up applies everything pending, reporting the count rather than finishing
// silently: "nothing to do" and "applied four migrations" should not look
// the same in a deploy log.
func up(db *sql.DB, t target) error {
	before, err := pendingCount(db, t)
	if err != nil {
		return err
	}
	if err := sqlitemigrate.Apply(db, t.fsys, t.dir); err != nil {
		return err
	}

	v, err := sqlitemigrate.Version(db)
	if err != nil {
		return err
	}
	fmt.Printf("%s: applied %d migration(s), now at version %d\n", t.name, before, v)
	return nil
}

// verify reports whether the database matches the migrations in this
// binary, for a deploy gate or a post-restore check. Pending migrations
// are not a failure; drift and unknown applied versions are.
func verify(db *sql.DB, t target) error {
	if err := sqlitemigrate.Verify(db, t.fsys, t.dir); err != nil {
		return err
	}
	fmt.Printf("%s: ok\n", t.name)
	return nil
}

// version prints the highest applied version for scripts that want a
// single number.
func version(db *sql.DB, t target) error {
	v, err := sqlitemigrate.Version(db)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %d\n", t.name, v)
	return nil
}

// pendingCount is how many of a target's migrations have yet to run.
func pendingCount(db *sql.DB, t target) (int, error) {
	records, err := sqlitemigrate.Status(db, t.fsys, t.dir)
	if err != nil {
		return 0, err
	}

	n := 0
	for _, r := range records {
		if r.Pending() {
			n++
		}
	}
	return n, nil
}

// appliedAt renders a record's timestamp, or a dash for one that has not
// run.
func appliedAt(r sqlitemigrate.Record) string {
	if r.AppliedAt.IsZero() {
		return "-"
	}
	return r.AppliedAt.Format(time.RFC3339)
}

func usage() {
	fmt.Fprintf(os.Stderr, `batch-jobs-migrate inspects and applies batch-jobs' SQLite schema migrations.

usage: batch-jobs-migrate [flags] <status|up|verify|version>

  status   list every migration and whether it has been applied
  up       apply all pending migrations
  verify   exit non-zero if the database does not match this binary's migrations
  version  print the highest applied migration version

flags:
`)
	flag.PrintDefaults()
}

// fail reports an error and exits non-zero, so a failed verify stops a
// deploy script rather than being scrolled past. Drift gets an extra line,
// because the fix is not obvious from the error alone.
func fail(err error) {
	fmt.Fprintf(os.Stderr, "batch-jobs-migrate: %v\n", err)
	if errors.Is(err, sqlitemigrate.ErrChecksumMismatch) {
		fmt.Fprintln(os.Stderr, "\nA migration file changed after it was applied. Either restore the file to\n"+
			"what was applied, or add a new migration expressing the change.")
	}
	if errors.Is(err, sqlitemigrate.ErrMissingMigration) {
		fmt.Fprintln(os.Stderr, "\nThis database was migrated by a newer build than the one running. Deploy\n"+
			"that build, or restore a database matching this one.")
	}
	os.Exit(1)
}
