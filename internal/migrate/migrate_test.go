package migrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/migrate"
	"github.com/jenishmsoni45/job-queue/internal/pgtest"
	"github.com/jenishmsoni45/job-queue/migrations"
)

// withRealMigrations returns an FS holding the project's real migrations plus extra
// test migrations, so the tests exercise the real 0001 that creates schema_migrations.
func withRealMigrations(t *testing.T, extra map[string]string) fstest.MapFS {
	t.Helper()
	fsys := fstest.MapFS{}
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		body, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			t.Fatal(err)
		}
		fsys[name] = &fstest.MapFile{Data: body}
	}
	for name, body := range extra {
		if _, clash := fsys[name]; clash {
			t.Fatalf("test migration %s clashes with a real one", name)
		}
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

func versions(ms []migrate.Migration) []int64 {
	var vs []int64
	for _, m := range ms {
		vs = append(vs, m.Version)
	}
	return vs
}

func TestLoadRealMigrations(t *testing.T) {
	ms, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("Load(migrations.FS): %v", err)
	}
	if len(ms) == 0 || ms[0].Version != 1 || ms[0].Name != "schema_migrations" {
		t.Fatalf("first real migration = %+v, want version 1 named schema_migrations", ms[0])
	}
	if !strings.Contains(ms[0].SQL, "CREATE TABLE schema_migrations") {
		t.Error("0001 does not create schema_migrations")
	}
}

func TestLoadSortsAndChecksums(t *testing.T) {
	fsys := fstest.MapFS{
		"0010_c.sql": {Data: []byte("SELECT 10;")},
		"0002_b.sql": {Data: []byte("SELECT 2;")},
		"0001_a.sql": {Data: []byte("SELECT 1;")},
		"README.md":  {Data: []byte("not a migration")},
	}
	ms, err := migrate.Load(fsys)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(versions(ms)); got != "[1 2 10]" {
		t.Fatalf("versions = %s, want [1 2 10]", got)
	}
	if ms[1].Name != "b" || ms[1].File != "0002_b.sql" || ms[1].SQL != "SELECT 2;" {
		t.Errorf("ms[1] = %+v", ms[1])
	}
	// The checksum is the hex SHA-256 of the file.
	for _, m := range ms {
		sum := sha256.Sum256([]byte(m.SQL))
		if want := hex.EncodeToString(sum[:]); m.Checksum != want {
			t.Errorf("%s: checksum %s, want %s", m.File, m.Checksum, want)
		}
	}
}

func TestLoadRejectsBadSets(t *testing.T) {
	tests := map[string]fstest.MapFS{
		"no files":           {},
		"bad name":           {"1_short.sql": {Data: []byte("SELECT 1;")}},
		"uppercase name":     {"0001_Init.sql": {Data: []byte("SELECT 1;")}},
		"version zero":       {"0000_zero.sql": {Data: []byte("SELECT 1;")}},
		"duplicate versions": {"0002_a.sql": {Data: []byte("SELECT 1;")}, "00002_b.sql": {Data: []byte("SELECT 2;")}},
	}
	for name, fsys := range tests {
		if _, err := migrate.Load(fsys); err == nil {
			t.Errorf("%s: Load returned nil error", name)
		}
	}
}

func TestUpAppliesInOrderAndRecordsEach(t *testing.T) {
	t.Parallel()
	conn := pgtest.New(t).Connect(t)
	fsys := withRealMigrations(t, map[string]string{
		"0002_create_a.sql": "CREATE TABLE a (id int PRIMARY KEY);",
		// Fails unless 0002 already ran, so it proves the order.
		"0003_alter_a.sql": "ALTER TABLE a ADD COLUMN note text NOT NULL DEFAULT '';",
	})

	applied, err := migrate.Up(t.Context(), conn, fsys)
	if err != nil {
		t.Fatalf("Up: %v", err)
	}
	if got := fmt.Sprint(versions(applied)); got != "[1 2 3]" {
		t.Fatalf("applied %s, want [1 2 3]", got)
	}

	want, err := migrate.Load(fsys)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(t.Context(), "SELECT version, name, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	type rec struct {
		Version        int64
		Name, Checksum string
	}
	recs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[rec])
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(want) {
		t.Fatalf("schema_migrations has %d rows, want %d", len(recs), len(want))
	}
	for i, r := range recs {
		if r.Version != want[i].Version || r.Name != want[i].Name || r.Checksum != want[i].Checksum {
			t.Errorf("row %d = %+v, want version %d name %s checksum %s", i, r, want[i].Version, want[i].Name, want[i].Checksum)
		}
	}
}

// P1-4: running the runner twice leaves the schema unchanged.
func TestUpTwiceLeavesSchemaUnchanged(t *testing.T) {
	t.Parallel()
	sets := map[string]fs.FS{
		"real migrations": migrations.FS,
		"real plus test migrations": withRealMigrations(t, map[string]string{
			"0002_objects.sql": `
				CREATE TABLE widgets (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				                      shop text NOT NULL, n int NOT NULL CHECK (n >= 0));
				CREATE INDEX widgets_shop ON widgets (shop) WHERE n > 0;
				INSERT INTO widgets (shop, n) VALUES ('a', 1), ('b', 2);`,
			"0003_more.sql": "ALTER TABLE widgets ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();",
		}),
	}
	for name, fsys := range sets {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			conn := pgtest.New(t).Connect(t)

			first, err := migrate.Up(t.Context(), conn, fsys)
			if err != nil {
				t.Fatalf("first Up: %v", err)
			}
			all, err := migrate.Load(fsys)
			if err != nil {
				t.Fatal(err)
			}
			if len(first) != len(all) {
				t.Fatalf("first Up applied %d migrations, want %d", len(first), len(all))
			}
			before := snapshot(t, conn)

			second, err := migrate.Up(t.Context(), conn, fsys)
			if err != nil {
				t.Fatalf("second Up: %v", err)
			}
			if len(second) != 0 {
				t.Fatalf("second Up applied %v, want nothing", versions(second))
			}
			if after := snapshot(t, conn); after != before {
				t.Fatalf("schema changed on the second run.\nbefore:\n%s\nafter:\n%s", before, after)
			}
		})
	}
}

// P1-4: runners started at once apply each migration exactly once.
func TestConcurrentRunnersApplyEachMigrationOnce(t *testing.T) {
	t.Parallel()
	const runners = 8
	fsys := withRealMigrations(t, map[string]string{
		// Not idempotent: a second application fails on CREATE TABLE or adds a second row.
		// The sleep keeps the first runner inside the migration while the others arrive.
		"0002_log.sql":  "CREATE TABLE apply_log (version int NOT NULL); INSERT INTO apply_log VALUES (2); SELECT pg_sleep(0.2);",
		"0003_more.sql": "INSERT INTO apply_log VALUES (3);",
	})
	all, err := migrate.Load(fsys)
	if err != nil {
		t.Fatal(err)
	}

	for round := range 3 {
		t.Run(fmt.Sprintf("round %d", round), func(t *testing.T) {
			t.Parallel()
			db := pgtest.New(t)
			conns := make([]*pgx.Conn, runners)
			for i := range conns {
				conns[i] = db.Connect(t)
			}

			start := make(chan struct{})
			results := make([][]migrate.Migration, runners)
			errs := make([]error, runners)
			var wg sync.WaitGroup
			for i := range runners {
				wg.Go(func() {
					<-start
					results[i], errs[i] = migrate.Up(t.Context(), conns[i], fsys)
				})
			}
			close(start)
			wg.Wait()

			applies := map[int64]int{}
			for i := range runners {
				if errs[i] != nil {
					t.Errorf("runner %d: %v", i, errs[i])
				}
				for _, m := range results[i] {
					applies[m.Version]++
				}
			}
			for _, m := range all {
				if applies[m.Version] != 1 {
					t.Errorf("migration %d applied by %d runners, want exactly 1", m.Version, applies[m.Version])
				}
			}

			check := conns[0]
			var logged string
			if err := check.QueryRow(t.Context(),
				"SELECT string_agg(version::text, ',' ORDER BY version) FROM apply_log").Scan(&logged); err != nil {
				t.Fatal(err)
			}
			if logged != "2,3" {
				t.Errorf("apply_log = %q, want \"2,3\" (each migration's effect exactly once)", logged)
			}
			var recorded int
			if err := check.QueryRow(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&recorded); err != nil {
				t.Fatal(err)
			}
			if recorded != len(all) {
				t.Errorf("schema_migrations has %d rows, want %d", recorded, len(all))
			}
			var held int
			if err := check.QueryRow(t.Context(),
				`SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database
				  WHERE l.locktype = 'advisory' AND d.datname = current_database()`).Scan(&held); err != nil {
				t.Fatal(err)
			}
			if held != 0 {
				t.Errorf("%d advisory locks still held after every runner returned, want 0", held)
			}
		})
	}
}

// A runner waits while another holds the lock, and gives up when its context ends.
func TestUpWaitsForTheLockAndHonoursContext(t *testing.T) {
	t.Parallel()
	db := pgtest.New(t)
	holder, runner := db.Connect(t), db.Connect(t)
	if _, err := holder.Exec(t.Context(), "SELECT pg_advisory_lock(hashtextextended('queued:migrate', 0))"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	applied, err := migrate.Up(ctx, runner, migrations.FS)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Up while the lock is held = (%v, %v), want a deadline error", versions(applied), err)
	}
	var exists bool
	if err := holder.QueryRow(t.Context(), "SELECT to_regclass('schema_migrations') IS NOT NULL").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("Up applied migrations without holding the lock")
	}

	if _, err := holder.Exec(t.Context(), "SELECT pg_advisory_unlock(hashtextextended('queued:migrate', 0))"); err != nil {
		t.Fatal(err)
	}
	// The cancelled query may have broken runner's connection, so use a fresh one.
	if _, err := migrate.Up(t.Context(), db.Connect(t), migrations.FS); err != nil {
		t.Fatalf("Up after the lock was released: %v", err)
	}
}

func TestUpRollsBackAFailedMigration(t *testing.T) {
	t.Parallel()
	conn := pgtest.New(t).Connect(t)
	broken := withRealMigrations(t, map[string]string{
		"0002_broken.sql": "CREATE TABLE half (id int); SELECT 1/0;",
		"0003_after.sql":  "CREATE TABLE after_broken (id int);",
	})
	applied, err := migrate.Up(t.Context(), conn, broken)
	if err == nil || !strings.Contains(err.Error(), "0002_broken.sql") {
		t.Fatalf("Up = %v, want an error naming 0002_broken.sql", err)
	}
	if got := fmt.Sprint(versions(applied)); got != "[1]" {
		t.Fatalf("applied %s before the failure, want [1]", got)
	}
	for _, table := range []string{"half", "after_broken"} {
		var exists bool
		if err := conn.QueryRow(t.Context(), "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Errorf("table %s exists after the failed run, want it rolled back or never created", table)
		}
	}
	var recorded int
	if err := conn.QueryRow(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != 1 {
		t.Fatalf("schema_migrations has %d rows, want 1", recorded)
	}

	fixed := withRealMigrations(t, map[string]string{
		"0002_broken.sql": "CREATE TABLE half (id int);",
		"0003_after.sql":  "CREATE TABLE after_broken (id int);",
	})
	applied, err = migrate.Up(t.Context(), conn, fixed)
	if err != nil {
		t.Fatalf("Up after the fix: %v", err)
	}
	if got := fmt.Sprint(versions(applied)); got != "[2 3]" {
		t.Fatalf("applied %s after the fix, want [2 3]", got)
	}
}

func TestUpRefusesInconsistentHistory(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"0002_a.sql": "CREATE TABLE a (id int);",
		"0004_c.sql": "CREATE TABLE c (id int);",
	}
	tests := []struct {
		name    string
		next    map[string]string
		wantErr string
	}{
		{
			name:    "applied migration edited",
			next:    map[string]string{"0002_a.sql": "CREATE TABLE a (id bigint);", "0004_c.sql": base["0004_c.sql"], "0005_d.sql": "CREATE TABLE d (id int);"},
			wantErr: "changed after it was applied",
		},
		{
			name:    "applied migration missing",
			next:    map[string]string{"0002_a.sql": base["0002_a.sql"], "0005_d.sql": "CREATE TABLE d (id int);"},
			wantErr: "doesn't have",
		},
		{
			name:    "new migration older than the newest applied",
			next:    map[string]string{"0002_a.sql": base["0002_a.sql"], "0003_b.sql": "CREATE TABLE b (id int);", "0004_c.sql": base["0004_c.sql"]},
			wantErr: "older than applied migration 4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conn := pgtest.New(t).Connect(t)
			if _, err := migrate.Up(t.Context(), conn, withRealMigrations(t, base)); err != nil {
				t.Fatalf("setup Up: %v", err)
			}
			before := snapshot(t, conn)

			applied, err := migrate.Up(t.Context(), conn, withRealMigrations(t, tt.next))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Up = %v, want an error containing %q", err, tt.wantErr)
			}
			if len(applied) != 0 {
				t.Errorf("Up applied %v despite refusing, want nothing", versions(applied))
			}
			if after := snapshot(t, conn); after != before {
				t.Errorf("schema changed although Up refused.\nbefore:\n%s\nafter:\n%s", before, after)
			}
		})
	}
}

// snapshot renders the public schema (tables, columns, indexes and constraints),
// the schema_migrations rows, and every table's row count as text, so two
// snapshots are equal exactly when nothing observable changed.
func snapshot(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var b strings.Builder
	for _, q := range []string{
		`SELECT table_name, column_name, data_type, is_nullable, coalesce(column_default, '')
		   FROM information_schema.columns WHERE table_schema = 'public' ORDER BY 1, 2`,
		`SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY 1`,
		`SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid)
		   FROM pg_constraint WHERE connamespace = 'public'::regnamespace ORDER BY 1, 2`,
		`SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version`,
	} {
		rows, err := conn.Query(t.Context(), q)
		if err != nil {
			t.Fatalf("snapshot query: %v", err)
		}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("snapshot row: %v", err)
			}
			fmt.Fprintln(&b, vals...)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("snapshot rows: %v", err)
		}
		b.WriteString("--\n")
	}

	rows, err := conn.Query(t.Context(),
		"SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1")
	if err != nil {
		t.Fatalf("snapshot tables: %v", err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("snapshot tables: %v", err)
	}
	for _, table := range tables {
		var n int64
		if err := conn.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&n); err != nil {
			t.Fatalf("snapshot count %s: %v", table, err)
		}
		fmt.Fprintln(&b, table, n)
	}
	return b.String()
}
