// Package pgtest gives each test its own empty database on the Compose
// Postgres, so tests never share state and can run in parallel.
//
// It connects with the standard libpq environment variables (PGHOST, PGUSER,
// PGPASSWORD and so on). `make test` runs the tests in the tools container on
// the Compose network, where those are set.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jenishmsoni45/job-queue/internal/migrate"
	"github.com/jenishmsoni45/job-queue/migrations"
)

const cleanupTimeout = 30 * time.Second

// DB is a throwaway database. It is dropped when the test ends.
type DB struct {
	Name   string
	config *pgx.ConnConfig
}

// New creates an empty database and drops it, with any open connections, when
// the test and its subtests finish.
func New(t testing.TB) *DB {
	t.Helper()
	if os.Getenv("PGHOST") == "" {
		t.Fatal("PGHOST is not set. Run the tests with `make test`, which runs them next to Postgres on the Compose network.")
	}
	name := "test_" + randomHex(t, 8)
	ident := pgx.Identifier{name}.Sanitize()

	admin := connectAdmin(t, t.Context())
	defer func() { _ = admin.Close(context.Background()) }()
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+ident); err != nil {
		t.Fatalf("pgtest: create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		admin := connectAdmin(t, ctx)
		defer func() { _ = admin.Close(ctx) }()
		if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop database %s: %v", name, err)
		}
	})

	cfg, err := pgx.ParseConfig("")
	if err != nil {
		t.Fatalf("pgtest: parse connection settings: %v", err)
	}
	cfg.Database = name
	return &DB{Name: name, config: cfg}
}

// Connect opens a new connection to the database. It is closed when the test ends.
func (d *DB) Connect(t testing.TB) *pgx.Conn {
	t.Helper()
	conn, err := pgx.ConnectConfig(t.Context(), d.config.Copy())
	if err != nil {
		t.Fatalf("pgtest: connect to %s: %v", d.Name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_ = conn.Close(ctx)
	})
	return conn
}

// NewMigrated creates an empty database, as New does, and applies every
// migration to it.
func NewMigrated(t testing.TB) *DB {
	t.Helper()
	d := New(t)
	conn := d.Connect(t)
	if _, err := migrate.Up(t.Context(), conn, migrations.FS); err != nil {
		t.Fatalf("pgtest: migrate %s: %v", d.Name, err)
	}
	if err := conn.Close(t.Context()); err != nil {
		t.Fatalf("pgtest: close %s: %v", d.Name, err)
	}
	return d
}

// ConnConfig returns a copy of the database's connection settings.
func (d *DB) ConnConfig() *pgx.ConnConfig { return d.config.Copy() }

// Pool opens a connection pool to the database, with every connection tagged
// with the application name, so a test can find and kill them in
// pg_stat_activity. The pool is closed when the test ends.
func (d *DB) Pool(t testing.TB, application string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		t.Fatalf("pgtest: parse pool settings: %v", err)
	}
	cfg.ConnConfig = d.config.Copy()
	if application != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = application
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("pgtest: pool for %s: %v", d.Name, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// ConnectAdmin opens a connection to the default database from the environment,
// for checks of server-wide settings. It is closed when the test ends.
func ConnectAdmin(t testing.TB) *pgx.Conn {
	t.Helper()
	if os.Getenv("PGHOST") == "" {
		t.Fatal("PGHOST is not set. Run the tests with `make test`, which runs them next to Postgres on the Compose network.")
	}
	conn := connectAdmin(t, t.Context())
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func connectAdmin(t testing.TB, ctx context.Context) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Fatalf("pgtest: connect to Postgres: %v", err)
	}
	return conn
}

func randomHex(t testing.TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("pgtest: random name: %v", err)
	}
	return hex.EncodeToString(b)
}
