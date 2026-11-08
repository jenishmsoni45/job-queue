// Package migrate applies SQL migrations to Postgres (design §3). It is the
// project's own small runner, because D-001 rules out a migration tool.
//
// Up takes a session advisory lock, so runners started at the same time apply
// each migration exactly once: the first applies, and the rest wait and then
// find nothing to do. Each migration runs in its own transaction together with
// its schema_migrations row, so a migration is either fully applied and
// recorded, or not applied at all.
package migrate

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// lockName names the advisory lock that serializes runners. Every runner of this
// project uses the same name, so it maps to the same lock key.
const lockName = "queued:migrate"

// unlockTimeout bounds the unlock after Up returns, even if ctx was cancelled.
const unlockTimeout = 5 * time.Second

// fileName is NNNN_name.sql: a version of at least four digits and a lowercase name.
var fileName = regexp.MustCompile(`^([0-9]{4,})_([a-z0-9_]+)\.sql$`)

// Migration is one SQL file.
type Migration struct {
	Version  int64
	Name     string
	File     string
	SQL      string
	Checksum string // hex SHA-256 of SQL
}

// Load reads every *.sql file at the root of fsys and returns them in version
// order. Every .sql file must be named NNNN_name.sql, versions must be unique and
// positive, and there must be at least one file.
func Load(fsys fs.FS) ([]Migration, error) {
	files, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("migrate: list migrations: %w", err)
	}
	if len(files) == 0 {
		return nil, errors.New("migrate: no migration files found")
	}
	var ms []Migration
	seen := map[int64]string{}
	for _, f := range files {
		match := fileName.FindStringSubmatch(path.Base(f))
		if match == nil {
			return nil, fmt.Errorf("migrate: %s: file name must look like 0001_name.sql", f)
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migrate: %s: version must be a positive integer", f)
		}
		if other, dup := seen[version]; dup {
			return nil, fmt.Errorf("migrate: %s and %s have the same version %d", other, f, version)
		}
		seen[version] = f
		body, err := fs.ReadFile(fsys, f)
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", f, err)
		}
		sum := sha256.Sum256(body)
		ms = append(ms, Migration{
			Version:  version,
			Name:     match[2],
			File:     f,
			SQL:      string(body),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	slices.SortFunc(ms, func(a, b Migration) int { return cmp.Compare(a.Version, b.Version) })
	return ms, nil
}

// Up applies every migration in fsys that the database doesn't have yet, in
// version order, and returns the ones it applied.
//
// It refuses to run, applying nothing, if the database has a migration that
// fsys lacks, if an applied migration's file has changed since, or if a pending
// migration is older than the newest applied one.
//
// Up holds a session advisory lock on conn while it runs and releases it before
// returning. If the unlock fails (for example because ctx was cancelled and the
// connection broke), closing conn releases the lock; callers should close conn
// when Up returns an error.
func Up(ctx context.Context, conn *pgx.Conn, fsys fs.FS) ([]Migration, error) {
	all, err := Load(fsys)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", lockName); err != nil {
		return nil, fmt.Errorf("migrate: take advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", lockName)
	}()

	applied, err := appliedMigrations(ctx, conn)
	if err != nil {
		return nil, err
	}
	pending, err := plan(all, applied)
	if err != nil {
		return nil, err
	}

	var done []Migration
	for _, m := range pending {
		if err := apply(ctx, conn, m); err != nil {
			return done, err
		}
		done = append(done, m)
	}
	return done, nil
}

// record is one row of schema_migrations.
type record struct {
	Version  int64
	Name     string
	Checksum string
}

// appliedMigrations returns the rows of schema_migrations in version order, or
// none if the table doesn't exist yet (the first migration creates it).
func appliedMigrations(ctx context.Context, conn *pgx.Conn) ([]record, error) {
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass('schema_migrations') IS NOT NULL").Scan(&exists); err != nil {
		return nil, fmt.Errorf("migrate: look for schema_migrations: %w", err)
	}
	if !exists {
		return nil, nil
	}
	rows, err := conn.Query(ctx, "SELECT version, name, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("migrate: read schema_migrations: %w", err)
	}
	recs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[record])
	if err != nil {
		return nil, fmt.Errorf("migrate: read schema_migrations: %w", err)
	}
	return recs, nil
}

// plan checks the applied records against the files and returns the files still
// to apply, in the order of all, which Load sorts by version.
func plan(all []Migration, applied []record) ([]Migration, error) {
	byVersion := make(map[int64]Migration, len(all))
	for _, m := range all {
		byVersion[m.Version] = m
	}
	done := make(map[int64]bool, len(applied))
	var newest int64
	for _, r := range applied {
		m, ok := byVersion[r.Version]
		if !ok {
			return nil, fmt.Errorf("migrate: the database has migration %d (%s), which this binary doesn't have", r.Version, r.Name)
		}
		if m.Checksum != r.Checksum {
			return nil, fmt.Errorf("migrate: %s changed after it was applied; add a new migration instead", m.File)
		}
		done[r.Version] = true
		newest = max(newest, r.Version)
	}
	var pending []Migration
	for _, m := range all {
		if done[m.Version] {
			continue
		}
		if m.Version < newest {
			return nil, fmt.Errorf("migrate: %s is older than applied migration %d; give it a newer version", m.File, newest)
		}
		pending = append(pending, m)
	}
	return pending, nil
}

// apply runs one migration and records it, in one transaction.
func apply(ctx context.Context, conn *pgx.Conn, m Migration) (err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: %s: begin: %w", m.File, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	// No arguments, so pgx sends the file with the simple protocol, which allows
	// several statements in one call.
	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("migrate: %s: %w", m.File, err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
		m.Version, m.Name, m.Checksum); err != nil {
		return fmt.Errorf("migrate: %s: record: %w", m.File, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: %s: commit: %w", m.File, err)
	}
	return nil
}
