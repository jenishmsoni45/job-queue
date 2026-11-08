// Command queued is the queue server. Each process runs one role (D-014):
//
//	queued api          enqueue, webhook receiver, read API (later phases)
//	queued dispatcher   scheduler, claims, reaper, pruner (later phases)
//	queued worker       handler pool, heartbeats, fenced results (later phases)
//
// and it has two one-shot commands:
//
//	queued migrate      apply the SQL migrations, then exit
//	queued healthcheck  exit 0 if this container's server answers GET /healthz
//
// In phase 1 every role serves GET /healthz and nothing else.
//
// Environment: QUEUED_HTTP_ADDR is the listen address (default ":8080"), and the
// standard libpq variables (PGHOST, PGUSER, PGPASSWORD, ...) locate Postgres.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/httpserver"
	"github.com/jenishmsoni45/job-queue/internal/migrate"
	"github.com/jenishmsoni45/job-queue/migrations"
)

const (
	defaultAddr        = ":8080"
	migrateTimeout     = 2 * time.Minute
	healthcheckTimeout = 2 * time.Second
	closeTimeout       = 5 * time.Second
)

const usage = `usage: queued <command>

roles (long-running):
  api           enqueue and read API
  dispatcher    scheduler and claims
  worker        job handlers

one-shot:
  migrate       apply the SQL migrations, then exit
  healthcheck   exit 0 if this process's /healthz answers 200
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stderr)
	stop()
	os.Exit(code)
}

// run executes one command and returns the process exit code.
func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) int {
	if len(args) != 1 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	cmd := args[0]
	log := slog.New(slog.NewJSONHandler(stderr, nil)).With("service", "queued", "cmd", cmd)
	addr := getenv("QUEUED_HTTP_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	switch cmd {
	case "api", "dispatcher", "worker":
		if err := httpserver.ListenAndServe(ctx, addr, httpserver.NewMux("queued "+cmd), log); err != nil {
			log.Error("server failed", "err", err)
			return 1
		}
		return 0
	case "migrate":
		if err := runMigrate(ctx, log); err != nil {
			log.Error("migrate failed", "err", err)
			return 1
		}
		return 0
	case "healthcheck":
		if err := healthcheck(ctx, addr); err != nil {
			_, _ = fmt.Fprintln(stderr, "unhealthy:", err)
			return 1
		}
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "queued: unknown command %q\n\n%s", cmd, usage)
		return 2
	}
}

// runMigrate applies every pending migration (design §3). Runners started at
// the same time are serialized by migrate.Up's advisory lock.
func runMigrate(ctx context.Context, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, migrateTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, "") // settings come from the PG* environment variables
	if err != nil {
		return fmt.Errorf("connect to Postgres: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	applied, err := migrate.Up(ctx, conn, migrations.FS)
	for _, m := range applied {
		log.Info("applied migration", "version", m.Version, "name", m.Name)
	}
	if err != nil {
		return err
	}
	log.Info("schema is up to date", "applied_now", len(applied))
	return nil
}

func healthcheck(ctx context.Context, addr string) error {
	url, err := httpserver.HealthURL(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()
	return httpserver.Probe(ctx, url)
}
