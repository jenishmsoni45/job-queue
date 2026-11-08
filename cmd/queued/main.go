// Command queued is the queue server. Each process runs one role (D-014):
//
//	queued api          enqueue and read jobs (POST /v1/jobs, GET /v1/jobs/{id})
//	queued dispatcher   leader lock, claims for long-polling workers, reaper
//	queued worker       handler pool, batched heartbeats, fenced results
//
// and it has two one-shot commands:
//
//	queued migrate      apply the SQL migrations, then exit
//	queued healthcheck  exit 0 if this container's server answers GET /healthz
//
// Every role serves GET /healthz. Settings come from the environment (design §2),
// with the shop and kind settings in the config file (design §11.4):
//
//	QUEUED_HTTP_ADDR          listen address (":8080")
//	QUEUED_CONFIG             config file (/etc/queued/shops.json); every role validates it
//	QUEUED_DISPATCHER_URL     worker: the dispatcher's base URL (http://dispatcher:8080)
//	QUEUED_WORKER_SLOTS       worker: handler goroutines (8)
//	QUEUED_TEST_HANDLERS      worker: "1" registers the test kinds (internal/testkinds)
//	QUEUED_REFRESH_MS         dispatcher: claim retry when nothing was notified (250)
//	QUEUED_CLAIM_TIMEOUT_MS   dispatcher and worker: claim long-poll timeout (25000)
//	QUEUED_REAPER_SCAN_MS     dispatcher: reaper scan (5000)
//	QUEUED_BACKOFF_BASE_MS    worker: retry backoff base (2000)
//	QUEUED_BACKOFF_CAP_MS     worker: retry backoff cap (300000)
//	QUEUED_DRAIN_MS           worker: on SIGTERM, how long in-flight runs may finish (8000)
//
// The standard libpq variables (PGHOST, PGUSER, PGPASSWORD, ...) locate Postgres.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/dispatcher"
	"github.com/jenishmsoni45/job-queue/internal/httpserver"
	"github.com/jenishmsoni45/job-queue/internal/migrate"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
	"github.com/jenishmsoni45/job-queue/migrations"
)

const (
	defaultAddr        = ":8080"
	migrateTimeout     = 2 * time.Minute
	healthcheckTimeout = 2 * time.Second
	closeTimeout       = 5 * time.Second
	seedTimeout        = 30 * time.Second
)

const usage = `usage: queued <command>

roles (long-running):
  api           enqueue and read API
  dispatcher    leader lock, claims and reaper
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
		if err := runRole(ctx, cmd, addr, getenv, log); err != nil {
			log.Error("role failed", "err", err.Error())
			return 1
		}
		return 0
	case "migrate":
		if err := runMigrate(ctx, getenv, log); err != nil {
			log.Error("migrate failed", "err", err.Error())
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

// settings are the environment settings of the roles.
type settings struct {
	configPath    string
	dispatcherURL string
	slots         int
	testHandlers  bool
	refresh       time.Duration
	longPoll      time.Duration
	reaperScan    time.Duration
	backoff       queue.Backoff
	drain         time.Duration
}

func loadSettings(getenv func(string) string) (settings, error) {
	s := settings{
		configPath:    getenv("QUEUED_CONFIG"),
		dispatcherURL: getenv("QUEUED_DISPATCHER_URL"),
		testHandlers:  getenv("QUEUED_TEST_HANDLERS") == "1",
	}
	if s.configPath == "" {
		s.configPath = "/etc/queued/shops.json"
	}
	if s.dispatcherURL == "" {
		s.dispatcherURL = "http://dispatcher:8080"
	}
	var errs []error
	num := func(name string, def int) int {
		v := getenv(name)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive integer, got %q", name, v))
			return def
		}
		return n
	}
	ms := func(name string, def int) time.Duration { return time.Duration(num(name, def)) * time.Millisecond }
	s.slots = num("QUEUED_WORKER_SLOTS", 8)
	s.refresh = ms("QUEUED_REFRESH_MS", 250)
	s.longPoll = ms("QUEUED_CLAIM_TIMEOUT_MS", 25_000)
	s.reaperScan = ms("QUEUED_REAPER_SCAN_MS", 5_000)
	s.backoff = queue.Backoff{Base: ms("QUEUED_BACKOFF_BASE_MS", 2_000), Cap: ms("QUEUED_BACKOFF_CAP_MS", 300_000)}
	s.drain = ms("QUEUED_DRAIN_MS", 8_000)
	if err := s.backoff.Validate(); err != nil {
		errs = append(errs, err)
	}
	return s, errors.Join(errs...)
}

// runRole validates the settings and the config file, then runs the role until
// ctx is done (design §11.4: every process fails fast on a bad config).
func runRole(ctx context.Context, role, addr string, getenv func(string) string, log *slog.Logger) error {
	s, err := loadSettings(getenv)
	if err != nil {
		return err
	}
	cfg, err := config.Load(s.configPath)
	if err != nil {
		return err
	}
	connCfg, err := connConfig(getenv)
	if err != nil {
		return err
	}
	switch role {
	case "api":
		return runAPI(ctx, addr, cfg, connCfg, log)
	case "dispatcher":
		return runDispatcher(ctx, addr, s, connCfg, log)
	default:
		return runWorker(ctx, addr, s, cfg, connCfg, log)
	}
}

// connConfig reads the PG* variables. PGDATABASE from getenv wins, so tests can
// point a role at their own database.
func connConfig(getenv func(string) string) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig("") // settings come from the PG* environment variables
	if err != nil {
		return nil, fmt.Errorf("read the Postgres settings: %w", err)
	}
	if db := getenv("PGDATABASE"); db != "" {
		cfg.Database = db
	}
	return cfg, nil
}

func openPool(ctx context.Context, connCfg *pgx.ConnConfig, app string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("pool settings: %w", err)
	}
	cfg.ConnConfig = connCfg.Copy()
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to Postgres: %w", err)
	}
	return pool, nil
}

// runAPI seeds shops and kinds from the config file (D-019), then serves the
// job endpoints.
func runAPI(ctx context.Context, addr string, cfg *config.File, connCfg *pgx.ConnConfig, log *slog.Logger) error {
	pool, err := openPool(ctx, connCfg, "queued-api", 0)
	if err != nil {
		return err
	}
	defer pool.Close()
	seedCtx, cancel := context.WithTimeout(ctx, seedTimeout)
	err = store.Seed(seedCtx, pool, cfg)
	cancel()
	if err != nil {
		return err
	}
	log.Info("seeded config", "shops", len(cfg.Shops), "kinds", len(cfg.Kinds))

	mux := httpserver.NewMux("queued api")
	api.New(pool, log).Register(mux)
	return httpserver.ListenAndServe(ctx, addr, mux, log)
}

// runDispatcher campaigns for the leader lock and serves the claim long-poll.
func runDispatcher(ctx context.Context, addr string, s settings, connCfg *pgx.ConnConfig, log *slog.Logger) error {
	pool, err := openPool(ctx, connCfg, "queued-dispatcher", 0)
	if err != nil {
		return err
	}
	defer pool.Close()
	leaderCfg := connCfg.Copy()
	leaderCfg.RuntimeParams["application_name"] = "queued-dispatcher-leader"
	d, err := dispatcher.New(dispatcher.Config{
		DB:             pool,
		Connect:        func(ctx context.Context) (*pgx.Conn, error) { return pgx.ConnectConfig(ctx, leaderCfg.Copy()) },
		Refresh:        s.refresh,
		LongPoll:       s.longPoll,
		ReaperInterval: s.reaperScan,
		ReaperBatch:    500, // design §9
		StandbyRetry:   time.Second,
		Log:            log,
	})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = d.Run(runCtx)
	}()

	mux := httpserver.NewMux("queued dispatcher")
	mux.Handle("POST "+dispatcher.ClaimPath, d)
	err = httpserver.ListenAndServe(runCtx, addr, mux, log)
	cancel()
	<-done
	return err
}

// runWorker runs the handler pool. On SIGTERM it stops claiming and lets
// in-flight runs finish for up to the drain time, then stops them; their leases
// expire and the reaper requeues the jobs.
func runWorker(ctx context.Context, addr string, s settings, cfg *config.File, connCfg *pgx.ConnConfig, log *slog.Logger) error {
	pool, err := openPool(ctx, connCfg, "queued-worker", int32(min(s.slots*2+4, 1000))) //nolint:gosec // bounded by min
	if err != nil {
		return err
	}
	defer pool.Close()

	reg := queue.NewRegistry()
	if s.testHandlers {
		testkinds.Register(reg)
	}
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("hostname: %w", err)
	}
	w, err := queue.New(queue.Config{
		DB:                pool,
		Claims:            &dispatcher.Client{URL: s.dispatcherURL, HTTP: &http.Client{Timeout: s.longPoll + 15*time.Second}},
		Registry:          reg,
		Slots:             s.slots,
		HeartbeatInterval: cfg.MinHeartbeat(),
		Backoff:           s.backoff,
		Identity:          fmt.Sprintf("%s/%d", host, os.Getpid()),
		Log:               log,
	})
	if err != nil {
		return err
	}
	log.Info("worker starting", "slots", s.slots, "kinds", reg.Kinds())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(runCtx)
	}()
	go func() {
		select {
		case <-done:
			return
		case <-runCtx.Done():
		}
		t := time.NewTimer(s.drain)
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
			log.Warn("drain time passed; stopping in-flight runs")
			w.Kill()
		}
	}()

	err = httpserver.ListenAndServe(runCtx, addr, httpserver.NewMux("queued worker"), log)
	if err != nil {
		w.Kill()
	}
	cancel()
	<-done
	return err
}

// runMigrate applies every pending migration (design §3). Runners started at
// the same time are serialized by migrate.Up's advisory lock.
func runMigrate(ctx context.Context, getenv func(string) string, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, migrateTimeout)
	defer cancel()
	cfg, err := connConfig(getenv)
	if err != nil {
		return err
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
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
