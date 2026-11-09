// Package queuetest holds helpers shared by the queue's tests: a migrated and
// seeded database, an in-process claim source, a worker harness that collects
// run reports, and polling helpers. Only tests import it.
package queuetest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/pgtest"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/store"
)

// Shops in tests/testdata/shops.json.
const (
	ShopA = "shop-a.test"
	ShopB = "shop-b.test"
	ShopC = "shop-c.test"
)

// RepoRoot returns the repository's root directory.
func RepoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("queuetest: cannot locate the repo root")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// Config loads tests/testdata/shops.json, applies edit (if any) and validates
// the result.
func Config(t testing.TB, edit func(*config.File)) *config.File {
	t.Helper()
	cfg, err := config.Load(filepath.Join(RepoRoot(t), "tests", "testdata", "shops.json"))
	if err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(cfg)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("queuetest: edited config is invalid: %v", err)
		}
	}
	return cfg
}

// SetKind changes the settings of one kind in cfg.
func SetKind(cfg *config.File, kind string, set func(*config.Kind)) {
	for i := range cfg.Kinds {
		if cfg.Kinds[i].Kind == kind {
			set(&cfg.Kinds[i])
			return
		}
	}
	panic("queuetest: no kind " + kind)
}

// Env is a migrated, seeded test database.
type Env struct {
	DB     *pgtest.DB
	Pool   *pgxpool.Pool
	Config *config.File
}

// New creates a migrated database seeded with cfg (or the test config if nil).
func New(t testing.TB, cfg *config.File) *Env {
	t.Helper()
	if cfg == nil {
		cfg = Config(t, nil)
	}
	db := pgtest.NewMigrated(t)
	pool := db.Pool(t, "queuetest", 40)
	if err := store.Seed(t.Context(), pool, cfg); err != nil {
		t.Fatal(err)
	}
	return &Env{DB: db, Pool: pool, Config: cfg}
}

// Enqueue enqueues a job for shop with the payload encoded as JSON.
func (e *Env) Enqueue(t testing.TB, shop, kind string, payload any) int64 {
	t.Helper()
	return e.EnqueueAt(t, shop, kind, payload, nil)
}

// EnqueueAt enqueues a job with a run_at.
func (e *Env) EnqueueAt(t testing.TB, shop, kind string, payload any, runAt *time.Time) int64 {
	t.Helper()
	if payload == nil {
		payload = map[string]any{}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	id, err := store.Enqueue(t.Context(), e.Pool, store.NewJob{Shop: shop, Kind: kind, Payload: body, RunAt: runAt})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id
}

// Job reads one job.
func (e *Env) Job(t testing.TB, id int64) store.Job {
	t.Helper()
	j, err := store.GetJob(context.Background(), e.Pool, id)
	if err != nil {
		t.Fatalf("get job %d: %v", id, err)
	}
	return j
}

// CheckBacklog fails the test unless every shop's backlog counter equals its
// real number of queued and running jobs (design §3).
func (e *Env) CheckBacklog(t testing.TB) {
	t.Helper()
	rows, err := e.Pool.Query(context.Background(), `
		SELECT s.shop_id, s.backlog,
		       (SELECT count(*) FROM jobs j WHERE j.shop_id = s.shop_id AND j.state IN ('queued', 'running'))
		  FROM shop_state s ORDER BY s.shop_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var shop string
		var counter, actual int64
		if err := rows.Scan(&shop, &counter, &actual); err != nil {
			t.Fatal(err)
		}
		if counter != actual {
			t.Errorf("shop %s: backlog counter %d, real queued+running %d", shop, counter, actual)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// DBNow returns the database clock.
func (e *Env) DBNow(t testing.TB) time.Time {
	t.Helper()
	var now time.Time
	if err := e.Pool.QueryRow(context.Background(), `SELECT statement_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

// DirectClaims is a queue.ClaimSource that claims straight from the database
// with the real claim SQL, polling every 5 ms, in place of the dispatcher.
type DirectClaims struct {
	DB store.DB
	// Wait is how long one Claim call polls before returning nothing (default 100 ms).
	Wait time.Duration
}

// Claim implements queue.ClaimSource. The poll deadline only stops the
// polling: a claim transaction always runs under the caller's context, because
// one cut off at its commit could lease a job that no worker knows it holds.
func (d DirectClaims) Claim(ctx context.Context, owner string) (*store.Claimed, error) {
	wait := d.Wait
	if wait == 0 {
		wait = 100 * time.Millisecond
	}
	poll := time.NewTimer(wait)
	defer poll.Stop()
	for {
		jobs, err := store.Claim(ctx, d.DB, []string{owner})
		if len(jobs) == 1 {
			return &jobs[0], nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil
			}
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, nil
		case <-poll.C:
			return nil, nil
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Reports collects worker run reports.
type Reports struct {
	mu   sync.Mutex
	list []queue.Report
	ch   chan queue.Report
}

// NewReports returns an empty collector.
func NewReports() *Reports { return &Reports{ch: make(chan queue.Report, 10000)} }

// Add records one report. Use it as queue.Config.OnReport.
func (r *Reports) Add(rep queue.Report) {
	r.mu.Lock()
	r.list = append(r.list, rep)
	r.mu.Unlock()
	select {
	case r.ch <- rep:
	default:
	}
}

// All returns every report so far.
func (r *Reports) All() []queue.Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]queue.Report(nil), r.list...)
}

// Next waits for the next report.
func (r *Reports) Next(t testing.TB, timeout time.Duration) queue.Report {
	t.Helper()
	select {
	case rep := <-r.ch:
		return rep
	case <-time.After(timeout):
		t.Fatalf("no run report within %v", timeout)
		return queue.Report{}
	}
}

// WorkerOptions adjusts a test worker's config.
type WorkerOptions func(*queue.Config)

// StartWorker starts a worker on e with registry reg and returns it with its
// report collector. Logs go to the test log only when the test fails. The
// worker is drained and stopped when the test ends.
func (e *Env) StartWorker(t testing.TB, reg *queue.Registry, opts ...WorkerOptions) (*queue.Worker, *Reports) {
	t.Helper()
	reports := NewReports()
	cfg := queue.Config{
		DB:                e.Pool,
		Claims:            DirectClaims{DB: e.Pool},
		Registry:          reg,
		Slots:             2,
		HeartbeatInterval: e.Config.MinHeartbeat(),
		Backoff:           queue.Backoff{Base: 20 * time.Millisecond, Cap: 200 * time.Millisecond},
		Identity:          "test/1",
		Log:               Logger(t),
		OnReport:          reports.Add,
	}
	for _, o := range opts {
		o(&cfg)
	}
	w, err := queue.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			w.Kill()
			<-done
		}
	})
	return w, reports
}

// Logger returns a logger that writes to the test log when -v is set, and
// discards otherwise.
func Logger(t testing.TB) *slog.Logger {
	if testing.Verbose() && os.Getenv("QUEUETEST_LOGS") == "1" {
		return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// Eventually polls cond every 10 ms until it holds, failing the test after timeout.
func Eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WaitState waits until job id is in state.
func (e *Env) WaitState(t testing.TB, id int64, state string, timeout time.Duration) store.Job {
	t.Helper()
	var j store.Job
	Eventually(t, timeout, "job to be "+state, func() bool {
		j = e.Job(t, id)
		return j.State == state
	})
	return j
}
