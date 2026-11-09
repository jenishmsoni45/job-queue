package invariants

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/dispatcher"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// P2-3, basic Q1: jobs are enqueued, and worker processes (goroutines with
// their own connection pools) are killed mid-run at random, along with their
// Postgres connections, and replaced. Every accepted job ends completed or
// dead. Set Q1_SEED to replay a run's kill schedule.
func TestQ1BasicNoAcceptedJobIsLost(t *testing.T) {
	t.Parallel()
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("Q1_SEED"); s != "" {
		var err error
		if seed, err = strconv.ParseUint(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("Q1_SEED=%d", seed)
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) //nolint:gosec // a test schedule, not a secret

	env := queuetest.New(t, queuetest.Config(t, func(c *config.File) {
		shortLeases(400, 100, 6)(c)
		for i := range c.Kinds {
			c.Kinds[i].TimeoutMS = 2000
		}
	}))
	d, err := dispatcher.New(dispatcher.Config{
		DB: env.Pool,
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			return pgx.ConnectConfig(ctx, env.DB.ConnConfig())
		},
		Refresh: 50 * time.Millisecond, LongPoll: time.Second, ReaperInterval: 50 * time.Millisecond,
		ReaperBatch: 500, StandbyRetry: 100 * time.Millisecond, Log: queuetest.Logger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	dctx, stopDispatcher := context.WithCancel(context.Background())
	dispatcherDone := make(chan struct{})
	go func() {
		defer close(dispatcherDone)
		_ = d.Run(dctx)
	}()
	mux := http.NewServeMux()
	mux.Handle("POST "+dispatcher.ClaimPath, d)
	srv := httptest.NewServer(mux)
	defer func() {
		stopDispatcher()
		<-dispatcherDone
		srv.Close()
	}()

	// The workload: quick successes, retries, permanent failures and panics.
	const jobs = 300
	ids := make([]int64, 0, jobs)
	for i := range jobs {
		shop := []string{shopA, shopB, shopC}[i%3]
		var kind string
		var payload testkinds.Payload
		switch r := rng.IntN(100); {
		case r < 60:
			kind, payload = testkinds.Sleep, testkinds.Payload{MS: 50 + rng.IntN(150)} // long enough that kills land mid-run
		case r < 85:
			kind, payload = testkinds.Fail, testkinds.Payload{FailTimes: rng.IntN(3)}
		case r < 92:
			kind, payload = testkinds.Fail, testkinds.Payload{FailTimes: -1, Permanent: true}
		default:
			kind = testkinds.Panic
		}
		ids = append(ids, env.Enqueue(t, shop, kind, payload))
	}

	reg := queue.NewRegistry()
	testkinds.Register(reg)
	var (
		mu      sync.Mutex
		live    []*procWorker
		next    int
		kills   int
		reports = queuetest.NewReports()
	)
	spawn := func() {
		next++
		w := startProcWorker(t, env, srv.URL, reg, fmt.Sprintf("q1-worker-%d", next), reports)
		live = append(live, w)
	}
	mu.Lock()
	for range 4 {
		spawn()
	}
	mu.Unlock()

	done := func() bool {
		var open int
		if err := env.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')`).Scan(&open); err != nil {
			t.Fatal(err)
		}
		return open == 0
	}
	deadline := time.Now().Add(90 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("jobs still unfinished after 90s (Q1_SEED=%d)", seed)
		}
		time.Sleep(time.Duration(50+rng.IntN(200)) * time.Millisecond)
		if kills < 25 {
			mu.Lock()
			victim := rng.IntN(len(live))
			w := live[victim]
			live = append(live[:victim], live[victim+1:]...)
			kills++
			spawn()
			mu.Unlock()
			w.kill(t, env)
		}
	}
	mu.Lock()
	for _, w := range live {
		w.stop()
	}
	mu.Unlock()

	// Q1: every accepted job ended completed or dead.
	states := map[string]int{}
	for _, id := range ids {
		j := env.Job(t, id)
		states[j.State]++
		if j.State != store.StateCompleted && j.State != store.StateDead {
			t.Errorf("job %d ended %s (Q1_SEED=%d)", id, j.State, seed)
		}
		if j.State == store.StateDead && (j.DeadReason == nil || j.LastError == nil) {
			t.Errorf("dead job %d has no reason or last error: %+v", id, j)
		}
	}
	env.CheckBacklog(t)
	abandoned := 0
	for _, r := range reports.All() {
		if r.Outcome == queue.OutcomeAbandoned {
			abandoned++
		}
	}
	t.Logf("%d jobs: %v; %d workers killed, %d runs cut off by a kill", len(ids), states, kills, abandoned)
	if kills == 0 || abandoned == 0 {
		t.Errorf("no run was cut off by a kill (kills %d), so the test proved nothing; Q1_SEED=%d", kills, seed)
	}
}

// procWorker is a worker standing in for a process: its own pool, its own
// connections (named so they can be killed), and its own lifetime.
type procWorker struct {
	name string
	w    *queue.Worker
	pool *pgxpool.Pool
	stop func()
	done chan struct{}
}

func startProcWorker(t *testing.T, env *queuetest.Env, url string, reg *queue.Registry, name string, reports *queuetest.Reports) *procWorker {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig = env.DB.ConnConfig()
	cfg.ConnConfig.RuntimeParams["application_name"] = name
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	w, err := queue.New(queue.Config{
		DB: pool, Claims: &dispatcher.Client{URL: url, HTTP: &http.Client{Timeout: 5 * time.Second}},
		Registry: reg, Slots: 3, HeartbeatInterval: 100 * time.Millisecond,
		Backoff:  queue.Backoff{Base: 10 * time.Millisecond, Cap: 100 * time.Millisecond},
		Identity: name, Log: queuetest.Logger(t), OnReport: reports.Add,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &procWorker{name: name, w: w, pool: pool, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		_ = w.Run(ctx)
	}()
	p.stop = func() {
		cancel()
		<-p.done
		pool.Close()
	}
	return p
}

// kill is kill -9 for an in-process worker: handlers are cancelled and nothing
// more is written, and its Postgres connections are terminated mid-statement.
func (p *procWorker) kill(t *testing.T, env *queuetest.Env) {
	t.Helper()
	p.w.Kill()
	if _, err := env.Pool.Exec(context.Background(), `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		 WHERE application_name = $1 AND datname = current_database()`, p.name); err != nil {
		t.Fatal(err)
	}
	p.stop()
}
