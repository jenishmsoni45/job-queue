package dispatcher_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/dispatcher"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

const shopA = queuetest.ShopA

type running struct {
	d      *dispatcher.Dispatcher
	srv    *httptest.Server
	client *dispatcher.Client
	app    string // application_name of its leader connection
}

// start runs a dispatcher against env until the test ends.
func start(t *testing.T, env *queuetest.Env, name string, edit func(*dispatcher.Config)) *running {
	t.Helper()
	app := "dispatcher-test-" + name
	cfg := dispatcher.Config{
		DB: env.Pool,
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			c := env.DB.ConnConfig()
			c.RuntimeParams["application_name"] = app
			return pgx.ConnectConfig(ctx, c)
		},
		Refresh:        250 * time.Millisecond,
		LongPoll:       5 * time.Second,
		ReaperInterval: 100 * time.Millisecond,
		ReaperBatch:    500,
		StandbyRetry:   100 * time.Millisecond,
		PruneInterval:  time.Hour,
		PruneBatch:     5000,
		Retention:      queuetest.Retention,
		Log:            queuetest.Logger(t),
	}
	if edit != nil {
		edit(&cfg)
	}
	d, err := dispatcher.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = d.Run(ctx)
	}()
	mux := http.NewServeMux()
	mux.Handle("POST "+dispatcher.ClaimPath, d)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		cancel()
		<-done
		srv.Close()
	})
	return &running{d: d, srv: srv, client: &dispatcher.Client{URL: srv.URL, HTTP: srv.Client()}, app: app}
}

func waitLeader(t *testing.T, r *running) {
	t.Helper()
	queuetest.Eventually(t, 5*time.Second, "dispatcher to lead", r.d.IsLeader)
}

// post sends a raw claim request and returns its status.
func post(t *testing.T, r *running, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, r.srv.URL+dispatcher.ClaimPath, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := r.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// P2-8: a second dispatcher answers 503 while the leader holds the lock, and
// takes over after the leader's lock connection is closed.
func TestStandbyTakesOverWhenTheLeaderConnectionCloses(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	first := start(t, env, "first", nil)
	waitLeader(t, first)
	second := start(t, env, "second", nil)
	time.Sleep(300 * time.Millisecond) // a few standby retries
	if second.d.IsLeader() {
		t.Fatal("two dispatchers lead at once")
	}
	if status := post(t, second, `{"owner": "w"}`); status != http.StatusServiceUnavailable {
		t.Fatalf("standby answered %d, want 503", status)
	}

	// A worker waiting on the leader when it loses the lock is sent away with 503.
	waiting := make(chan error, 1)
	go func() {
		_, err := first.client.Claim(t.Context(), "w-waiting")
		waiting <- err
	}()
	time.Sleep(100 * time.Millisecond)

	var killed int
	if err := env.Pool.QueryRow(t.Context(), `
		SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
		 WHERE application_name = $1 AND datname = current_database()`, first.app).Scan(&killed); err != nil {
		t.Fatal(err)
	}
	if killed != 1 {
		t.Fatalf("terminated %d leader connections, want 1", killed)
	}
	select {
	case err := <-waiting:
		if !errors.Is(err, dispatcher.ErrNotLeader) {
			t.Errorf("waiting worker got %v, want ErrNotLeader", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting worker was not answered after the leader lost its lock")
	}

	waitLeader(t, second)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	job, err := second.client.Claim(t.Context(), "w-new")
	if err != nil || job == nil || job.ID != id {
		t.Fatalf("claim from the new leader = %+v, %v; want job %d", job, err, id)
	}
	// The old leader reconnects as a standby.
	time.Sleep(300 * time.Millisecond)
	if first.d.IsLeader() {
		t.Error("the old leader leads again while the new one holds the lock")
	}
	if _, err := first.client.Claim(t.Context(), "w"); !errors.Is(err, dispatcher.ErrNotLeader) {
		t.Errorf("old leader's claim = %v, want ErrNotLeader", err)
	}
}

func TestClaimLongPoll(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	r := start(t, env, "longpoll", func(c *dispatcher.Config) {
		c.LongPoll = 300 * time.Millisecond
		c.Refresh = time.Hour // only notifications and new waiters trigger a claim
	})
	waitLeader(t, r)

	begin := time.Now()
	job, err := r.client.Claim(t.Context(), "w")
	if err != nil || job != nil {
		t.Fatalf("claim on an empty queue = %+v, %v; want 204", job, err)
	}
	if waited := time.Since(begin); waited < 300*time.Millisecond {
		t.Errorf("204 after %v, want after the long-poll timeout", waited)
	}

	// An enqueue's notification wakes the dispatcher; the refresh never would.
	got := make(chan *store.Claimed, 1)
	go func() {
		job, _ := r.client.Claim(t.Context(), "w-notify")
		got <- job
	}()
	time.Sleep(100 * time.Millisecond)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	select {
	case job := <-got:
		if job == nil || job.ID != id || job.Owner != "w-notify" || job.LeaseMS != 1000 || job.HeartbeatMS != 200 {
			t.Fatalf("claimed %+v, want job %d for w-notify with its kind's lease settings", job, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the enqueue notification did not wake the dispatcher")
	}
}

// Each waiting worker gets its own job.
func TestWaitingWorkersGetDistinctJobs(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	r := start(t, env, "distinct", nil)
	waitLeader(t, r)
	const n = 6
	for range n {
		env.Enqueue(t, shopA, testkinds.Sleep, nil)
	}
	var mu sync.Mutex
	seen := map[int64]string{}
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			owner := fmt.Sprintf("w%d", i)
			job, err := r.client.Claim(t.Context(), owner)
			if err != nil || job == nil {
				t.Errorf("%s: claim = %+v, %v", owner, job, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if prev, dup := seen[job.ID]; dup {
				t.Errorf("job %d went to %s and %s", job.ID, prev, owner)
			}
			seen[job.ID] = owner
			if job.Owner != owner {
				t.Errorf("job %d leased to %s but answered to %s", job.ID, job.Owner, owner)
			}
		})
	}
	wg.Wait()
	if len(seen) != n {
		t.Errorf("%d distinct jobs claimed, want %d", len(seen), n)
	}
}

func TestClaimRejectsBadBodies(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	r := start(t, env, "bad", nil)
	waitLeader(t, r)
	for _, body := range []string{``, `{}`, `{"owner": ""}`, `{"owner": "w", "x": 1}`, `[]`,
		`{"owner": "` + strings.Repeat("w", 513) + `"}`} {
		if status := post(t, r, body); status != http.StatusBadRequest {
			t.Errorf("body %.40q: status %d, want 400", body, status)
		}
	}
}

// P2-5 (AC-3.1 and AC-3.2): a scheduled job is not claimed before run_at, and
// a waiting worker gets it within one refresh interval after run_at, plus the
// claim transaction's own time.
func TestScheduledJobIsClaimedWithinOneRefresh(t *testing.T) {
	t.Parallel()
	const refresh = 250 * time.Millisecond // design §2
	// claimAllowance covers the claim transaction and HTTP handling. It is a
	// test tolerance, not a setting.
	const claimAllowance = 150 * time.Millisecond
	env := queuetest.New(t, nil)
	r := start(t, env, "scheduled", func(c *dispatcher.Config) { c.Refresh = refresh })
	waitLeader(t, r)

	for i := range 3 {
		runAt := env.DBNow(t).Add(700 * time.Millisecond)
		id := env.EnqueueAt(t, shopA, testkinds.Sleep, nil, &runAt)
		job, err := r.client.Claim(t.Context(), fmt.Sprintf("w%d", i))
		if err != nil || job == nil || job.ID != id {
			t.Fatalf("claim = %+v, %v; want job %d", job, err, id)
		}
		if ok, err := store.Complete(t.Context(), env.Pool, job.Fence()); err != nil || !ok {
			t.Fatalf("complete = %v, %v", ok, err)
		}
		claimedAt := *env.Job(t, id).FirstClaimedAt
		if claimedAt.Before(runAt) {
			t.Errorf("claimed at %v, before run_at %v (AC-3.1)", claimedAt, runAt)
		}
		if late := claimedAt.Sub(runAt); late > refresh+claimAllowance {
			t.Errorf("claimed %v after run_at, want within one refresh (%v) plus %v (AC-3.2)", late, refresh, claimAllowance)
		}
	}
}

// AC-4.4 through the dispatcher: the leader's reaper requeues an expired lease
// at once with one more attempt, and a waiting worker gets the job again.
func TestReaperRequeuesExpiredLeases(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, func(c *config.File) {
		queuetest.SetKind(c, testkinds.Sleep, func(k *config.Kind) { k.LeaseMS, k.HeartbeatMS = 200, 50 })
	}))
	r := start(t, env, "reaper", func(c *dispatcher.Config) { c.Refresh = time.Hour })
	waitLeader(t, r)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	first, err := r.client.Claim(t.Context(), "w-crashed")
	if err != nil || first == nil || first.ID != id {
		t.Fatalf("first claim = %+v, %v", first, err)
	}
	// w-crashed never heartbeats. The reaper requeues the job and wakes the
	// claim loop, so the next worker gets it without waiting for a refresh.
	second, err := r.client.Claim(t.Context(), "w-next")
	if err != nil || second == nil || second.ID != id || second.ClaimSeq != 2 || second.Attempts != 1 {
		t.Fatalf("second claim = %+v, %v; want job %d at claim_seq 2 with 1 attempt", second, err, id)
	}
	if j := env.Job(t, id); *j.LastError != "lease expired" {
		t.Errorf("last_error = %v", j.LastError)
	}
}

// Design §9: the leader runs the pruner every PruneInterval. A completed job
// past its retention goes; a young one stays.
func TestLeaderRunsThePruner(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	old := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	young := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	for _, id := range []int64{old, young} {
		jobs, err := store.Claim(t.Context(), env.Pool, []string{"w"})
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim = %v, %v", jobs, err)
		}
		if ok, err := store.Complete(t.Context(), env.Pool, jobs[0].Fence()); err != nil || !ok {
			t.Fatalf("complete job %d = %v, %v", id, ok, err)
		}
	}
	if _, err := env.Pool.Exec(t.Context(), `UPDATE jobs SET completed_at = now() - interval '2 hours' WHERE id = $1`, old); err != nil {
		t.Fatal(err)
	}
	r := start(t, env, "pruner", func(c *dispatcher.Config) {
		c.PruneInterval = 50 * time.Millisecond
		c.Retention.Completed = time.Hour
	})
	waitLeader(t, r)
	queuetest.Eventually(t, 5*time.Second, "the pruner to delete the old job", func() bool {
		_, err := store.GetJob(t.Context(), env.Pool, old)
		return errors.Is(err, store.ErrNotFound)
	})
	if _, err := store.GetJob(t.Context(), env.Pool, young); err != nil {
		t.Errorf("the young completed job was pruned: %v", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	if _, err := dispatcher.New(dispatcher.Config{}); err == nil {
		t.Error("New with an empty config succeeded")
	}
}

func TestClientStatuses(t *testing.T) {
	for status, want := range map[int]string{
		http.StatusNoContent:           "",
		http.StatusServiceUnavailable:  dispatcher.ErrNotLeader.Error(),
		http.StatusInternalServerError: "unexpected status 500",
		http.StatusOK:                  "decode",
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r.Body)
			if r.Method != http.MethodPost || r.URL.Path != dispatcher.ClaimPath || buf.String() != `{"owner":"w"}` {
				http.Error(w, "bad request", http.StatusTeapot)
				return
			}
			w.WriteHeader(status)
			if status == http.StatusOK {
				_, _ = w.Write([]byte("not json"))
			}
		}))
		job, err := (&dispatcher.Client{URL: srv.URL, HTTP: srv.Client()}).Claim(t.Context(), "w")
		srv.Close()
		if job != nil {
			t.Errorf("status %d: job = %+v", status, job)
		}
		if want == "" && err != nil || want != "" && (err == nil || !strings.Contains(err.Error(), want)) {
			t.Errorf("status %d: err = %v, want %q", status, err, want)
		}
	}
}
