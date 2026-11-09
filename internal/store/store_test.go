package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

const (
	shopA = queuetest.ShopA
	shopB = queuetest.ShopB
)

// withLease sets the lease and heartbeat of every test kind.
func withLease(leaseMS, heartbeatMS int) func(*config.File) {
	return func(c *config.File) {
		for i := range c.Kinds {
			c.Kinds[i].LeaseMS, c.Kinds[i].HeartbeatMS = leaseMS, heartbeatMS
		}
	}
}

func claimOne(t *testing.T, env *queuetest.Env, owner string) store.Claimed {
	t.Helper()
	jobs, err := store.Claim(t.Context(), env.Pool, []string{owner})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(jobs))
	}
	return jobs[0]
}

func countJobs(t *testing.T, env *queuetest.Env) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func backlog(t *testing.T, env *queuetest.Env, shop string) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT backlog FROM shop_state WHERE shop_id = $1`, shop).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSeedIsIdempotentAndKeepsCounters(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)

	cfg := queuetest.Config(t, func(c *config.File) {
		c.Shops[0].Weight = 7
		queuetest.SetKind(c, testkinds.Sleep, func(k *config.Kind) { k.Cost = 3 })
	})
	for range 2 {
		if err := store.Seed(t.Context(), env.Pool, cfg); err != nil {
			t.Fatal(err)
		}
	}
	var weight, cost int
	if err := env.Pool.QueryRow(t.Context(), `SELECT weight FROM shops WHERE id = $1`, shopA).Scan(&weight); err != nil {
		t.Fatal(err)
	}
	if err := env.Pool.QueryRow(t.Context(), `SELECT cost FROM job_kinds WHERE kind = $1`, testkinds.Sleep).Scan(&cost); err != nil {
		t.Fatal(err)
	}
	if weight != 7 || cost != 3 {
		t.Errorf("after reseed: weight %d, cost %d; want 7 and 3", weight, cost)
	}
	if got := backlog(t, env, shopA); got != 2 {
		t.Errorf("reseeding changed the backlog counter to %d, want 2", got)
	}
}

func TestEnqueue(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)

	id := env.Enqueue(t, shopA, testkinds.Sleep, map[string]int{"ms": 5})
	j := env.Job(t, id)
	if j.State != store.StateQueued || j.Shop != shopA || j.Kind != testkinds.Sleep || j.Cost != 1 ||
		j.ClaimSeq != 0 || j.Attempts != 0 || j.FirstClaimedAt != nil || j.LeaseOwner != nil {
		t.Errorf("new job = %+v", j)
	}
	if !j.RunAt.Equal(j.EnqueuedAt) || !j.FirstRunnableAt.Equal(j.EnqueuedAt) {
		t.Errorf("run_at %v, first_runnable_at %v, want both = enqueued_at %v", j.RunAt, j.FirstRunnableAt, j.EnqueuedAt)
	}
	var payload map[string]int
	if err := json.Unmarshal(j.Payload, &payload); err != nil || payload["ms"] != 5 {
		t.Errorf("payload = %s", j.Payload)
	}
	if got := backlog(t, env, shopA); got != 1 {
		t.Errorf("backlog = %d, want 1", got)
	}

	// D-011: first_runnable_at is the later of the enqueue time and run_at.
	future := env.DBNow(t).Add(time.Hour)
	j = env.Job(t, env.EnqueueAt(t, shopA, testkinds.Sleep, nil, &future))
	if !j.RunAt.Equal(future) || !j.FirstRunnableAt.Equal(future) {
		t.Errorf("future job: run_at %v, first_runnable_at %v, want both %v", j.RunAt, j.FirstRunnableAt, future)
	}
	past := env.DBNow(t).Add(-time.Hour)
	j = env.Job(t, env.EnqueueAt(t, shopA, testkinds.Sleep, nil, &past))
	if !j.RunAt.Equal(past) || !j.FirstRunnableAt.Equal(j.EnqueuedAt) {
		t.Errorf("past job: run_at %v, first_runnable_at %v, want %v and %v", j.RunAt, j.FirstRunnableAt, past, j.EnqueuedAt)
	}
}

// AC-1.2 and AC-1.3 at the store: a rejected enqueue creates nothing.
func TestEnqueueRejectsAndCreatesNothing(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	tests := []struct {
		name string
		job  store.NewJob
		want error
	}{
		{"unknown shop", store.NewJob{Shop: "nope.test", Kind: testkinds.Sleep, Payload: json.RawMessage(`{}`)}, store.ErrUnknownShop},
		{"unknown kind", store.NewJob{Shop: shopA, Kind: "test.nope", Payload: json.RawMessage(`{}`)}, store.ErrUnknownKind},
		{"unstorable payload", store.NewJob{Shop: shopA, Kind: testkinds.Sleep, Payload: json.RawMessage(`{"a": "\u0000"}`)}, store.ErrInvalidPayload},
	}
	for _, tt := range tests {
		_, err := store.Enqueue(t.Context(), env.Pool, tt.job)
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: err = %v, want %v", tt.name, err, tt.want)
		}
	}
	if n := countJobs(t, env); n != 0 {
		t.Errorf("%d jobs exist after rejected enqueues, want 0", n)
	}
	if got := backlog(t, env, shopA); got != 0 {
		t.Errorf("backlog = %d after rejected enqueues, want 0", got)
	}
}

// Enqueue wakes the dispatcher with a notification at commit (design §5.1).
func TestEnqueueNotifies(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	conn := env.DB.Connect(t)
	if _, err := conn.Exec(t.Context(), "LISTEN "+store.EnqueueNotifyChannel); err != nil {
		t.Fatal(err)
	}
	env.Enqueue(t, shopB, testkinds.Sleep, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n.Payload != shopB {
		t.Errorf("notification payload %q, want %q", n.Payload, shopB)
	}
}

func TestClaimOldestFirstWithLeaseFromDBClock(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	if jobs, err := store.Claim(t.Context(), env.Pool, []string{"w1"}); err != nil || len(jobs) != 0 {
		t.Fatalf("claim on an empty queue = %v, %v", jobs, err)
	}

	now := env.DBNow(t)
	older, oldest := now.Add(-time.Hour), now.Add(-2*time.Hour)
	newest := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	second := env.EnqueueAt(t, shopB, testkinds.Sleep, nil, &older)
	first := env.EnqueueAt(t, shopA, testkinds.Sleep, nil, &oldest)
	future := now.Add(time.Hour)
	env.EnqueueAt(t, shopA, testkinds.Sleep, nil, &future)

	jobs, err := store.Claim(t.Context(), env.Pool, []string{"w1", "w2", "w3", "w4"})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 3 {
		t.Fatalf("claimed %d jobs, want the 3 runnable ones", len(jobs))
	}
	for i, want := range []int64{first, second, newest} {
		c := jobs[i]
		if c.ID != want || c.Owner != []string{"w1", "w2", "w3"}[i] || c.ClaimSeq != 1 || c.LeaseMS != 1000 {
			t.Errorf("claim %d = %+v, want job %d for owner w%d", i, c, want, i+1)
		}
		if got := c.LeaseExpiresAt.Sub(c.ClaimedAt); got != time.Second {
			t.Errorf("job %d: lease_expires_at - claimed_at = %v, want lease_ms exactly", c.ID, got)
		}
		j := env.Job(t, c.ID)
		if j.State != store.StateRunning || *j.LeaseOwner != c.Owner || !j.FirstClaimedAt.Equal(c.ClaimedAt) {
			t.Errorf("job %d after claim = %+v", c.ID, j)
		}
	}
}

// AC-3.1 at the store: a scheduled job is not claimable before run_at by the
// database clock, and is claimable once it passes.
func TestClaimWaitsForRunAt(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	runAt := env.DBNow(t).Add(300 * time.Millisecond)
	id := env.EnqueueAt(t, shopA, testkinds.Sleep, nil, &runAt)
	var c store.Claimed
	queuetest.Eventually(t, 5*time.Second, "the scheduled job to be claimed", func() bool {
		jobs, err := store.Claim(t.Context(), env.Pool, []string{"w"})
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 1 {
			c = jobs[0]
		}
		return len(jobs) == 1
	})
	if c.ID != id || c.ClaimedAt.Before(runAt) {
		t.Fatalf("claimed job %d at %v, want job %d no earlier than run_at %v", c.ID, c.ClaimedAt, id, runAt)
	}
}

func TestCompleteIsFenced(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	c := claimOne(t, env, "w1")
	f := c.Fence()

	for name, stale := range map[string]store.Fence{
		"older claim_seq": {JobID: id, ClaimSeq: f.ClaimSeq - 1, Owner: f.Owner},
		"newer claim_seq": {JobID: id, ClaimSeq: f.ClaimSeq + 1, Owner: f.Owner},
		"other owner":     {JobID: id, ClaimSeq: f.ClaimSeq, Owner: "w2"},
		"other job":       {JobID: id + 1, ClaimSeq: f.ClaimSeq, Owner: f.Owner},
	} {
		if ok, err := store.Complete(t.Context(), env.Pool, stale); err != nil || ok {
			t.Errorf("%s: Complete = %v, %v; want false", name, ok, err)
		}
	}
	if ok, err := store.Complete(t.Context(), env.Pool, f); err != nil || !ok {
		t.Fatalf("Complete with the live fence = %v, %v; want true", ok, err)
	}
	if ok, err := store.Complete(t.Context(), env.Pool, f); err != nil || ok {
		t.Errorf("second Complete = %v, %v; want false", ok, err)
	}
	j := env.Job(t, id)
	if j.State != store.StateCompleted || j.CompletedAt == nil || j.LeaseOwner != nil || j.LeaseExpiresAt != nil || j.ClaimSeq != 1 {
		t.Errorf("completed job = %+v", j)
	}
	if got := backlog(t, env, shopA); got != 0 {
		t.Errorf("backlog = %d after completion, want 0", got)
	}
}

// AC-5.1 and AC-5.2 at the store: a failure counts an attempt and requeues at
// the database clock plus the drawn backoff; at the max it goes dead with a
// reason and the last error.
func TestFailRetriesThenGoesDead(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, func(c *config.File) {
		queuetest.SetKind(c, testkinds.Fail, func(k *config.Kind) { k.MaxAttempts = 3 })
	}))
	id := env.Enqueue(t, shopA, testkinds.Fail, nil)

	c := claimOne(t, env, "w1")
	before := env.DBNow(t)
	res, err := store.Fail(t.Context(), env.Pool, c.Fence(), "boom 1", time.Second)
	after := env.DBNow(t)
	if err != nil || !res.Applied || res.State != store.StateQueued {
		t.Fatalf("first Fail = %+v, %v", res, err)
	}
	j := env.Job(t, id)
	if j.Attempts != 1 || j.State != store.StateQueued || *j.LastError != "boom 1" || j.LeaseOwner != nil {
		t.Errorf("after one failure: %+v", j)
	}
	if lo, hi := before.Add(time.Second), after.Add(time.Second); j.RunAt.Before(lo) || j.RunAt.After(hi) {
		t.Errorf("run_at %v, want the write's database time + 1s, in [%v, %v]", j.RunAt, lo, hi)
	}
	if jobs, err := store.Claim(t.Context(), env.Pool, []string{"w2"}); err != nil || len(jobs) != 0 {
		t.Errorf("claimed %d jobs during the backoff, want 0 (err %v)", len(jobs), err)
	}
	if ok, err := store.Complete(t.Context(), env.Pool, c.Fence()); err != nil || ok {
		t.Errorf("Complete with the failed run's fence = %v, %v; want false", ok, err)
	}

	time.Sleep(1100 * time.Millisecond)
	for attempt := int32(2); attempt <= 3; attempt++ {
		c = claimOne(t, env, "w1")
		res, err = store.Fail(t.Context(), env.Pool, c.Fence(), fmt.Sprintf("boom %d", attempt), 0)
		if err != nil || !res.Applied {
			t.Fatalf("Fail %d = %+v, %v", attempt, res, err)
		}
	}
	if res.State != store.StateDead {
		t.Fatalf("third failure left state %s, want dead", res.State)
	}
	j = env.Job(t, id)
	if j.State != store.StateDead || j.Attempts != 3 || *j.DeadReason != store.ReasonAttemptsExhausted ||
		*j.LastError != "boom 3" || j.DeadAt == nil || j.LeaseOwner != nil {
		t.Errorf("dead job = %+v", j)
	}
	if got := backlog(t, env, shopA); got != 0 {
		t.Errorf("backlog = %d after the job died, want 0", got)
	}
}

// AC-5.3 at the store.
func TestPermanentGoesStraightToDead(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	id := env.Enqueue(t, shopA, testkinds.Fail, nil)
	c := claimOne(t, env, "w1")
	if ok, err := store.Permanent(t.Context(), env.Pool, store.Fence{JobID: id, ClaimSeq: 9, Owner: "w1"}, "x"); err != nil || ok {
		t.Errorf("Permanent with a stale fence = %v, %v; want false", ok, err)
	}
	if ok, err := store.Permanent(t.Context(), env.Pool, c.Fence(), "bad input"); err != nil || !ok {
		t.Fatalf("Permanent = %v, %v", ok, err)
	}
	j := env.Job(t, id)
	if j.State != store.StateDead || *j.DeadReason != store.ReasonPermanent || *j.LastError != "bad input" || j.Attempts != 0 {
		t.Errorf("job = %+v", j)
	}
	if got := backlog(t, env, shopA); got != 0 {
		t.Errorf("backlog = %d, want 0", got)
	}
}

// AC-4.1 and Q2 at the store: a heartbeat extends live leases by the database
// clock and never revives an expired one.
func TestHeartbeat(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withLease(1000, 200)))
	a := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	b := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	ca, cb := claimOne(t, env, "w1"), claimOne(t, env, "w1")

	time.Sleep(100 * time.Millisecond)
	alive, err := store.Heartbeat(t.Context(), env.Pool, []store.Fence{ca.Fence(), cb.Fence(), {JobID: b, ClaimSeq: 7, Owner: "w1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(alive) != 2 {
		t.Fatalf("alive = %v, want both %d and %d (and not the stale fence)", alive, a, b)
	}
	if exp := env.Job(t, a).LeaseExpiresAt; !exp.After(ca.LeaseExpiresAt) {
		t.Errorf("lease_expires_at %v not extended past %v", exp, ca.LeaseExpiresAt)
	}

	time.Sleep(1100 * time.Millisecond) // both leases expire
	expired := *env.Job(t, a).LeaseExpiresAt
	alive, err = store.Heartbeat(t.Context(), env.Pool, []store.Fence{ca.Fence(), cb.Fence()})
	if err != nil || len(alive) != 0 {
		t.Fatalf("heartbeat after expiry = %v, %v; want no live leases", alive, err)
	}
	if got := *env.Job(t, a).LeaseExpiresAt; !got.Equal(expired) {
		t.Errorf("an expired lease was moved from %v to %v", expired, got)
	}
}

// AC-4.4 at the store: an expired lease is requeued at once with one more
// attempt, or goes dead at the max; live leases are untouched.
func TestReap(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, func(c *config.File) {
		withLease(500, 100)(c)
		queuetest.SetKind(c, testkinds.Fail, func(k *config.Kind) { k.MaxAttempts = 1 })
	}))
	retried := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	killed := env.Enqueue(t, shopA, testkinds.Fail, nil)
	claimOne(t, env, "w1")
	claimOne(t, env, "w1")

	if r, err := store.Reap(t.Context(), env.Pool, 500); err != nil || r != (store.ReapResult{}) {
		t.Fatalf("reap of live leases = %+v, %v; want nothing", r, err)
	}
	time.Sleep(600 * time.Millisecond)
	live := env.Enqueue(t, shopB, testkinds.Sleep, nil)
	claimOne(t, env, "w2")

	r, err := store.Reap(t.Context(), env.Pool, 500)
	if err != nil || r.Requeued != 1 || r.Dead != 1 {
		t.Fatalf("reap = %+v, %v; want 1 requeued and 1 dead", r, err)
	}
	reapedAt := env.DBNow(t)
	j := env.Job(t, retried)
	if j.State != store.StateQueued || j.Attempts != 1 || *j.LastError != "lease expired" || j.LeaseOwner != nil || j.RunAt.After(reapedAt) {
		t.Errorf("requeued job = %+v", j)
	}
	j = env.Job(t, killed)
	if j.State != store.StateDead || *j.DeadReason != store.ReasonAttemptsExhausted || j.Attempts != 1 {
		t.Errorf("job at its max attempts = %+v", j)
	}
	if j := env.Job(t, live); j.State != store.StateRunning {
		t.Errorf("a live lease was reaped: %+v", j)
	}
	c := claimOne(t, env, "w3") // runnable at once (D-031)
	if c.ID != retried || c.ClaimSeq != 2 {
		t.Errorf("reclaim = %+v, want job %d at claim_seq 2", c, retried)
	}
	if got := backlog(t, env, shopA); got != 1 {
		t.Errorf("backlog = %d, want 1 (the dead job left it)", got)
	}
}

func TestReapHonoursBatchLimit(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withLease(100, 30)))
	for range 3 {
		env.Enqueue(t, shopA, testkinds.Sleep, nil)
	}
	if jobs, err := store.Claim(t.Context(), env.Pool, []string{"a", "b", "c"}); err != nil || len(jobs) != 3 {
		t.Fatalf("claim = %d, %v", len(jobs), err)
	}
	time.Sleep(150 * time.Millisecond)
	for _, want := range []int{2, 1, 0} {
		r, err := store.Reap(t.Context(), env.Pool, 2)
		if err != nil || r.Requeued != want {
			t.Fatalf("reap = %+v, %v; want %d requeued", r, err, want)
		}
	}
}

// D-036: the fenced completion runs in the handler's run.Tx() transaction,
// which began before the lease expired. Postgres's now() is frozen at that
// transaction's start, so the fence must read statement_timestamp().
func TestFencedCompletionInALongTransactionSeesExpiry(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withLease(200, 50)))
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	c := claimOne(t, env, "w1")

	tx, err := env.Pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(t.Context(), `CREATE TEMP TABLE effect (n int); INSERT INTO effect VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // the lease expires while the transaction is open

	var liveByNow bool
	if err := tx.QueryRow(t.Context(), `SELECT lease_expires_at > now() FROM jobs WHERE id = $1`, id).Scan(&liveByNow); err != nil {
		t.Fatal(err)
	}
	if !liveByNow {
		t.Fatal("expected now() to still be before the lease expiry inside the old transaction; the test no longer shows the D-036 hazard")
	}
	ok, err := store.Complete(t.Context(), tx, c.Fence())
	if err != nil || ok {
		t.Fatalf("Complete in a transaction older than the expired lease = %v, %v; want false", ok, err)
	}
}

func TestCleanError(t *testing.T) {
	if got := store.CleanError("short"); got != "short" {
		t.Errorf("CleanError(short) = %q", got)
	}
	if got := store.CleanError("a\x00b\xffc"); got != "a�b�c" {
		t.Errorf("CleanError kept a NUL or invalid UTF-8: %q", got)
	}
	long := strings.Repeat("é", store.LastErrorMax) // 2 bytes each
	got := store.CleanError(long)
	if len(got) > store.LastErrorMax || !utf8.ValidString(got) || len(got) < store.LastErrorMax-1 {
		t.Errorf("CleanError(long) has %d bytes, valid UTF-8 %v; want at most %d, cut on a rune", len(got), utf8.ValidString(got), store.LastErrorMax)
	}
}
