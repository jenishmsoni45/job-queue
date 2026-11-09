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

func countKeys(t *testing.T, env *queuetest.Env) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM dedupe_keys`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func enqueueKey(t *testing.T, env *queuetest.Env, shop, key string) store.Enqueued {
	t.Helper()
	res, err := store.Enqueue(t.Context(), env.Pool, store.NewJob{Shop: shop, Kind: testkinds.Sleep, Payload: json.RawMessage(`{}`), DedupeKey: key})
	if err != nil {
		t.Fatalf("enqueue %s/%s: %v", shop, key, err)
	}
	return res
}

// AC-1.4, D-018: a key the shop already has returns its job as a duplicate and
// creates nothing, before and after the job completes. Keys are per shop.
func TestEnqueueDedupe(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	first := enqueueKey(t, env, shopA, "webhook-1")
	if first.Duplicate || first.ID == 0 {
		t.Fatalf("first enqueue = %+v, want a new job", first)
	}
	if again := enqueueKey(t, env, shopA, "webhook-1"); !again.Duplicate || again.ID != first.ID {
		t.Fatalf("repeat = %+v, want duplicate of %d", again, first.ID)
	}
	if n, b := countJobs(t, env), backlog(t, env, shopA); n != 1 || b != 1 {
		t.Errorf("%d jobs, backlog %d after a duplicate; want 1 and 1", n, b)
	}

	other := enqueueKey(t, env, shopB, "webhook-1")
	if other.Duplicate || other.ID == first.ID {
		t.Errorf("same key for another shop = %+v, want a new job", other)
	}

	// Complete the first job: the key still dedupes (Q4 across completion).
	c := claimOne(t, env, "w")
	if ok, err := store.Complete(t.Context(), env.Pool, c.Fence()); err != nil || !ok {
		t.Fatalf("complete = %v, %v", ok, err)
	}
	if again := enqueueKey(t, env, shopA, "webhook-1"); !again.Duplicate || again.ID != first.ID {
		t.Errorf("after completion = %+v, want duplicate of %d", again, first.ID)
	}
	if n := countKeys(t, env); n != 2 {
		t.Errorf("%d dedupe keys, want 2", n)
	}
	env.CheckBacklog(t)
}

// Design §5.1's conflict branch: two enqueues of one new key both pass the
// dedupe check before either commits. The shop_state row lock serializes them;
// the second one's key insert conflicts, its job rolls back, and it returns the
// first one's job. The test holds the row lock until both have passed the
// check, so the branch runs every time.
func TestEnqueueDedupeConcurrentConflict(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	holder, err := env.Pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(t.Context(), `SELECT 1 FROM shop_state WHERE shop_id = $1 FOR UPDATE`, shopA); err != nil {
		t.Fatal(err)
	}

	const k = 4
	results := make(chan store.Enqueued, k)
	errs := make(chan error, k)
	for range k {
		go func() {
			res, err := store.Enqueue(context.Background(), env.Pool,
				store.NewJob{Shop: shopA, Kind: testkinds.Sleep, Payload: json.RawMessage(`{}`), DedupeKey: "race"})
			results <- res
			errs <- err
		}()
	}
	// Every enqueue has passed the dedupe check once it waits on the row lock.
	queuetest.Eventually(t, 10*time.Second, "every enqueue to wait on the shop_state lock", func() bool {
		var waiting int
		if err := env.Pool.QueryRow(t.Context(), `
			SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%shop_state%FOR UPDATE%'`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting == k
	})
	if err := holder.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	ids := map[int64]bool{}
	created := 0
	for range k {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		res := <-results
		ids[res.ID] = true
		if !res.Duplicate {
			created++
		}
	}
	if len(ids) != 1 || created != 1 {
		t.Errorf("%d enqueues gave IDs %v with %d created, want one job created once", k, ids, created)
	}
	if n, keys := countJobs(t, env), countKeys(t, env); n != 1 || keys != 1 {
		t.Errorf("%d jobs and %d keys, want 1 and 1: a losing enqueue's job must roll back", n, keys)
	}
	env.CheckBacklog(t)
}

// A rejected enqueue with a dedupe key stores no key, so a later valid
// enqueue with the same key creates its job.
func TestRejectedEnqueueStoresNoKey(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	_, err := store.Enqueue(t.Context(), env.Pool,
		store.NewJob{Shop: shopA, Kind: testkinds.Sleep, Payload: json.RawMessage(`{"a": "\u0000"}`), DedupeKey: "k"})
	if !errors.Is(err, store.ErrInvalidPayload) {
		t.Fatalf("err = %v, want ErrInvalidPayload", err)
	}
	if n := countKeys(t, env); n != 0 {
		t.Fatalf("%d keys after a rejected enqueue", n)
	}
	if res := enqueueKey(t, env, shopA, "k"); res.Duplicate {
		t.Errorf("the key of a rejected enqueue dedupes a later one: %+v", res)
	}
}

// P3-7, D-041: the pruner deletes completed jobs, discarded jobs and dedupe
// keys past their windows, by the database clock, and nothing else. A pruned
// job's idempotency keys go with it.
func TestPrune(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	ctx := t.Context()
	insert := func(state, ages string) int64 {
		t.Helper()
		var id int64
		// ages sets the timestamps a row's state needs, as intervals before now.
		if err := env.Pool.QueryRow(ctx, `
			INSERT INTO jobs (shop_id, kind, cost, payload, state, run_at, first_runnable_at, enqueued_at,
			                  completed_at, dead_at, dead_reason, discarded_at, lease_owner, lease_expires_at)
			SELECT $1, $2, 1, '{}', $3, now() - interval '30 days', now() - interval '30 days', now() - interval '30 days',
			       CASE WHEN $3 = 'completed' THEN now() - a.age END,
			       CASE WHEN $3 = 'dead' THEN now() - interval '30 days' END,
			       CASE WHEN $3 = 'dead' THEN 'permanent' END,
			       CASE WHEN $4 THEN now() - a.age END,
			       CASE WHEN $3 = 'running' THEN 'w' END,
			       CASE WHEN $3 = 'running' THEN now() + interval '1 hour' END
			  FROM (SELECT $5::interval AS age) a
			RETURNING id`, shopA, testkinds.Sleep, state, strings.HasPrefix(ages, "discarded"),
			strings.TrimPrefix(ages, "discarded "),
		).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	oldDone := insert("completed", "2 hours")
	youngDone := insert("completed", "30 minutes")
	oldDiscarded := insert("dead", "discarded 2 hours")
	youngDiscarded := insert("dead", "discarded 30 minutes")
	oldDead := insert("dead", "2 hours") // dead for 30 days, never discarded
	oldQueued := insert("queued", "2 hours")
	oldRunning := insert("running", "2 hours")
	if _, err := env.Pool.Exec(ctx, `INSERT INTO idempotency_keys (job_id, step) VALUES ($1, 's'), ($2, 's')`, oldDone, youngDone); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Pool.Exec(ctx, `
		INSERT INTO dedupe_keys (shop_id, key, job_id, created_at) VALUES
		  ($1, 'old', $2, now() - interval '3 hours'), ($1, 'young', $3, now() - interval '1 hour')`,
		shopA, oldDone, youngDone); err != nil {
		t.Fatal(err)
	}

	r := store.Retention{Completed: time.Hour, Discarded: time.Hour, DedupeKeys: 2 * time.Hour}
	got, err := store.Prune(ctx, env.Pool, r, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if got != (store.PruneResult{Completed: 1, Discarded: 1, DedupeKeys: 1}) {
		t.Errorf("Prune = %+v, want one of each", got)
	}
	for _, id := range []int64{oldDone, oldDiscarded} {
		if _, err := store.GetJob(ctx, env.Pool, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("job %d past its window still exists (%v)", id, err)
		}
	}
	for _, id := range []int64{youngDone, youngDiscarded, oldDead, oldQueued, oldRunning} {
		if _, err := store.GetJob(ctx, env.Pool, id); err != nil {
			t.Errorf("job %d was pruned: %v", id, err)
		}
	}
	var steps []int64
	rows, err := env.Pool.Query(ctx, `SELECT job_id FROM idempotency_keys ORDER BY job_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		steps = append(steps, id)
	}
	if len(steps) != 1 || steps[0] != youngDone {
		t.Errorf("idempotency keys left for jobs %v, want only %d's", steps, youngDone)
	}
	var keys []string
	rows, err = env.Pool.Query(ctx, `SELECT key FROM dedupe_keys`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	if len(keys) != 1 || keys[0] != "young" {
		t.Errorf("dedupe keys left %v, want [young]", keys)
	}
}

// The pruner repeats each delete while a batch comes back full, so a backlog
// larger than one batch is cleared in one pass.
func TestPruneBatches(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	if _, err := env.Pool.Exec(t.Context(), `
		INSERT INTO jobs (shop_id, kind, cost, payload, state, run_at, first_runnable_at, completed_at)
		SELECT $1, $2, 1, '{}', 'completed', now(), now(), now() - interval '2 hours' FROM generate_series(1, 7)`,
		shopA, testkinds.Sleep); err != nil {
		t.Fatal(err)
	}
	r := store.Retention{Completed: time.Hour, Discarded: time.Hour, DedupeKeys: time.Hour}
	got, err := store.Prune(t.Context(), env.Pool, r, 3)
	if err != nil || got.Completed != 7 {
		t.Errorf("Prune with batch 3 = %+v, %v; want all 7 completed jobs", got, err)
	}
	if _, err := store.Prune(t.Context(), env.Pool, store.Retention{}, 3); err == nil {
		t.Error("Prune accepted zero retention windows")
	}
	if _, err := store.Prune(t.Context(), env.Pool, r, 0); err == nil {
		t.Error("Prune accepted a zero batch")
	}
}

// openStep begins a transaction that records a step of job id, as an open
// run.Step does, and keeps it open until the test ends. The idempotency_keys
// foreign key locks the job's row FOR KEY SHARE meanwhile.
func openStep(t *testing.T, env *queuetest.Env, id int64) {
	t.Helper()
	tx, err := env.Pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(t.Context(), `INSERT INTO idempotency_keys (job_id, step) VALUES ($1, 'open')`, id); err != nil {
		t.Fatal(err)
	}
}

// D-047: an open step of a job doesn't hide it from the reaper. Its expired
// lease is requeued while the step's transaction still holds the key.
func TestReapIgnoresAnOpenStep(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withLease(100, 50)))
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	claimOne(t, env, "stuck-in-a-step")
	openStep(t, env, id)
	time.Sleep(200 * time.Millisecond) // the lease expires
	r, err := store.Reap(t.Context(), env.Pool, 500)
	if err != nil || r.Requeued != 1 {
		t.Fatalf("Reap = %+v, %v; want the job requeued although a step of it is open", r, err)
	}
}

// D-047: an open step of a queued job doesn't hide it from claims.
func TestClaimIgnoresAnOpenStep(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	openStep(t, env, id)
	jobs, err := store.Claim(t.Context(), env.Pool, []string{"w"})
	if err != nil || len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("Claim = %+v, %v; want job %d although a step of it is open", jobs, err, id)
	}
}

// Design §5.1, D-018: dedupe runs before the shop_state row lock, so a
// duplicate is answered while another transaction holds that lock (as a busy
// shop's enqueues do), and it never takes the lock itself.
func TestDuplicateDoesNotWaitOnTheShopLock(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	first := enqueueKey(t, env, shopA, "webhook-9")
	holder, err := env.Pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := holder.Exec(t.Context(), `SELECT 1 FROM shop_state WHERE shop_id = $1 FOR UPDATE`, shopA); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	res, err := store.Enqueue(ctx, env.Pool,
		store.NewJob{Shop: shopA, Kind: testkinds.Sleep, Payload: json.RawMessage(`{}`), DedupeKey: "webhook-9"})
	if err != nil || !res.Duplicate || res.ID != first.ID {
		t.Fatalf("duplicate while the shop row is locked = %+v, %v; want duplicate of %d without waiting", res, err, first.ID)
	}
}
