package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

func throttledUntil(t *testing.T, env *queuetest.Env, shop string) *time.Time {
	t.Helper()
	var until *time.Time
	if err := env.Pool.QueryRow(t.Context(), `SELECT throttled_until FROM shop_state WHERE shop_id = $1`, shop).Scan(&until); err != nil {
		t.Fatal(err)
	}
	return until
}

// AC-6.1, Q7 at the store: a throttle requeues the job no earlier than
// statement_timestamp() + wait + jitter, adds one to throttles and leaves
// attempts alone; the shop is paused until statement_timestamp() + wait; the job
// stays in the backlog.
func TestThrottleRequeuesWithoutAnAttempt(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	c := claimOne(t, env, "w")
	before := env.DBNow(t)
	res, err := store.Throttle(t.Context(), env.Pool, c.Fence(), 2*time.Second, 150*time.Millisecond, "throttled: retry after 2s")
	if err != nil {
		t.Fatal(err)
	}
	after := env.DBNow(t)
	if !res.Applied || res.State != store.StateQueued {
		t.Fatalf("result = %+v, want applied and queued", res)
	}
	j := env.Job(t, id)
	if j.State != store.StateQueued || j.Attempts != 0 || j.Throttles != 1 || j.LeaseOwner != nil || j.LeaseExpiresAt != nil {
		t.Errorf("job = state %s, attempts %d, throttles %d, owner %v", j.State, j.Attempts, j.Throttles, j.LeaseOwner)
	}
	if j.RunAt.Before(before.Add(2150*time.Millisecond)) || j.RunAt.After(after.Add(2150*time.Millisecond)) {
		t.Errorf("run_at = %v, want statement time + 2.15s (between %v and %v)", j.RunAt, before, after)
	}
	if j.LastError == nil || *j.LastError != "throttled: retry after 2s" || j.DeadReason != nil {
		t.Errorf("last_error %v, dead_reason %v", j.LastError, j.DeadReason)
	}
	until := throttledUntil(t, env, shopA)
	if until == nil || !until.Equal(res.ThrottledUntil) || until.Before(before.Add(2*time.Second)) || until.After(after.Add(2*time.Second)) {
		t.Errorf("throttled_until = %v (result %v), want statement time + 2s", until, res.ThrottledUntil)
	}
	if until := throttledUntil(t, env, shopB); until != nil {
		t.Errorf("another shop was paused until %v", until)
	}
	if backlog(t, env, shopA) != 1 {
		t.Errorf("backlog = %d, want 1: a throttled job is still unfinished", backlog(t, env, shopA))
	}
	env.CheckBacklog(t)
}

// throttled_until only moves later: a shorter throttle after a longer one keeps
// the longer pause.
func TestThrottleNeverShortensThePause(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	jobs, err := store.Claim(t.Context(), env.Pool, []string{"w1", "w2"})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("claimed %d (%v)", len(jobs), err)
	}
	long, err := store.Throttle(t.Context(), env.Pool, jobs[0].Fence(), 10*time.Second, 0, "long")
	if err != nil {
		t.Fatal(err)
	}
	short, err := store.Throttle(t.Context(), env.Pool, jobs[1].Fence(), time.Second, 0, "short")
	if err != nil {
		t.Fatal(err)
	}
	if !short.ThrottledUntil.Equal(long.ThrottledUntil) {
		t.Errorf("after a 1s throttle the pause is %v, want it kept at %v", short.ThrottledUntil, long.ThrottledUntil)
	}
}

// P5-2, AC-6.3: a job throttled more often than its kind's max_throttles goes
// dead with "throttle budget exhausted", leaves the backlog, and has still used
// no attempt. The 429 still pauses the shop.
func TestThrottleBudgetExhausted(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, queuetest.WithKind(testkinds.Sleep, func(k *config.Kind) { k.MaxThrottles = 2 })))
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	for i := range 3 {
		if _, err := env.Pool.Exec(t.Context(), `UPDATE jobs SET run_at = statement_timestamp() WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		if _, err := env.Pool.Exec(t.Context(), `UPDATE shop_state SET throttled_until = NULL`); err != nil {
			t.Fatal(err)
		}
		c := claimOne(t, env, "w")
		res, err := store.Throttle(t.Context(), env.Pool, c.Fence(), 100*time.Millisecond, 0, "throttled")
		if err != nil {
			t.Fatal(err)
		}
		want := store.StateQueued
		if i == 2 {
			want = store.StateDead
		}
		if !res.Applied || res.State != want {
			t.Fatalf("throttle %d: %+v, want %s", i+1, res, want)
		}
	}
	j := env.Job(t, id)
	if j.State != store.StateDead || j.DeadReason == nil || *j.DeadReason != store.ReasonThrottleBudget ||
		j.Throttles != 3 || j.Attempts != 0 || j.DeadAt == nil {
		t.Errorf("job = state %s, reason %v, throttles %d, attempts %d", j.State, j.DeadReason, j.Throttles, j.Attempts)
	}
	if throttledUntil(t, env, shopA) == nil {
		t.Error("the last throttle did not pause the shop")
	}
	if backlog(t, env, shopA) != 0 {
		t.Errorf("backlog = %d, want 0", backlog(t, env, shopA))
	}
	env.CheckBacklog(t)
}

// Q2 for throttles: a throttle from a run whose lease is gone changes nothing,
// not even the shop's pause.
func TestThrottleIsFenced(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	c := claimOne(t, env, "w")
	stale := c.Fence()
	stale.ClaimSeq--
	for name, f := range map[string]store.Fence{
		"old claim_seq": stale,
		"other owner":   {JobID: c.ID, ClaimSeq: c.ClaimSeq, Owner: "someone-else"},
	} {
		res, err := store.Throttle(t.Context(), env.Pool, f, time.Second, 0, "x")
		if err != nil || res.Applied {
			t.Errorf("%s: %+v (%v), want not applied", name, res, err)
		}
	}
	// An expired lease that the reaper hasn't requeued yet also loses (Q2).
	if _, err := env.Pool.Exec(t.Context(), `UPDATE jobs SET lease_expires_at = statement_timestamp() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if res, err := store.Throttle(t.Context(), env.Pool, c.Fence(), time.Second, 0, "x"); err != nil || res.Applied {
		t.Errorf("expired lease: %+v (%v), want not applied", res, err)
	}
	j := env.Job(t, id)
	if j.State != store.StateRunning || j.Throttles != 0 {
		t.Errorf("job = state %s, throttles %d; want untouched", j.State, j.Throttles)
	}
	if until := throttledUntil(t, env, shopA); until != nil {
		t.Errorf("a fenced-out throttle paused the shop until %v", until)
	}
}

// A throttle notifies shop_throttled with the shop, so the dispatcher
// refreshes its view at once (design §6.1).
func TestThrottleNotifies(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	conn := env.DB.Connect(t)
	if _, err := conn.Exec(t.Context(), "LISTEN "+store.ThrottleNotifyChannel); err != nil {
		t.Fatal(err)
	}
	env.Enqueue(t, shopB, testkinds.Sleep, nil)
	c := claimOne(t, env, "w")
	if _, err := store.Throttle(t.Context(), env.Pool, c.Fence(), time.Second, 0, "x"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n.Payload != shopB {
		t.Errorf("payload %q, want %q", n.Payload, shopB)
	}
}

// P5-2, AC-6.2 at the claim SQL: once a throttle commits, every claim that
// starts later skips the shop until throttled_until has passed by the database
// clock, whatever the claim's spec, while other shops are claimed as usual. Then
// the shop's jobs are claimable again.
func TestNoClaimForAThrottledShopBeforeRetryAfter(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	for range 3 {
		env.Enqueue(t, shopA, testkinds.Sleep, nil)
	}
	c := claimOne(t, env, "w0")
	res, err := store.Throttle(t.Context(), env.Pool, c.Fence(), 600*time.Millisecond, 0, "throttled")
	if err != nil || !res.Applied {
		t.Fatalf("throttle: %+v (%v)", res, err)
	}
	// Another shop is claimed as usual during the pause.
	env.Enqueue(t, shopB, testkinds.Sleep, nil)
	out, err := store.ClaimFor(t.Context(), env.Pool, []string{"wb"}, store.ClaimSpec{Shops: []string{shopB}})
	if err != nil || len(out.Jobs) != 1 || !out.Jobs[0].ClaimedAt.Before(res.ThrottledUntil) {
		t.Fatalf("claim for the other shop during the pause: %+v (%v)", out, err)
	}

	// Every kind of claim skips the throttled shop until throttled_until.
	specs := []store.ClaimSpec{{}, {Shops: []string{shopA}}, {Shops: []string{shopA}, MaxCost: 10}}
	for {
		for _, spec := range specs {
			out, err := store.ClaimFor(t.Context(), env.Pool, []string{"w1"}, spec)
			if err != nil {
				t.Fatal(err)
			}
			for _, j := range out.Jobs {
				if j.ClaimedAt.Before(res.ThrottledUntil) {
					t.Fatalf("claimed job %d of the throttled shop at %v, before throttled_until %v", j.ID, j.ClaimedAt, res.ThrottledUntil)
				}
				return // claimed after the pause: done
			}
			if out.End != store.ClaimBlocked {
				t.Fatalf("a claim during the pause ended %v, want ClaimBlocked", out.End)
			}
		}
		if env.DBNow(t).After(res.ThrottledUntil.Add(5 * time.Second)) {
			t.Fatal("the throttled shop's jobs were never claimed after the pause")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
