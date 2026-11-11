package dispatcher_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/dispatcher"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/scheduler"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

const shopB = queuetest.ShopB

func withWeight(shop string, weight int) func(*config.File) {
	return queuetest.WithShop(shop, func(s *config.Shop) { s.Weight = weight })
}

func withCap(shop string, maxConcurrency int) func(*config.File) {
	return queuetest.WithShop(shop, func(s *config.Shop) { s.MaxConcurrency = maxConcurrency })
}

func withCost(kind string, cost int) func(*config.File) {
	return queuetest.WithKind(kind, func(k *config.Kind) { k.Cost = cost })
}

// longLeases keeps held jobs from expiring, and the reaper from requeuing them,
// while a test waits.
func longLeases(c *config.File) {
	for i := range c.Kinds {
		c.Kinds[i].LeaseMS, c.Kinds[i].HeartbeatMS = 60_000, 10_000
	}
}

func fifo(c *dispatcher.Config) { c.Scheduler = scheduler.ModeFIFO }

// claimSequentially claims n jobs one at a time, as one worker slot would, and
// returns them in order. With one worker waiting at a time, each claim is for one job.
func claimSequentially(t *testing.T, r *running, n int) []store.Claimed {
	t.Helper()
	waitLeader(t, r)
	jobs := make([]store.Claimed, 0, n)
	for i := range n {
		j, err := r.client.Claim(t.Context(), fmt.Sprintf("slot-%d", i))
		if err != nil || j == nil {
			t.Fatalf("claim %d: %v, %v", i, j, err)
		}
		jobs = append(jobs, *j)
	}
	return jobs
}

func tally(jobs []store.Claimed) map[string]int {
	got := map[string]int{}
	for _, j := range jobs {
		got[j.Shop]++
	}
	return got
}

func enqueueMany(t *testing.T, env *queuetest.Env, shop, kind string, n int) []int64 {
	t.Helper()
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = env.Enqueue(t, shop, kind, nil)
	}
	return ids
}

// P4-5, AC-9.1: DRR picks shops by weight. With weights 1 and 3, a saturated
// dispatcher gives shop B three claims for each of shop A's.
func TestDRRServesShopsInProportionToTheirWeights(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, queuetest.Edits(withWeight(shopA, 1), withWeight(shopB, 3))))
	enqueueMany(t, env, shopA, testkinds.Sleep, 40)
	enqueueMany(t, env, shopB, testkinds.Sleep, 40)
	r := start(t, env, "drr", nil)

	got := tally(claimSequentially(t, r, 16))
	if got[shopA] != 4 || got[shopB] != 12 {
		t.Fatalf("16 claims went %v, want shop A 4 and shop B 12 (weights 1:3)", got)
	}
}

// P4-5, AC-9.1: DRR charges each job its kind's cost. Shop A's jobs cost 3 and
// shop B's cost 1, with equal weights, so over a full cycle each shop gets the
// same cost units: one A job for three B jobs.
func TestDRRChargesEachJobItsCost(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withCost(testkinds.Fail, 3)))
	if env.Config.MaxCost() != 3 {
		t.Fatalf("test setup: max cost %d, want 3", env.Config.MaxCost())
	}
	enqueueMany(t, env, shopA, testkinds.Fail, 20)  // cost 3
	enqueueMany(t, env, shopB, testkinds.Sleep, 40) // cost 1
	r := start(t, env, "cost", nil)

	got := tally(claimSequentially(t, r, 16))
	if got[shopA] != 4 || got[shopB] != 12 {
		t.Fatalf("16 claims went %v; want shop A 4 jobs (12 cost units) and shop B 12 jobs (12 cost units)", got)
	}
}

// P4-5, AC-9.3: with the FIFO scheduler the dispatcher claims the globally
// oldest runnable jobs, whatever the weights; and nothing else changes: a shop's
// cap still holds.
func TestFIFOClaimsTheGloballyOldestJobsAndKeepsTheCaps(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, queuetest.Edits(withWeight(shopB, 50), withCap(shopA, 2))))
	a := enqueueMany(t, env, shopA, testkinds.Sleep, 6) // the oldest jobs, from the shop with the small weight
	enqueueMany(t, env, shopB, testkinds.Sleep, 6)
	r := start(t, env, "fifo", fifo)

	jobs := claimSequentially(t, r, 2)
	for i, j := range jobs {
		if j.ID != a[i] {
			t.Fatalf("claim %d is job %d, want the oldest runnable job %d", i, j.ID, a[i])
		}
	}
	// Shop A is at its cap of 2, so the next jobs, though B's are younger, are B's.
	rest := claimSequentially(t, r, 3)
	if got := tally(rest); got[shopB] != 3 {
		t.Fatalf("with shop A at its cap, the next claims went %v, want 3 for shop B", got)
	}
}

// The same data under DRR interleaves the shops, which FIFO does not (above).
func TestDRRInterleavesWhereFIFOWouldNot(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	enqueueMany(t, env, shopA, testkinds.Sleep, 10)
	enqueueMany(t, env, shopB, testkinds.Sleep, 10)
	r := start(t, env, "drr", nil)
	if got := tally(claimSequentially(t, r, 6)); got[shopA] != 3 || got[shopB] != 3 {
		t.Fatalf("6 claims went %v, want 3 each: with shop A's 10 jobs all older, FIFO would give A all 6", got)
	}
}

// P4-1, AC-10.1: through the dispatcher, a shop never runs more than its cap,
// and a finished job frees a slot for the next one.
func TestDispatcherNeverStartsMoreThanTheCap(t *testing.T) {
	t.Parallel()
	for name, edit := range map[string]func(*dispatcher.Config){"drr": nil, "fifo": fifo} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := queuetest.New(t, queuetest.Config(t, queuetest.Edits(withCap(shopA, 2), longLeases)))
			enqueueMany(t, env, shopA, testkinds.Sleep, 6)
			r := start(t, env, name, edit)
			waitLeader(t, r)

			var mu sync.Mutex
			var held []store.Claimed
			var wg sync.WaitGroup
			for i := range 5 { // five workers waiting, a cap of two
				wg.Go(func() {
					j, err := r.client.Claim(t.Context(), fmt.Sprintf("w%d", i))
					if err != nil {
						return // the test ended
					}
					if j != nil {
						mu.Lock()
						held = append(held, *j)
						mu.Unlock()
					}
				})
			}
			queuetest.Eventually(t, 3*time.Second, "two jobs to be claimed", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(held) == 2
			})
			time.Sleep(600 * time.Millisecond) // two more refreshes
			mu.Lock()
			n := len(held)
			first := held[0]
			mu.Unlock()
			if n != 2 {
				t.Fatalf("%d jobs claimed with a cap of 2", n)
			}
			var running int
			if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM jobs WHERE state = 'running'`).Scan(&running); err != nil || running != 2 {
				t.Fatalf("%d jobs running, %v; want 2", running, err)
			}

			// One finishes. A waiting worker gets the next job at the next refresh.
			if ok, err := store.Complete(t.Context(), env.Pool, first.Fence()); err != nil || !ok {
				t.Fatalf("complete: %v, %v", ok, err)
			}
			queuetest.Eventually(t, 3*time.Second, "a third job to be claimed", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(held) == 3
			})
			time.Sleep(600 * time.Millisecond)
			mu.Lock()
			n = len(held)
			mu.Unlock()
			if n != 3 {
				t.Fatalf("%d jobs claimed after one completion, want 3", n)
			}
			// Release the pollers still waiting, so the test ends promptly.
			t.Cleanup(wg.Wait)
		})
	}
}

// One round serves every waiting worker, across shops, when each shop has a job.
func TestOneRoundServesWorkersAcrossShops(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	shops := []string{shopA, shopB, queuetest.ShopC, "shop-d.test"}
	for _, s := range shops {
		env.Enqueue(t, s, testkinds.Sleep, nil)
	}
	r := start(t, env, "across", nil)
	waitLeader(t, r)

	got := make(chan store.Claimed, len(shops))
	for i := range shops {
		go func() {
			if j, err := r.client.Claim(t.Context(), fmt.Sprintf("w%d", i)); err == nil && j != nil {
				got <- *j
			}
		}()
	}
	seen := map[string]bool{}
	for range shops {
		select {
		case j := <-got:
			seen[j.Shop] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d workers got a job: %v", len(seen), len(shops), seen)
		}
	}
	if len(seen) != len(shops) {
		t.Fatalf("workers got jobs from %v, want one from each of %d shops", seen, len(shops))
	}
}

// AC-6.2 (the dispatcher half): a throttled shop's jobs aren't claimed until
// throttled_until has passed by the database clock, and are claimed soon after.
func TestDispatcherWaitsOutAThrottle(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	var until time.Time
	if err := env.Pool.QueryRow(t.Context(),
		`UPDATE shop_state SET throttled_until = statement_timestamp() + interval '1200 milliseconds' WHERE shop_id = $1 RETURNING throttled_until`,
		shopA).Scan(&until); err != nil {
		t.Fatal(err)
	}
	r := start(t, env, "throttle", nil)
	waitLeader(t, r)

	j, err := r.client.Claim(t.Context(), "w")
	if err != nil || j == nil || j.ID != id {
		t.Fatalf("claim = %+v, %v; want job %d", j, err, id)
	}
	if j.ClaimedAt.Before(until) {
		t.Fatalf("claimed at %v, before throttled_until %v", j.ClaimedAt, until)
	}
	if wait := j.ClaimedAt.Sub(until); wait > 2*time.Second {
		t.Fatalf("claimed %v after throttled_until, want within a few refreshes", wait)
	}
}

// P4-6, AC-10.2: through the dispatcher, claims for a shop never pass its token
// bucket: rate × elapsed + burst. The bucket is the only limit here: the cap is
// high and the jobs are all queued.
func TestDispatcherClaimsStayWithinTheTokenBucket(t *testing.T) {
	t.Parallel()
	const rate, burst = 40.0, 3
	env := queuetest.New(t, queuetest.Config(t, queuetest.Edits(longLeases, queuetest.WithShop(shopA, func(s *config.Shop) {
		s.RatePerSec, s.RateBurst = rate, burst
	}))))
	enqueueMany(t, env, shopA, testkinds.Sleep, 200)
	began := time.Now()
	r := start(t, env, "bucket", func(c *dispatcher.Config) { c.Refresh = 20 * time.Millisecond })
	waitLeader(t, r)

	claimed := 0
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		j, err := r.client.Claim(ctx, fmt.Sprintf("w%d", claimed))
		cancel()
		if err == nil && j != nil {
			claimed++
			continue
		}
		// A timeout, or a claim answered after the worker left, isn't counted
		// here; the database is (below).
	}
	// Every claim adds 1 to a job's claim_seq, including one whose worker had
	// already given up, so the sum counts exactly the claims made.
	var inDB int
	if err := env.Pool.QueryRow(t.Context(), `SELECT coalesce(sum(claim_seq), 0) FROM jobs`).Scan(&inDB); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(began).Seconds()
	if bound := rate*elapsed + burst; float64(inDB) > bound {
		t.Fatalf("%d claims in %.2f s, over rate %.0f × elapsed + burst %d = %.1f", inDB, elapsed, rate, burst, bound)
	}
	if inDB < burst+10 {
		t.Fatalf("%d claims: the bucket never refilled (a rate of %.0f/s should have allowed about %.0f)", inDB, rate, rate*1.5)
	}
}
