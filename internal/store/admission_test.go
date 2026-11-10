package store_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/admission"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// shopEdit changes one shop's settings in the test config.
func shopEdit(shop string, set func(*config.Shop)) func(*config.File) {
	return func(c *config.File) {
		for i := range c.Shops {
			if c.Shops[i].ID == shop {
				set(&c.Shops[i])
				return
			}
		}
		panic("no shop " + shop)
	}
}

// edits applies each edit in turn.
func edits(list ...func(*config.File)) func(*config.File) {
	return func(c *config.File) {
		for _, e := range list {
			e(c)
		}
	}
}

// withBacklogLimit sets a shop's backlog limit and its Retry-After.
func withBacklogLimit(shop string, limit, retryAfterMS int) func(*config.File) {
	return shopEdit(shop, func(s *config.Shop) { s.BacklogLimit, s.BacklogRetryAfterMS = limit, retryAfterMS })
}

func shedTotal(t *testing.T, env *queuetest.Env, shop string) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT shed_total FROM shop_state WHERE shop_id = $1`, shop).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func enqueueJob(t *testing.T, env *queuetest.Env, shop, key string) (store.Enqueued, error) {
	t.Helper()
	return store.Enqueue(t.Context(), env.Pool, store.NewJob{Shop: shop, Kind: testkinds.Sleep, Payload: []byte(`{}`), DedupeKey: key})
}

// P4-3, Q8 (NFR-8, AC-11.1): at the backlog limit an enqueue is rejected with the
// shop's Retry-After, and creates no job and no dedupe key. The shop's shed
// count goes up by one.
func TestEnqueueAtTheBacklogLimitIsRejectedAndCreatesNothing(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 3, 2500)))
	for i := range 3 {
		if _, err := enqueueJob(t, env, shopA, fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("enqueue %d under the limit: %v", i, err)
		}
	}

	for _, key := range []string{"", "over-the-limit"} {
		_, err := enqueueJob(t, env, shopA, key)
		var rej *admission.Rejected
		if !errors.As(err, &rej) {
			t.Fatalf("enqueue at the limit (key %q): err = %v, want *admission.Rejected", key, err)
		}
		if rej.Shop != shopA || rej.Shed != 1 || rej.RetryAfter != 2500*time.Millisecond {
			t.Fatalf("rejection = %+v, want shop %s, 1 shed, Retry-After 2.5 s", rej, shopA)
		}
	}
	if n := countJobs(t, env); n != 3 {
		t.Fatalf("%d jobs, want the 3 accepted ones", n)
	}
	if n := countKeys(t, env); n != 3 {
		t.Fatalf("%d dedupe keys, want 3: a rejected enqueue stores no key", n)
	}
	if got := backlog(t, env, shopA); got != 3 {
		t.Fatalf("backlog %d, want 3", got)
	}
	if got := shedTotal(t, env, shopA); got != 2 {
		t.Fatalf("shed_total %d, want 2 (one per rejected enqueue)", got)
	}
	env.CheckBacklog(t)
}

// AC-1.4, D-018: a duplicate is answered with the existing job even when the
// shop is full, and is not counted as shed.
func TestDuplicateAtTheBacklogLimitStillGetsTheExistingJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 1, 1000)))
	first, err := enqueueJob(t, env, shopA, "k")
	if err != nil {
		t.Fatal(err)
	}
	again, err := enqueueJob(t, env, shopA, "k")
	if err != nil || !again.Duplicate || again.ID != first.ID {
		t.Fatalf("duplicate at the limit = %+v, %v; want the existing job %d", again, err, first.ID)
	}
	if got := shedTotal(t, env, shopA); got != 0 {
		t.Fatalf("shed_total %d, want 0: a duplicate isn't shed", got)
	}
}

// The backlog counts running jobs too, and a finished job makes room again.
func TestBacklogLimitCountsRunningJobsAndFreesOnCompletion(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 2, 1000)))
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	running := claimOne(t, env, "w")

	var rej *admission.Rejected
	if _, err := enqueueJob(t, env, shopA, ""); !errors.As(err, &rej) {
		t.Fatalf("one running and one queued is a backlog of 2: err = %v, want a rejection", err)
	}
	if ok, err := store.Complete(t.Context(), env.Pool, running.Fence()); err != nil || !ok {
		t.Fatalf("complete: %v, %v", ok, err)
	}
	if _, err := enqueueJob(t, env, shopA, ""); err != nil {
		t.Fatalf("enqueue after a completion freed room: %v", err)
	}
	env.CheckBacklog(t)
}

// P4-7, AC-11.2: a shop at its limit rejects only its own enqueues.
func TestOneShopsRejectionsNeverRejectAnotherShop(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 1, 1000)))
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	for range 5 {
		if _, err := enqueueJob(t, env, shopA, ""); err == nil {
			t.Fatal("a full shop accepted an enqueue")
		}
		if _, err := enqueueJob(t, env, shopB, ""); err != nil {
			t.Fatalf("shop B rejected while only shop A is full: %v", err)
		}
	}
	if a, b := shedTotal(t, env, shopA), shedTotal(t, env, shopB); a != 5 || b != 0 {
		t.Fatalf("shed_total A=%d B=%d, want 5 and 0", a, b)
	}
}

// Exactness under concurrency: with the limit at 5, twenty racing enqueues
// create exactly five jobs and reject fifteen, and every rejection is counted.
func TestConcurrentEnqueuesNeverPassTheLimit(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 5, 1000)))
	const callers = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, rejected := 0, 0
	for range callers {
		wg.Go(func() {
			_, err := enqueueJob(t, env, shopA, "")
			var rej *admission.Rejected
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.As(err, &rej):
				rejected++
			default:
				t.Errorf("enqueue: %v", err)
			}
		})
	}
	wg.Wait()
	if accepted != 5 || rejected != callers-5 {
		t.Fatalf("accepted %d, rejected %d; want 5 and %d", accepted, rejected, callers-5)
	}
	if n := countJobs(t, env); n != 5 {
		t.Fatalf("%d jobs, want 5", n)
	}
	if got := shedTotal(t, env, shopA); got != callers-5 {
		t.Fatalf("shed_total %d, want %d", got, callers-5)
	}
	env.CheckBacklog(t)
}
