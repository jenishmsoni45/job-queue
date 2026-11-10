package store_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/admission"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

func batchOf(shop string, keys ...string) []store.NewJob {
	jobs := make([]store.NewJob, len(keys))
	for i, k := range keys {
		jobs[i] = store.NewJob{Shop: shop, Kind: testkinds.Sleep, Payload: []byte(`{}`), DedupeKey: k}
	}
	return jobs
}

func enqueueBatch(t *testing.T, env *queuetest.Env, jobs []store.NewJob) (store.BatchResult, error) {
	t.Helper()
	return store.EnqueueBatch(t.Context(), env.Pool, jobs)
}

// AC-2.1: a batch creates every job, in order, adds them all to the backlog and
// sends one notification.
func TestBatchCreatesEveryJobInOrderWithOneNotify(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	conn, err := pgx.ConnectConfig(t.Context(), env.DB.ConnConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(t.Context()) })
	if _, err := conn.Exec(t.Context(), "LISTEN "+store.EnqueueNotifyChannel); err != nil {
		t.Fatal(err)
	}

	res, err := enqueueBatch(t, env, batchOf(shopA, "k1", "", "k3", ""))
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 4 || len(res.IDs) != 4 || len(res.Duplicates) != 0 {
		t.Fatalf("result = %+v, want 4 created in order, no duplicates", res)
	}
	if !slices.IsSorted(res.IDs) {
		t.Fatalf("ids %v are not in request order", res.IDs)
	}
	for i, id := range res.IDs {
		if j := env.Job(t, id); j.State != store.StateQueued || j.Shop != shopA {
			t.Fatalf("job %d (%d) = %+v", i, id, j)
		}
	}
	if got := backlog(t, env, shopA); got != 4 {
		t.Fatalf("backlog %d, want 4", got)
	}
	if n := countKeys(t, env); n != 2 {
		t.Fatalf("%d dedupe keys, want 2", n)
	}
	env.CheckBacklog(t)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if n, err := conn.WaitForNotification(ctx); err != nil || n.Payload != shopA {
		t.Fatalf("notification = %+v, %v; want one for %s", n, err, shopA)
	}
	ctx, cancel = context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if n, err := conn.WaitForNotification(ctx); err == nil {
		t.Fatalf("a second notification arrived: %+v", n)
	}
}

// P4-4, AC-2.1: a batch is all or nothing. Whatever is wrong with one job, none
// of the others is created, and the backlog is unchanged.
func TestBatchWithOneBadJobCreatesNothing(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	for name, edit := range map[string]func([]store.NewJob){
		"an unknown kind": func(jobs []store.NewJob) { jobs[2].Kind = "no.such.kind" },
		"an unstorable payload": func(jobs []store.NewJob) {
			jobs[3].Payload = []byte(`{"x": "\u0000"}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			jobs := batchOf(shopA, "a", "b", "c", "d", "e")
			edit(jobs)
			_, err := enqueueBatch(t, env, jobs)
			if err == nil {
				t.Fatal("a batch with a bad job was accepted")
			}
			if n := countJobs(t, env); n != 0 {
				t.Fatalf("%d jobs created", n)
			}
			if n := countKeys(t, env); n != 0 {
				t.Fatalf("%d dedupe keys created", n)
			}
			if got := backlog(t, env, shopA); got != 0 {
				t.Fatalf("backlog %d", got)
			}
		})
	}
}

// P4-4, AC-2.4: the store refuses a batch it can't make sense of, and creates nothing.
func TestBatchRejectsEmptySelfDuplicatingAndMixedShopBatches(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	if _, err := enqueueBatch(t, env, nil); !errors.Is(err, store.ErrEmptyBatch) {
		t.Fatalf("empty batch: %v", err)
	}
	if _, err := enqueueBatch(t, env, batchOf(shopA, "same", "x", "same")); !errors.Is(err, store.ErrDuplicateKeyInBatch) {
		t.Fatalf("a key twice in the batch: %v", err)
	}
	mixed := batchOf(shopA, "", "")
	mixed[1].Shop = shopB
	if _, err := enqueueBatch(t, env, mixed); !errors.Is(err, store.ErrMixedShops) {
		t.Fatalf("two shops: %v", err)
	}
	if _, err := enqueueBatch(t, env, batchOf("no-such-shop.test", "", "")); !errors.Is(err, store.ErrUnknownShop) {
		t.Fatalf("unknown shop: %v", err)
	}
	if n := countJobs(t, env); n != 0 {
		t.Fatalf("%d jobs created by refused batches", n)
	}
}

// P4-4, AC-2.3: a job whose key exists is reported with the existing ID, is not
// counted toward the limit and creates nothing.
func TestBatchReportsExistingKeysAndDoesNotCountThem(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 4, 1000)))
	first, err := enqueueBatch(t, env, batchOf(shopA, "k1", "k2", "k3"))
	if err != nil {
		t.Fatal(err)
	}

	// Backlog 3 of 4. Two existing keys and one new job: 1 new, which fits.
	res, err := enqueueBatch(t, env, batchOf(shopA, "k1", "fresh", "k3"))
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{first.IDs[0], res.IDs[1], first.IDs[2]}
	if !slices.Equal(res.IDs, want) || res.Created != 1 || !slices.Equal(res.Duplicates, []int64{first.IDs[0], first.IDs[2]}) {
		t.Fatalf("result = %+v, want ids %v, 1 created, duplicates %v", res, want, []int64{first.IDs[0], first.IDs[2]})
	}
	if res.IDs[1] == first.IDs[0] || res.IDs[1] == first.IDs[2] {
		t.Fatal("the new job reuses an existing ID")
	}
	if got := backlog(t, env, shopA); got != 4 {
		t.Fatalf("backlog %d, want 4", got)
	}

	// Full now. A batch of only duplicates still succeeds, and creates nothing.
	res, err = enqueueBatch(t, env, batchOf(shopA, "k1", "k2"))
	if err != nil || res.Created != 0 || !slices.Equal(res.IDs, first.IDs[:2]) || !slices.Equal(res.Duplicates, first.IDs[:2]) {
		t.Fatalf("all-duplicate batch at the limit = %+v, %v", res, err)
	}
	if n := countJobs(t, env); n != 4 {
		t.Fatalf("%d jobs, want 4", n)
	}
	env.CheckBacklog(t)
}

// P4-3, Q8 (AC-2.2): a batch that would pass the limit is rejected whole. It
// creates no job and no key, and counts its new jobs, not its duplicates, as shed (D-049).
func TestBatchOverTheLimitIsRejectedWholeAndCountsNewJobsAsShed(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 5, 3200)))
	if _, err := enqueueBatch(t, env, batchOf(shopA, "k1", "k2", "k3")); err != nil {
		t.Fatal(err)
	}

	// Two of these five exist. The three new ones would make a backlog of 6 > 5.
	_, err := enqueueBatch(t, env, batchOf(shopA, "k1", "n1", "k2", "n2", "n3"))
	var rej *admission.Rejected
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want *admission.Rejected", err)
	}
	if rej.Shed != 3 || rej.RetryAfter != 3200*time.Millisecond {
		t.Fatalf("rejection = %+v, want 3 shed and Retry-After 3.2 s", rej)
	}
	if n := countJobs(t, env); n != 3 {
		t.Fatalf("%d jobs, want the first batch's 3: a rejected batch creates nothing", n)
	}
	if n := countKeys(t, env); n != 3 {
		t.Fatalf("%d keys, want 3", n)
	}
	if got := shedTotal(t, env, shopA); got != 3 {
		t.Fatalf("shed_total %d, want 3", got)
	}
	if got := backlog(t, env, shopA); got != 3 {
		t.Fatalf("backlog %d, want 3", got)
	}
	// A batch that fits exactly is accepted: the check is exact.
	if _, err := enqueueBatch(t, env, batchOf(shopA, "n1", "n2")); err != nil {
		t.Fatalf("a batch that fills the limit exactly: %v", err)
	}
	env.CheckBacklog(t)
}

// P4-7: another shop's batches are unaffected by a full shop.
func TestBatchRejectionsAreCountedPerShop(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withBacklogLimit(shopA, 2, 1000)))
	if _, err := enqueueBatch(t, env, batchOf(shopA, "", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := enqueueBatch(t, env, batchOf(shopA, "", "", "")); err == nil {
		t.Fatal("a full shop accepted a batch")
	}
	if _, err := enqueueBatch(t, env, batchOf(shopB, "", "", "")); err != nil {
		t.Fatalf("shop B: %v", err)
	}
	if a, b := shedTotal(t, env, shopA), shedTotal(t, env, shopB); a != 3 || b != 0 {
		t.Fatalf("shed_total A=%d B=%d, want 3 and 0", a, b)
	}
}

// Q4 for batches: batches racing with the same keys create each key's job once.
func TestConcurrentBatchesWithTheSameKeysCreateEachJobOnce(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	const callers = 8
	results := make([]store.BatchResult, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			res, err := enqueueBatch(t, env, batchOf(shopA, "ka", "kb", "kc"))
			if err != nil {
				t.Errorf("batch %d: %v", i, err)
			}
			results[i] = res
		})
	}
	wg.Wait()
	if n := countJobs(t, env); n != 3 {
		t.Fatalf("%d jobs, want 3", n)
	}
	created := 0
	for _, r := range results {
		created += r.Created
		if !slices.Equal(r.IDs, results[0].IDs) {
			t.Fatalf("batches got different IDs: %v and %v", r.IDs, results[0].IDs)
		}
	}
	if created != 3 {
		t.Fatalf("batches report %d jobs created in all, want 3", created)
	}
	env.CheckBacklog(t)
}

// A maximum-size batch goes in with one lock and a single round trip.
func TestLargeBatch(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}
	res, err := enqueueBatch(t, env, batchOf(shopA, keys...))
	if err != nil || res.Created != 1000 {
		t.Fatalf("result = %d created, %v", res.Created, err)
	}
	if got := backlog(t, env, shopA); got != 1000 {
		t.Fatalf("backlog %d", got)
	}
	env.CheckBacklog(t)
}
