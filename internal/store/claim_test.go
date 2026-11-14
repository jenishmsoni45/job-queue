package store_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// withCap sets a shop's concurrency cap.
func withCap(shop string, maxConcurrency int) func(*config.File) {
	return queuetest.WithShop(shop, func(s *config.Shop) { s.MaxConcurrency = maxConcurrency })
}

// withCost sets the DRR cost of a kind.
func withCost(kind string, cost int) func(*config.File) {
	return func(c *config.File) { queuetest.SetKind(c, kind, func(k *config.Kind) { k.Cost = cost }) }
}

func owners(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("w%d", i+1)
	}
	return names
}

func claimFor(t *testing.T, env *queuetest.Env, n int, spec store.ClaimSpec) store.ClaimOutcome {
	t.Helper()
	out, err := store.ClaimFor(t.Context(), env.Pool, owners(n), spec)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func runningCount(t *testing.T, env *queuetest.Env, shop string) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM jobs WHERE shop_id = $1 AND state = 'running'`, shop).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// P4-1, AC-10.1: the claim transaction never starts a job past the shop's cap.
// Completing a job makes room for exactly one more.
func TestClaimNeverPassesTheConcurrencyCap(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withCap(shopA, 2)))
	for range 5 {
		env.Enqueue(t, shopA, testkinds.Sleep, nil)
	}

	out := claimFor(t, env, 5, store.ClaimSpec{})
	if len(out.Jobs) != 2 || out.End != store.ClaimBlocked {
		t.Fatalf("claimed %d, end %v; want 2 and ClaimBlocked", len(out.Jobs), out.End)
	}
	if out = claimFor(t, env, 5, store.ClaimSpec{}); len(out.Jobs) != 0 || out.End != store.ClaimBlocked {
		t.Fatalf("a capped shop claimed %d, end %v", len(out.Jobs), out.End)
	}
	first := claimFor(t, env, 1, store.ClaimSpec{Shops: []string{"nowhere.test"}})
	if len(first.Jobs) != 0 {
		t.Fatalf("claimed %d from a shop that has no jobs", len(first.Jobs))
	}

	done, err := store.Complete(t.Context(), env.Pool, store.Fence{JobID: 1, ClaimSeq: 1, Owner: "w1"})
	if err != nil || !done {
		t.Fatalf("complete: %v, %v", done, err)
	}
	if out = claimFor(t, env, 5, store.ClaimSpec{}); len(out.Jobs) != 1 {
		t.Fatalf("after one completion claimed %d, want 1", len(out.Jobs))
	}
	if got := runningCount(t, env, shopA); got != 2 {
		t.Fatalf("%d running, want the cap of 2", got)
	}
}

// The cap is per shop: a capped shop's jobs don't stop other shops' jobs from
// being claimed in the same FIFO claim, even when they are older.
func TestCappedShopDoesNotBlockOtherShopsInTheSameClaim(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withCap(shopA, 1)))
	for range 3 {
		env.Enqueue(t, shopA, testkinds.Sleep, nil) // the oldest jobs
	}
	b := []int64{env.Enqueue(t, shopB, testkinds.Sleep, nil), env.Enqueue(t, shopB, testkinds.Sleep, nil)}

	out := claimFor(t, env, 6, store.ClaimSpec{})
	got := map[string]int{}
	for _, j := range out.Jobs {
		got[j.Shop]++
	}
	if got[shopA] != 1 || got[shopB] != 2 || out.End != store.ClaimBlocked {
		t.Fatalf("claimed %v, end %v; want 1 from A (its cap), 2 from B, ClaimBlocked", got, out.End)
	}
	for _, id := range b {
		if env.Job(t, id).State != store.StateRunning {
			t.Fatalf("shop B's job %d was not claimed", id)
		}
	}
}

// The token limit takes away from a claim exactly what the dispatcher says the
// bucket can't pay for.
func TestClaimHonoursPerShopLimits(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	for range 3 {
		env.Enqueue(t, shopA, testkinds.Sleep, nil)
		env.Enqueue(t, shopB, testkinds.Sleep, nil)
	}
	out := claimFor(t, env, 6, store.ClaimSpec{Limits: map[string]int{shopA: 1, shopB: 0}})
	if len(out.Jobs) != 1 || out.Jobs[0].Shop != shopA || out.End != store.ClaimBlocked {
		t.Fatalf("claimed %+v, end %v; want one job of A and ClaimBlocked", out.Jobs, out.End)
	}
}

// AC-6.2 (the claim half): a claim that starts after throttled_until was set
// skips the shop, and takes its jobs again once the time has passed.
func TestClaimSkipsAThrottledShop(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	env.Enqueue(t, shopB, testkinds.Sleep, nil)
	if _, err := env.Pool.Exec(t.Context(),
		`UPDATE shop_state SET throttled_until = statement_timestamp() + interval '1 hour' WHERE shop_id = $1`, shopA); err != nil {
		t.Fatal(err)
	}
	out := claimFor(t, env, 4, store.ClaimSpec{})
	if len(out.Jobs) != 1 || out.Jobs[0].Shop != shopB || out.End != store.ClaimBlocked {
		t.Fatalf("claimed %+v, end %v; want only shop B's job and ClaimBlocked", out.Jobs, out.End)
	}
	if _, err := env.Pool.Exec(t.Context(),
		`UPDATE shop_state SET throttled_until = statement_timestamp() - interval '1 second' WHERE shop_id = $1`, shopA); err != nil {
		t.Fatal(err)
	}
	if out = claimFor(t, env, 4, store.ClaimSpec{}); len(out.Jobs) != 1 || out.Jobs[0].Shop != shopA {
		t.Fatalf("after throttled_until passed, claimed %+v", out.Jobs)
	}
}

// P4-5, AC-9.1: a claim with an allowance takes jobs by their cost, oldest first.
func TestClaimChargesEachJobsCostAgainstTheAllowance(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withCost(testkinds.Fail, 3)))
	ids := []int64{
		env.Enqueue(t, shopA, testkinds.Fail, nil),  // cost 3
		env.Enqueue(t, shopA, testkinds.Sleep, nil), // cost 1
		env.Enqueue(t, shopA, testkinds.Sleep, nil), // cost 1
		env.Enqueue(t, shopA, testkinds.Fail, nil),  // cost 3
	}
	out := claimFor(t, env, 4, store.ClaimSpec{Shops: []string{shopA}, MaxCost: 5})
	if len(out.Jobs) != 3 || out.Cost != 5 || out.End != store.ClaimCost {
		t.Fatalf("claimed %d jobs costing %d, end %v; want 3 jobs costing 5 and ClaimCost", len(out.Jobs), out.Cost, out.End)
	}
	for i, j := range out.Jobs {
		if j.ID != ids[i] {
			t.Fatalf("job %d claimed is %d, want %d (oldest first)", i, j.ID, ids[i])
		}
	}
	if env.Job(t, ids[3]).State != store.StateQueued {
		t.Fatal("the job that didn't fit was claimed")
	}
}

// A job that doesn't fit ends the claim; the claim doesn't skip to a cheaper
// one behind it, so a shop's jobs still go in order.
func TestClaimDoesNotSkipAheadToACheaperJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withCost(testkinds.Fail, 3)))
	expensive := env.Enqueue(t, shopA, testkinds.Fail, nil)
	cheap := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	out := claimFor(t, env, 2, store.ClaimSpec{Shops: []string{shopA}, MaxCost: 2})
	if len(out.Jobs) != 0 || out.End != store.ClaimCost {
		t.Fatalf("claimed %d, end %v; want 0 and ClaimCost", len(out.Jobs), out.End)
	}
	if env.Job(t, expensive).State != store.StateQueued || env.Job(t, cheap).State != store.StateQueued {
		t.Fatal("a job was claimed")
	}
}

// A claim that names shops takes only their jobs, even when others are older.
func TestClaimForNamedShopsIgnoresOtherShops(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	env.Enqueue(t, shopA, testkinds.Sleep, nil)
	want := env.Enqueue(t, shopB, testkinds.Sleep, nil)
	out := claimFor(t, env, 3, store.ClaimSpec{Shops: []string{shopB}})
	if len(out.Jobs) != 1 || out.Jobs[0].ID != want || out.End != store.ClaimDry {
		t.Fatalf("claimed %+v, end %v; want shop B's job and ClaimDry", out.Jobs, out.End)
	}
}

// P4-5, AC-9.3: a claim with no shop filter and no allowance, which is what FIFO
// offers, takes the globally oldest runnable jobs by (run_at, id) across shops.
func TestFIFOClaimTakesTheGloballyOldestJobs(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	now := env.DBNow(t)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }
	// Enqueued in this order, but run_at puts the jobs in the order c, a, d, b.
	b := env.EnqueueAt(t, shopB, testkinds.Sleep, nil, at(-1*time.Second))
	a := env.EnqueueAt(t, shopA, testkinds.Sleep, nil, at(-3*time.Second))
	d := env.EnqueueAt(t, shopB, testkinds.Sleep, nil, at(-2*time.Second))
	c := env.EnqueueAt(t, shopA, testkinds.Sleep, nil, at(-4*time.Second))
	_ = env.EnqueueAt(t, shopA, testkinds.Sleep, nil, at(time.Hour)) // not runnable yet

	out := claimFor(t, env, 10, store.ClaimSpec{})
	var got []int64
	for _, j := range out.Jobs {
		got = append(got, j.ID)
	}
	want := []int64{c, a, d, b}
	if fmt.Sprint(got) != fmt.Sprint(want) || out.End != store.ClaimDry {
		t.Fatalf("claimed %v, end %v; want %v and ClaimDry", got, out.End, want)
	}
}

func TestClaimEndFull(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	for range 3 {
		env.Enqueue(t, shopA, testkinds.Sleep, nil)
	}
	if out := claimFor(t, env, 2, store.ClaimSpec{}); len(out.Jobs) != 2 || out.End != store.ClaimFull {
		t.Fatalf("claimed %d, end %v; want 2 and ClaimFull", len(out.Jobs), out.End)
	}
	if out := claimFor(t, env, 0, store.ClaimSpec{}); len(out.Jobs) != 0 {
		t.Fatalf("claimed %d with no owners", len(out.Jobs))
	}
}

// The claim and the snapshot must be answered from jobs_queued_shop even when the
// planner's statistics are stale, which they are just after a flood (D-059). The table
// is analysed while it holds only finished jobs, then a flooding shop queues 45,000 jobs
// and a quiet shop one, with no ANALYZE after. With sorts allowed the planner believes
// the quiet shop has about one queued job and picks jobs_queued_fifo plus a top-N Sort,
// which reads the whole flood (9,157 buffers in the snapshot, against 41).
func TestClaimAndSnapshotQueriesUseTheShopIndexWithStaleStatistics(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	ctx := t.Context()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := env.Pool.Exec(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO jobs (shop_id, kind, cost, payload, state, run_at, first_runnable_at, completed_at, claim_seq)
	      SELECT s, $2, 1, '{}', 'completed', now() - interval '1 day', now() - interval '1 day', now() - interval '1 day', 1
	        FROM unnest($1::text[]) s, generate_series(1, 2400)`, []string{shopA, shopB}, testkinds.Sleep)
	exec(`ANALYZE jobs`)
	exec(`INSERT INTO jobs (shop_id, kind, cost, payload, state, run_at, first_runnable_at)
	      SELECT $1, $2, 1, '{}', 'queued', now() - interval '30 seconds' + g * interval '1 millisecond',
	             now() - interval '30 seconds' + g * interval '1 millisecond'
	        FROM generate_series(1, 45000) g`, shopA, testkinds.Sleep)
	exec(`INSERT INTO jobs (shop_id, kind, cost, payload, state, run_at, first_runnable_at)
	      VALUES ($1, $2, 1, '{}', 'queued', now() - interval '1 second', now() - interval '1 second')`, shopB, testkinds.Sleep)

	// The claim for the quiet shop, which DRR sends for the shop whose turn it is, still works.
	out := claimFor(t, env, 1, store.ClaimSpec{Shops: []string{shopB}})
	if len(out.Jobs) != 1 || out.Jobs[0].Shop != shopB {
		t.Fatalf("claimed %+v, want shop B's one job", out.Jobs)
	}
	env.Enqueue(t, shopB, testkinds.Sleep, nil) // so the plan below has a row to find

	cand := scans(t, env, store.CandidatesSQL(1), 1, shopB)
	if !slices.Contains(cand, "jobs_queued_shop") || slices.Contains(cand, "jobs_queued_fifo") || slices.Contains(cand, "Sort") {
		t.Errorf("the single-shop claim statement is planned on %v, want jobs_queued_shop alone", cand)
	}
	// The UPDATE that starts the locked candidates is found by primary key. Joined to
	// unnest it was a nested loop over every queued row (D-060).
	var ids []int64
	rows, err := env.Pool.Query(ctx, `SELECT id FROM jobs WHERE state = 'queued' AND shop_id = $1 ORDER BY run_at LIMIT 3`, shopA)
	if err == nil {
		ids, err = pgx.CollectRows(rows, pgx.RowTo[int64])
	}
	if err != nil || len(ids) != 3 {
		t.Fatalf("picking ids: %v, %v", ids, err)
	}
	upd := scans(t, env, store.ClaimRowsSQL, ids, []string{"w1", "w2", "w3"})
	if !slices.Contains(upd, "jobs_pkey") || slices.Contains(upd, "Seq Scan on jobs") || slices.Contains(upd, "jobs_queued_fifo") {
		t.Errorf("the claim's UPDATE is planned on %v, want jobs_pkey and no scan of the queue", upd)
	}
	snap := scans(t, env, store.SnapshotShopsSQL)
	if !slices.Contains(snap, "jobs_queued_shop") || slices.Contains(snap, "jobs_queued_fifo") || slices.Contains(snap, "Sort") {
		t.Errorf("the snapshot statement is planned on %v, want jobs_queued_shop alone", snap)
	}
}

// scans EXPLAINs a statement and lists its scans: the index name of an index scan,
// and "Seq Scan on <table>" for a sequential one.
func scans(t *testing.T, env *queuetest.Env, query string, args ...any) []string {
	t.Helper()
	var js string
	err := pgx.BeginFunc(t.Context(), env.Pool, func(tx pgx.Tx) error {
		// What store does before these statements: noPlannerSort.
		if _, err := tx.Exec(t.Context(), `SET LOCAL enable_sort = off`); err != nil {
			return err
		}
		return tx.QueryRow(t.Context(), `EXPLAIN (FORMAT JSON) `+query, args...).Scan(&js)
	})
	if err != nil {
		t.Fatal(err)
	}
	var top []struct{ Plan map[string]any }
	if err := json.Unmarshal([]byte(js), &top); err != nil || len(top) != 1 {
		t.Fatalf("plan %s: %v", js, err)
	}
	var found []string
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		switch typ, _ := node["Node Type"].(string); typ {
		case "Seq Scan":
			found = append(found, fmt.Sprintf("Seq Scan on %v", node["Relation Name"]))
		case "Sort", "Incremental Sort":
			found = append(found, "Sort")
		case "Index Scan", "Index Only Scan", "Bitmap Index Scan":
			found = append(found, fmt.Sprint(node["Index Name"]))
		}
		if kids, ok := node["Plans"].([]any); ok {
			for _, k := range kids {
				walk(k.(map[string]any))
			}
		}
	}
	walk(top[0].Plan)
	return found
}
