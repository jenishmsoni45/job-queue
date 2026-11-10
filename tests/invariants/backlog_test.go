package invariants

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/admission"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// P2-9, P4-8: after any sequence of enqueues, batch enqueues, rejections at the
// backlog limit, claims, results (complete, fail, permanent), expiries, reaper
// passes and stale writes, each shop's backlog counter equals its real number of
// queued and running jobs (design §3). Every admission decision is exact: an
// enqueue is rejected exactly when its new jobs would pass the limit, and shed_total
// counts exactly the jobs rejected.
func TestBacklogCounterMatchesRealCount(t *testing.T) {
	t.Parallel()
	// Long leases: the test expires leases itself, by moving lease_expires_at
	// to the database's past. Three attempts, so jobs also die by failures and
	// by the reaper.
	env := queuetest.New(t, queuetest.Config(t, shortLeases(60_000, 10_000, 3)))
	shops := []string{shopA, shopB, shopC}

	rapid.Check(t, func(rt *rapid.T) {
		ctx := context.Background()
		limit := rapid.IntRange(1, 7).Draw(rt, "backlog limit")
		if _, err := env.Pool.Exec(ctx, `TRUNCATE jobs, idempotency_keys, dedupe_keys; UPDATE shop_state SET backlog = 0, shed_total = 0`); err != nil {
			rt.Fatal(err)
		}
		if _, err := env.Pool.Exec(ctx, `UPDATE shops SET backlog_limit = $1`, limit); err != nil {
			rt.Fatal(err)
		}
		shed := map[string]int{} // jobs rejected, per shop, by this test's own count
		backlogOf := func(shop string) int {
			var n int
			if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE shop_id = $1 AND state IN ('queued', 'running')`, shop).Scan(&n); err != nil {
				rt.Fatal(err)
			}
			return n
		}
		// admit checks one enqueue's outcome against the limit, exactly. newJobs
		// is how many jobs it would create.
		admit := func(shop string, before, newJobs int, err error) {
			var rej *admission.Rejected
			switch {
			case errors.As(err, &rej):
				if before+newJobs <= limit {
					rt.Fatalf("shop %s: %d new jobs rejected with a backlog of %d under a limit of %d", shop, newJobs, before, limit)
				}
				if rej.Shed != newJobs {
					rt.Fatalf("shop %s: rejection says %d shed, want %d", shop, rej.Shed, newJobs)
				}
				shed[shop] += newJobs
				if after := backlogOf(shop); after != before {
					rt.Fatalf("shop %s: a rejected enqueue changed the backlog from %d to %d", shop, before, after)
				}
			case err != nil:
				rt.Fatal(err)
			case before+newJobs > limit:
				rt.Fatalf("shop %s: %d new jobs accepted with a backlog of %d under a limit of %d", shop, newJobs, before, limit)
			}
		}
		keyExists := func(shop, key string) bool {
			var n int
			if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM dedupe_keys WHERE shop_id = $1 AND key = $2`, shop, key).Scan(&n); err != nil {
				rt.Fatal(err)
			}
			return n > 0
		}
		var held, over []store.Fence // live claims; claims that have ended
		owners := 0
		take := func(rt *rapid.T, list *[]store.Fence) store.Fence {
			if len(*list) == 0 {
				rt.Skip("nothing to act on")
			}
			i := rapid.IntRange(0, len(*list)-1).Draw(rt, "index")
			f := (*list)[i]
			*list = append((*list)[:i], (*list)[i+1:]...)
			return f
		}

		rt.Repeat(map[string]func(*rapid.T){
			"enqueue": func(rt *rapid.T) {
				job := store.NewJob{Shop: rapid.SampledFrom(shops).Draw(rt, "shop"), Kind: testkinds.Sleep, Payload: []byte(`{}`)}
				if rapid.Bool().Draw(rt, "scheduled") {
					at := time.Now().Add(time.Hour)
					job.RunAt = &at
				}
				newJobs := 1
				if rapid.Bool().Draw(rt, "with a dedupe key") {
					job.DedupeKey = rapid.SampledFrom([]string{"k0", "k1", "k2", "k3"}).Draw(rt, "key")
					if keyExists(job.Shop, job.DedupeKey) {
						newJobs = 0 // a duplicate: answered with the existing job, never rejected
					}
				}
				before := backlogOf(job.Shop)
				_, err := store.Enqueue(ctx, env.Pool, job)
				admit(job.Shop, before, newJobs, err)
			},
			"batch": func(rt *rapid.T) {
				shop := rapid.SampledFrom(shops).Draw(rt, "shop")
				size := rapid.IntRange(1, 5).Draw(rt, "batch size")
				keys := rapid.SliceOfNDistinct(rapid.SampledFrom([]string{"k0", "k1", "k2", "k3", "k4", ""}), 0, size, func(k string) string { return k }).Draw(rt, "keys")
				// One job per key, and keyless jobs to make up the size.
				jobs := make([]store.NewJob, 0, size)
				newJobs := 0
				for _, k := range keys {
					jobs = append(jobs, store.NewJob{Shop: shop, Kind: testkinds.Sleep, Payload: []byte(`{}`), DedupeKey: k})
				}
				for len(jobs) < size {
					jobs = append(jobs, store.NewJob{Shop: shop, Kind: testkinds.Sleep, Payload: []byte(`{}`)})
				}
				for _, j := range jobs {
					if j.DedupeKey == "" || !keyExists(shop, j.DedupeKey) {
						newJobs++
					}
				}
				before := backlogOf(shop)
				res, err := store.EnqueueBatch(ctx, env.Pool, jobs)
				admit(shop, before, newJobs, err)
				if err == nil && res.Created != newJobs {
					rt.Fatalf("batch created %d jobs, want %d", res.Created, newJobs)
				}
			},
			"claim": func(rt *rapid.T) {
				n := rapid.IntRange(1, 3).Draw(rt, "n")
				names := make([]string, n)
				for i := range names {
					owners++
					names[i] = fmt.Sprintf("w%d", owners)
				}
				jobs, err := store.Claim(ctx, env.Pool, names)
				if err != nil {
					rt.Fatal(err)
				}
				for _, j := range jobs {
					held = append(held, j.Fence())
				}
			},
			"complete": func(rt *rapid.T) {
				f := take(rt, &held)
				if _, err := store.Complete(ctx, env.Pool, f); err != nil {
					rt.Fatal(err)
				}
				over = append(over, f)
			},
			"fail": func(rt *rapid.T) {
				f := take(rt, &held)
				backoff := rapid.SampledFrom([]time.Duration{0, time.Hour}).Draw(rt, "backoff")
				if _, err := store.Fail(ctx, env.Pool, f, "failed", backoff); err != nil {
					rt.Fatal(err)
				}
				over = append(over, f)
			},
			"permanent": func(rt *rapid.T) {
				f := take(rt, &held)
				if _, err := store.Permanent(ctx, env.Pool, f, "permanent"); err != nil {
					rt.Fatal(err)
				}
				over = append(over, f)
			},
			"expire": func(rt *rapid.T) {
				f := take(rt, &held)
				if _, err := env.Pool.Exec(ctx, `
					UPDATE jobs SET lease_expires_at = statement_timestamp() - interval '1 millisecond'
					 WHERE id = $1 AND claim_seq = $2 AND state = 'running'`, f.JobID, f.ClaimSeq); err != nil {
					rt.Fatal(err)
				}
				over = append(over, f)
			},
			"reap": func(rt *rapid.T) {
				if _, err := store.Reap(ctx, env.Pool, rapid.IntRange(1, 5).Draw(rt, "limit")); err != nil {
					rt.Fatal(err)
				}
			},
			"stale write": func(rt *rapid.T) {
				f := take(rt, &over)
				over = append(over, f)
				var applied bool
				var err error
				switch rapid.IntRange(0, 2).Draw(rt, "write") {
				case 0:
					applied, err = store.Complete(ctx, env.Pool, f)
				case 1:
					var res store.FailResult
					res, err = store.Fail(ctx, env.Pool, f, "stale", 0)
					applied = res.Applied
				default:
					applied, err = store.Permanent(ctx, env.Pool, f, "stale")
				}
				if err != nil {
					rt.Fatal(err)
				}
				if applied {
					rt.Fatalf("a write with an ended claim (job %d, claim_seq %d) applied", f.JobID, f.ClaimSeq)
				}
			},
			"": func(rt *rapid.T) {
				rows, err := env.Pool.Query(ctx, `
					SELECT s.shop_id, s.backlog,
					       (SELECT count(*) FROM jobs j WHERE j.shop_id = s.shop_id AND j.state IN ('queued', 'running')),
					       s.shed_total
					  FROM shop_state s`)
				if err != nil {
					rt.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
					var shop string
					var counter, actual, shedTotal int
					if err := rows.Scan(&shop, &counter, &actual, &shedTotal); err != nil {
						rt.Fatal(err)
					}
					if counter != actual {
						rt.Fatalf("shop %s: backlog counter %d, real queued+running %d", shop, counter, actual)
					}
					if counter > limit {
						rt.Fatalf("shop %s: backlog %d is over the limit of %d", shop, counter, limit)
					}
					if shedTotal != shed[shop] {
						rt.Fatalf("shop %s: shed_total %d, but this test saw %d jobs rejected", shop, shedTotal, shed[shop])
					}
				}
				if err := rows.Err(); err != nil {
					rt.Fatal(err)
				}
			},
		})
	})
}
