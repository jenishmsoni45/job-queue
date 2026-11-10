package invariants

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// P2-9: after any sequence of enqueues, claims, results (complete, fail,
// permanent), expiries, reaper passes and stale writes, each shop's backlog
// counter equals its real number of queued and running jobs (design §3).
func TestBacklogCounterMatchesRealCount(t *testing.T) {
	t.Parallel()
	// Long leases: the test expires leases itself, by moving lease_expires_at
	// to the database's past. Three attempts, so jobs also die by failures and
	// by the reaper.
	env := queuetest.New(t, queuetest.Config(t, shortLeases(60_000, 10_000, 3)))
	shops := []string{shopA, shopB, shopC}

	rapid.Check(t, func(rt *rapid.T) {
		ctx := context.Background()
		if _, err := env.Pool.Exec(ctx, `TRUNCATE jobs, idempotency_keys; UPDATE shop_state SET backlog = 0`); err != nil {
			rt.Fatal(err)
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
				if _, err := store.Enqueue(ctx, env.Pool, job); err != nil {
					rt.Fatal(err)
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
					       (SELECT count(*) FROM jobs j WHERE j.shop_id = s.shop_id AND j.state IN ('queued', 'running'))
					  FROM shop_state s`)
				if err != nil {
					rt.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
					var shop string
					var counter, actual int
					if err := rows.Scan(&shop, &counter, &actual); err != nil {
						rt.Fatal(err)
					}
					if counter != actual {
						rt.Fatalf("shop %s: backlog counter %d, real queued+running %d", shop, counter, actual)
					}
				}
				if err := rows.Err(); err != nil {
					rt.Fatal(err)
				}
			},
		})
	})
}
