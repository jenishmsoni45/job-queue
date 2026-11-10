package invariants

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// P2-2, Q3: many concurrent claimers run the real claim SQL against the same
// jobs, with no dispatcher, while jobs complete, fail and are requeued at once,
// and leases are abandoned and reaped. Every (job, claim_seq) has exactly one
// owner, and the leases of one job never overlap by the database clock.
func TestQ3AtMostOneLiveLeasePerJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, shortLeases(40, 10, 1000)))
	if _, err := env.Pool.Exec(t.Context(), queuetest.LeaseLogSQL); err != nil {
		t.Fatal(err)
	}

	rapid.Check(t, func(rt *rapid.T) {
		ctx := context.Background()
		if _, err := env.Pool.Exec(ctx, `TRUNCATE jobs, idempotency_keys, lease_log; UPDATE shop_state SET backlog = 0`); err != nil {
			rt.Fatal(err)
		}
		jobs := rapid.IntRange(1, 10).Draw(rt, "jobs")
		claimers := rapid.IntRange(2, 8).Draw(rt, "claimers")
		type step struct {
			batch   int
			actions []string // per claimed job: complete, fail or abandon
			pauseMS int
		}
		scripts := make([][]step, claimers)
		for c := range scripts {
			rounds := rapid.IntRange(1, 8).Draw(rt, fmt.Sprintf("rounds[%d]", c))
			for r := range rounds {
				batch := rapid.IntRange(1, 3).Draw(rt, fmt.Sprintf("batch[%d][%d]", c, r))
				actions := rapid.SliceOfN(rapid.SampledFrom([]string{"complete", "fail", "abandon"}), batch, batch).
					Draw(rt, fmt.Sprintf("actions[%d][%d]", c, r))
				pause := rapid.IntRange(0, 20).Draw(rt, fmt.Sprintf("pause[%d][%d]", c, r))
				scripts[c] = append(scripts[c], step{batch, actions, pause})
			}
		}
		for range jobs {
			if _, err := store.Enqueue(ctx, env.Pool, store.NewJob{Shop: shopA, Kind: testkinds.Sleep, Payload: []byte(`{}`)}); err != nil {
				rt.Fatal(err)
			}
		}

		var mu sync.Mutex
		claimedBy := map[[2]int64]string{}
		var errs []error
		fail := func(err error) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
		reapCtx, stopReaper := context.WithCancel(ctx)
		reaperDone := make(chan struct{})
		go func() {
			defer close(reaperDone)
			for reapCtx.Err() == nil {
				if _, err := store.Reap(reapCtx, env.Pool, 500); err != nil && reapCtx.Err() == nil {
					fail(fmt.Errorf("reap: %w", err))
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		start := make(chan struct{})
		var wg sync.WaitGroup
		for c, script := range scripts {
			wg.Go(func() {
				<-start
				for r, s := range script {
					owners := make([]string, s.batch)
					for k := range owners {
						owners[k] = fmt.Sprintf("claimer-%d/round-%d/%d", c, r, k)
					}
					got, err := store.Claim(ctx, env.Pool, owners)
					if err != nil {
						fail(fmt.Errorf("claim: %w", err))
						return
					}
					for k, job := range got {
						mu.Lock()
						key := [2]int64{job.ID, int64(job.ClaimSeq)}
						if prev, dup := claimedBy[key]; dup {
							errs = append(errs, fmt.Errorf("job %d claim_seq %d handed to %s and %s", job.ID, job.ClaimSeq, prev, job.Owner))
						}
						claimedBy[key] = job.Owner
						mu.Unlock()
						switch s.actions[k] {
						case "complete":
							_, err = store.Complete(ctx, env.Pool, job.Fence())
						case "fail":
							_, err = store.Fail(ctx, env.Pool, job.Fence(), "retry now", 0)
						}
						if err != nil {
							fail(err)
						}
					}
					time.Sleep(time.Duration(s.pauseMS) * time.Millisecond)
				}
			})
		}
		close(start)
		wg.Wait()
		stopReaper()
		<-reaperDone
		for _, err := range errs {
			rt.Error(err)
		}

		records, err := queuetest.ReadLeaseLog(ctx, env.Pool)
		if err != nil {
			rt.Fatal(err)
		}
		if len(records) != len(claimedBy) {
			rt.Errorf("lease log has %d leases, claimers received %d", len(records), len(claimedBy))
		}
		for _, rec := range records {
			if owner, ok := claimedBy[[2]int64{rec.JobID, int64(rec.Seq)}]; !ok || owner != rec.Owner {
				rt.Errorf("job %d claim_seq %d: logged owner %s, claimer saw %q", rec.JobID, rec.Seq, rec.Owner, owner)
			}
		}
		for _, err := range queuetest.LeaseLogErrors(records, false) {
			rt.Error(err)
		}
		var running int
		if err := env.Pool.QueryRow(ctx, `
			SELECT count(*) FROM jobs j JOIN lease_log l ON l.job_id = j.id AND l.claim_seq = j.claim_seq
			 WHERE j.state = 'running' AND (l.ended_at IS NOT NULL OR l.owner <> j.lease_owner)`).Scan(&running); err != nil {
			rt.Fatal(err)
		}
		if running != 0 {
			rt.Errorf("%d running jobs disagree with their current lease", running)
		}
	})
}
