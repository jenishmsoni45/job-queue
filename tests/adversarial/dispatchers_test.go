package adversarial

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/dispatcher"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/scheduler"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// startLeader runs a dispatcher that leads alongside any other (design §14:
// "two dispatchers at once").
func startLeader(t *testing.T, env *queuetest.Env, name string, mode scheduler.Mode) *dispatcher.Client {
	t.Helper()
	return env.StartDispatcher(t, name, mode)
}

// P4-1, Q5 and Q3 with two dispatchers (design §14, §16): both dispatchers lead
// at once, each with its own scheduler and its own view of the shops, and
// twelve workers poll them both while three shops with small caps run through
// 180 jobs. Neither dispatcher knows what the other has claimed, so only the claim
// transaction's cap step stands between a shop and its cap. A sampler reads the
// running counts as fast as it can: none ever passes its shop's cap. No job
// has two overlapping leases, and every job completes once.
func TestTwoDispatchersNeverPassACap(t *testing.T) {
	t.Parallel()
	for _, mode := range []scheduler.Mode{scheduler.ModeDRR, scheduler.ModeFIFO} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			caps := map[string]int{queuetest.ShopA: 2, queuetest.ShopB: 3, queuetest.ShopC: 1}
			env := queuetest.New(t, queuetest.Config(t, func(c *config.File) {
				for i := range c.Shops {
					if cap, ok := caps[c.Shops[i].ID]; ok {
						c.Shops[i].MaxConcurrency = cap
					}
				}
				for i := range c.Kinds {
					c.Kinds[i].LeaseMS, c.Kinds[i].HeartbeatMS = 60_000, 10_000
				}
			}))
			if _, err := env.Pool.Exec(t.Context(), queuetest.LeaseLogSQL); err != nil {
				t.Fatal(err)
			}
			const perShop = 60
			for shop := range caps {
				for range perShop {
					env.Enqueue(t, shop, testkinds.Sleep, nil)
				}
			}
			total := perShop * len(caps)
			clients := []*dispatcher.Client{startLeader(t, env, "one", mode), startLeader(t, env, "two", mode)}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var mu sync.Mutex
			var errs []error
			fail := func(err error) {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}

			// The sampler.
			var samples atomic.Int64
			var samplerWG sync.WaitGroup
			samplerWG.Go(func() {
				for ctx.Err() == nil {
					rows, err := env.Pool.Query(ctx, `SELECT shop_id, count(*) FROM jobs WHERE state = 'running' GROUP BY shop_id`)
					if err != nil {
						return
					}
					for rows.Next() {
						var shop string
						var n int
						if rows.Scan(&shop, &n) == nil && n > caps[shop] {
							fail(fmt.Errorf("sampled %d running jobs for %s, over its cap of %d", n, shop, caps[shop]))
						}
					}
					rows.Close()
					samples.Add(1)
				}
			})

			// The workers: each polls one dispatcher, holds its job briefly, and completes it.
			claimsVia := make([]atomic.Int64, len(clients))
			var workers sync.WaitGroup
			for w := range 12 {
				workers.Go(func() {
					via := w % len(clients)
					for ctx.Err() == nil {
						job, err := clients[via].Claim(ctx, fmt.Sprintf("worker-%d", w))
						if err != nil || job == nil {
							continue
						}
						claimsVia[via].Add(1)
						time.Sleep(time.Duration(1+w%5) * time.Millisecond)
						if ok, err := store.Complete(ctx, env.Pool, job.Fence()); err != nil && ctx.Err() == nil {
							fail(err)
						} else if !ok && ctx.Err() == nil {
							fail(fmt.Errorf("job %d: the fenced completion of a live lease affected 0 rows", job.ID))
						}
					}
				})
			}

			queuetest.Eventually(t, 60*time.Second, "every job to complete", func() bool {
				var done int
				if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM jobs WHERE state = 'completed'`).Scan(&done); err != nil {
					t.Fatal(err)
				}
				return done == total
			})
			cancel()
			workers.Wait()
			samplerWG.Wait()

			mu.Lock()
			defer mu.Unlock()
			for _, err := range errs {
				t.Error(err)
			}
			if samples.Load() < 20 {
				t.Errorf("the sampler read the running counts only %d times", samples.Load())
			}
			if claimsVia[0].Load() == 0 || claimsVia[1].Load() == 0 {
				t.Errorf("claims via dispatcher one: %d, via two: %d; both must have served workers for this to be a split brain",
					claimsVia[0].Load(), claimsVia[1].Load())
			}
			records, err := queuetest.ReadLeaseLog(t.Context(), env.Pool)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != total {
				t.Errorf("%d leases for %d jobs: a job was claimed more than once", len(records), total)
			}
			for _, err := range queuetest.LeaseLogErrors(records, true) {
				t.Error(err)
			}
			env.CheckBacklog(t)
		})
	}
}
