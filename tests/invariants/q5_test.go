package invariants

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// P4-1, Q5 (NFR-5, AC-10.1) at the claim SQL: many concurrent claimers, with no
// dispatcher and so no in-memory hints at all, claim jobs for three shops with
// random caps. Some complete their jobs at once and some hold them, so caps are hit
// and freed. A shop's running jobs never pass its max_concurrency. This is checked
// two ways: a sampler reads every shop's running count as fast as it can while
// the claimers work, and afterwards the lease log, which records every lease with
// database times, is swept to find the most leases any shop held at once.
func TestQ5RunningJobsNeverPassTheCap(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, shortLeases(60_000, 10_000, 1000)))
	if _, err := env.Pool.Exec(t.Context(), queuetest.LeaseLogSQL); err != nil {
		t.Fatal(err)
	}
	shops := []string{shopA, shopB, shopC}

	rapid.Check(t, func(rt *rapid.T) {
		ctx := context.Background()
		caps := map[string]int{}
		for _, shop := range shops {
			caps[shop] = rapid.IntRange(1, 4).Draw(rt, "cap "+shop)
		}
		if _, err := env.Pool.Exec(ctx, `TRUNCATE jobs, idempotency_keys, lease_log; UPDATE shop_state SET backlog = 0`); err != nil {
			rt.Fatal(err)
		}
		for _, shop := range shops {
			if _, err := env.Pool.Exec(ctx, `UPDATE shops SET max_concurrency = $2 WHERE id = $1`, shop, caps[shop]); err != nil {
				rt.Fatal(err)
			}
			for range rapid.IntRange(0, 12).Draw(rt, "jobs "+shop) {
				if _, err := store.Enqueue(ctx, env.Pool, store.NewJob{Shop: shop, Kind: testkinds.Sleep, Payload: []byte(`{}`)}); err != nil {
					rt.Fatal(err)
				}
			}
		}

		type round struct {
			shop    string // "" claims across every shop, as FIFO does
			batch   int
			actions []string // per claimed job: complete or hold
		}
		claimers := rapid.IntRange(2, 8).Draw(rt, "claimers")
		scripts := make([][]round, claimers)
		for c := range scripts {
			for r := range rapid.IntRange(1, 8).Draw(rt, fmt.Sprintf("rounds[%d]", c)) {
				batch := rapid.IntRange(1, 5).Draw(rt, fmt.Sprintf("batch[%d][%d]", c, r))
				scripts[c] = append(scripts[c], round{
					shop:  rapid.SampledFrom([]string{"", shopA, shopB, shopC}).Draw(rt, fmt.Sprintf("shop[%d][%d]", c, r)),
					batch: batch,
					actions: rapid.SliceOfN(rapid.SampledFrom([]string{"complete", "hold"}), batch, batch).
						Draw(rt, fmt.Sprintf("actions[%d][%d]", c, r)),
				})
			}
		}

		var mu sync.Mutex
		var errs []error
		fail := func(err error) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}

		// The sampler reads every shop's running count as fast as it can.
		samplerCtx, stopSampler := context.WithCancel(ctx)
		samplerDone := make(chan struct{})
		go func() {
			defer close(samplerDone)
			for samplerCtx.Err() == nil {
				rows, err := env.Pool.Query(samplerCtx, `SELECT shop_id, count(*) FROM jobs WHERE state = 'running' GROUP BY shop_id`)
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
					spec := store.ClaimSpec{}
					if s.shop != "" {
						spec.Shops = []string{s.shop}
					}
					out, err := store.ClaimFor(ctx, env.Pool, owners, spec)
					if err != nil {
						fail(fmt.Errorf("claim: %w", err))
						return
					}
					for k, job := range out.Jobs {
						if s.actions[k] == "complete" {
							if _, err := store.Complete(ctx, env.Pool, job.Fence()); err != nil {
								fail(err)
							}
						}
					}
				}
			})
		}
		close(start)
		wg.Wait()
		stopSampler()
		<-samplerDone
		for _, err := range errs {
			rt.Error(err)
		}

		// The sweep: the most leases any shop held at once, by the database clock.
		peak, err := peakLeases(ctx, env)
		if err != nil {
			rt.Fatal(err)
		}
		for _, shop := range shops {
			if peak[shop] > caps[shop] {
				rt.Errorf("shop %s held %d leases at once, over its cap of %d", shop, peak[shop], caps[shop])
			}
		}
		var over int
		if err := env.Pool.QueryRow(ctx, `
			SELECT count(*) FROM (SELECT j.shop_id FROM jobs j JOIN shops s ON s.id = j.shop_id
			                       WHERE j.state = 'running' GROUP BY j.shop_id, s.max_concurrency
			                      HAVING count(*) > s.max_concurrency) x`).Scan(&over); err != nil {
			rt.Fatal(err)
		}
		if over != 0 {
			rt.Errorf("%d shops end with more running jobs than their cap", over)
		}
		env.CheckBacklog(t)
	})
}

// peakLeases sweeps the lease log: for each shop, the largest number of leases
// live at the same time. A lease is live from its start until it ends; a lease
// that never ended is live to the end. When one lease ends at the instant another
// starts, the end counts first.
func peakLeases(ctx context.Context, env *queuetest.Env) (map[string]int, error) {
	rows, err := env.Pool.Query(ctx, `
		SELECT j.shop_id, l.started_at, l.ended_at FROM lease_log l JOIN jobs j ON j.id = l.job_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type event struct {
		at    time.Time
		delta int
	}
	events := map[string][]event{}
	for rows.Next() {
		var shop string
		var started time.Time
		var ended *time.Time
		if err := rows.Scan(&shop, &started, &ended); err != nil {
			return nil, err
		}
		events[shop] = append(events[shop], event{started, +1})
		if ended != nil {
			events[shop] = append(events[shop], event{*ended, -1})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	peak := map[string]int{}
	for shop, list := range events {
		sort.Slice(list, func(i, j int) bool {
			if !list[i].at.Equal(list[j].at) {
				return list[i].at.Before(list[j].at)
			}
			return list[i].delta < list[j].delta // ends before starts
		})
		live := 0
		for _, e := range list {
			live += e.delta
			peak[shop] = max(peak[shop], live)
		}
	}
	return peak, nil
}
