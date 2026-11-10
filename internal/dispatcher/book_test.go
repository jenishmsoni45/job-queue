package dispatcher

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/scheduler"
	"github.com/jenishmsoni45/job-queue/internal/store"
)

// model is a stand-in for Postgres, and the truth the book is checked against.
// Its claim does everything the claim transaction does except the cap step: it
// takes the jobs of the offered shops oldest first, within the spec's allowance
// and per-shop limits. So the only thing between a shop and its cap is what the
// book put into the limits.
type model struct {
	clock   time.Time
	maxConc map[string]int
	running map[string]int // true running jobs
	queued  map[string]int // runnable jobs
	claimed map[string]int // every job ever claimed
	ids     []string       // shops, in order
}

func (m *model) now() time.Time { return m.clock }

func (m *model) snapshots(rates map[string]float64, bursts map[string]int, weights map[string]int) []store.ShopSnapshot {
	snaps := make([]store.ShopSnapshot, 0, len(m.ids))
	for _, id := range m.ids {
		snaps = append(snaps, store.ShopSnapshot{
			ID: id, Weight: weights[id], MaxConcurrency: m.maxConc[id], RatePerSec: rates[id], RateBurst: bursts[id],
			Running: m.running[id], Runnable: m.queued[id] > 0,
		})
	}
	return snaps
}

// claim mimics store.ClaimFor without its cap step, for unit-cost jobs.
func (m *model) claim(p plan) store.ClaimOutcome {
	var out store.ClaimOutcome
	left := map[string]int{}
	for _, id := range p.spec.Shops {
		left[id] = m.queued[id]
	}
	blocked, overCost := false, false
	taken := map[string]int{}
	for out.End = store.ClaimFull; len(out.Jobs) < p.workers; {
		// The next oldest job: the first listed shop that still has one. (Which
		// shop is oldest doesn't matter to the cap.)
		var shop string
		for _, id := range p.spec.Shops {
			if left[id] > 0 {
				shop = id
				break
			}
		}
		if shop == "" {
			break
		}
		if limit, ok := p.spec.Limits[shop]; ok && taken[shop] >= limit {
			blocked = true
			left[shop] = 0
			continue
		}
		if p.spec.MaxCost > 0 && out.Cost+1 > p.spec.MaxCost {
			overCost = true
			break
		}
		left[shop]--
		taken[shop]++
		out.Cost++
		out.Jobs = append(out.Jobs, store.Claimed{Shop: shop})
	}
	switch {
	case overCost:
		out.End = store.ClaimCost
	case blocked:
		out.End = store.ClaimBlocked
	case len(out.Jobs) < p.workers:
		out.End = store.ClaimDry
	default:
		out.End = store.ClaimFull
	}
	for _, j := range out.Jobs {
		m.queued[j.Shop]--
		m.running[j.Shop]++
		m.claimed[j.Shop]++
	}
	return out
}

// round makes claims for waiting workers the way Dispatcher.serve does.
func (m *model) round(bk *book, waiting int, check func()) {
	for range waiting + len(m.ids)*2 + maxClaimRoundsPerWake {
		if waiting == 0 {
			return
		}
		p, ok := bk.next(waiting)
		if !ok {
			return
		}
		out := m.claim(p)
		waiting -= len(out.Jobs)
		bk.done(p, out)
		check()
	}
}

type world struct {
	m       *model
	bk      *book
	rates   map[string]float64
	bursts  map[string]int
	weights map[string]int
	started time.Time
}

// drawWorld draws shops with random caps, weights and bucket settings, a random
// scheduler, and a model that starts at a random time.
func drawWorld(rt *rapid.T, hugeBuckets, hugeCaps bool) *world {
	n := rapid.IntRange(1, 5).Draw(rt, "shops")
	mode := rapid.SampledFrom([]scheduler.Mode{scheduler.ModeDRR, scheduler.ModeFIFO}).Draw(rt, "scheduler")
	sched, err := scheduler.New(mode, rapid.Int64Range(1, 4).Draw(rt, "quantum"))
	if err != nil {
		rt.Fatal(err)
	}
	w := &world{
		m: &model{
			clock:   time.Unix(1_000_000, 0),
			maxConc: map[string]int{}, running: map[string]int{}, queued: map[string]int{}, claimed: map[string]int{},
		},
		rates: map[string]float64{}, bursts: map[string]int{}, weights: map[string]int{},
	}
	for i := range n {
		id := fmt.Sprintf("shop-%d", i)
		w.m.ids = append(w.m.ids, id)
		w.m.maxConc[id] = rapid.IntRange(1, 6).Draw(rt, "cap "+id)
		if hugeCaps {
			w.m.maxConc[id] = math.MaxInt32
		}
		w.weights[id] = rapid.IntRange(1, 4).Draw(rt, "weight "+id)
		w.rates[id] = rapid.Float64Range(0.5, 40).Draw(rt, "rate "+id)
		w.bursts[id] = rapid.IntRange(1, 10).Draw(rt, "burst "+id)
		if hugeBuckets {
			w.rates[id], w.bursts[id] = 1e9, math.MaxInt32
		}
	}
	sort.Strings(w.m.ids)
	w.bk = newBook(w.m.now, sched)
	w.started = w.m.clock
	w.bk.refresh(w.m.snapshots(w.rates, w.bursts, w.weights))
	return w
}

func (w *world) refresh() { w.bk.refresh(w.m.snapshots(w.rates, w.bursts, w.weights)) }

// P4-1, Q5 (NFR-5, AC-10.1): a book that follows its own limits never lets a shop's
// running jobs pass its max_concurrency, whatever the mix of enqueues, refreshes,
// claim rounds, completions and passing time. The model's claim has no cap step of
// its own, so this is the book's hint on its own: the running count it keeps
// between refreshes is never below the truth, since a job that finishes only
// lowers the truth. (The claim transaction's cap step, which makes Q5 hold even
// with two dispatchers, is tested against Postgres.)
func TestQ5BookNeverClaimsPastTheCap(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		w := drawWorld(rt, true, false)
		check := func() {
			for _, id := range w.m.ids {
				if w.m.running[id] > w.m.maxConc[id] {
					rt.Fatalf("shop %s has %d running jobs, over its max_concurrency of %d", id, w.m.running[id], w.m.maxConc[id])
				}
			}
		}
		rt.Repeat(map[string]func(*rapid.T){
			"enqueue": func(rt *rapid.T) {
				w.m.queued[rapid.SampledFrom(w.m.ids).Draw(rt, "shop")] += rapid.IntRange(1, 12).Draw(rt, "jobs")
			},
			"complete": func(rt *rapid.T) {
				id := rapid.SampledFrom(w.m.ids).Draw(rt, "shop")
				if w.m.running[id] == 0 {
					rt.Skip("nothing running")
				}
				w.m.running[id] -= rapid.IntRange(1, w.m.running[id]).Draw(rt, "finished")
			},
			"refresh": func(rt *rapid.T) { w.refresh() },
			"round":   func(rt *rapid.T) { w.m.round(w.bk, rapid.IntRange(1, 10).Draw(rt, "waiting workers"), check) },
			"": func(rt *rapid.T) { // every action ends by checking the invariant
				check()
			},
		})
	})
}

// P4-6, AC-10.2: claims for a shop never pass its bucket: rate × elapsed + burst,
// however claims, refreshes and the passing of time interleave.
func TestClaimsNeverPassRateTimesElapsedPlusBurst(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		w := drawWorld(rt, false, true)
		check := func() {
			elapsed := w.m.clock.Sub(w.started).Seconds()
			for _, id := range w.m.ids {
				if bound := w.rates[id]*elapsed + float64(w.bursts[id]); float64(w.m.claimed[id]) > bound+1e-6 {
					rt.Fatalf("shop %s claimed %d jobs in %.3f s, over rate %.3f × elapsed + burst %d = %.3f",
						id, w.m.claimed[id], elapsed, w.rates[id], w.bursts[id], bound)
				}
			}
		}
		rt.Repeat(map[string]func(*rapid.T){
			"enqueue": func(rt *rapid.T) {
				w.m.queued[rapid.SampledFrom(w.m.ids).Draw(rt, "shop")] += rapid.IntRange(1, 30).Draw(rt, "jobs")
			},
			"wait": func(rt *rapid.T) {
				w.m.clock = w.m.clock.Add(time.Duration(rapid.IntRange(0, 3000).Draw(rt, "ms")) * time.Millisecond)
			},
			"complete": func(rt *rapid.T) {
				for _, id := range w.m.ids {
					w.m.running[id] = 0
				}
			},
			"refresh": func(rt *rapid.T) { w.refresh() },
			"round":   func(rt *rapid.T) { w.m.round(w.bk, rapid.IntRange(1, 12).Draw(rt, "waiting workers"), check) },
			"": func(rt *rapid.T) {
				check()
			},
		})
	})
}

func TestBucketRefillsWithTimeAndNeverPassesBurst(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(0, 0)
	b := newBucket(10, 5, t0)
	if got := b.whole(t0); got != 5 {
		t.Fatalf("a new bucket holds %d tokens, want its burst of 5", got)
	}
	b.spend(5, t0)
	if got := b.whole(t0); got != 0 {
		t.Fatalf("after spending all, %d tokens", got)
	}
	if got := b.whole(t0.Add(250 * time.Millisecond)); got != 2 {
		t.Fatalf("after 250 ms at 10/s, %d tokens, want 2", got)
	}
	if got := b.whole(t0.Add(time.Hour)); got != 5 {
		t.Fatalf("after an hour, %d tokens, want the burst of 5", got)
	}
	b.spend(100, t0.Add(time.Hour)) // never below zero
	if got := b.whole(t0.Add(time.Hour)); got != 0 {
		t.Fatalf("overspent bucket holds %d tokens", got)
	}
	// A clock that doesn't move forward neither refills nor drains.
	b.configure(10, 2, t0.Add(time.Hour))
	if got := b.whole(t0); got != 0 {
		t.Fatalf("a bucket read at an earlier time holds %d tokens, want 0", got)
	}
	b.configure(10, 2, t0.Add(2*time.Hour))
	if got := b.whole(t0.Add(2 * time.Hour)); got != 2 {
		t.Fatalf("after a smaller burst was configured, %d tokens, want 2", got)
	}
}

func TestBookTreatsAThrottledShopAsOutOfRoom(t *testing.T) {
	t.Parallel()
	sched, err := scheduler.NewDRR(1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	bk := newBook(func() time.Time { return now }, sched)
	snap := store.ShopSnapshot{ID: "a", Weight: 1, MaxConcurrency: 5, RatePerSec: 10, RateBurst: 10, Runnable: true, Throttled: true}
	bk.refresh([]store.ShopSnapshot{snap})
	if _, ok := bk.next(3); ok {
		t.Fatal("the book offered a throttled shop")
	}
	snap.Throttled = false
	bk.refresh([]store.ShopSnapshot{snap})
	p, ok := bk.next(3)
	if !ok || p.workers != 3 || p.spec.Limits["a"] != 5 {
		t.Fatalf("plan = %+v, %v; want 3 workers and a limit of 5 (the shop's cap)", p, ok)
	}
}
