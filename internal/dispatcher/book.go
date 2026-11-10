package dispatcher

import (
	"time"

	"github.com/jenishmsoni45/job-queue/internal/scheduler"
	"github.com/jenishmsoni45/job-queue/internal/store"
)

// book is the dispatcher's in-memory view of every shop (design §6.1): its
// weight, cap, token bucket and running count. It turns a scheduler's offers
// into claims, and the claims' results back into scheduler state. It does no I/O,
// and one goroutine, the claim loop, owns it.
//
// The book only hints at what a claim can do. The cap, the throttle and the
// claim's own limits are enforced again inside the claim transaction, which is
// the authority (design §6.3). But the hints are kept conservative: the running
// count starts at what the last refresh read and goes up with every claim made
// since, and a job that finishes meanwhile only lowers the true count. So a book
// that is following its own limits never claims past a cap, even with the claim
// transaction's cap step switched off.
type book struct {
	now   func() time.Time
	sched scheduler.Scheduler
	shops map[string]*shopInfo
}

type shopInfo struct {
	weight    int
	maxConc   int
	running   int // at the last refresh, plus claimed since
	runnable  bool
	throttled bool
	bucket    *bucket
}

func newBook(now func() time.Time, sched scheduler.Scheduler) *book {
	return &book{now: now, sched: sched, shops: map[string]*shopInfo{}}
}

// limit is the most jobs the shop could claim now: room under its cap, and
// the whole tokens in its bucket. A throttled shop has none.
func (b *book) limit(id string) int {
	s, ok := b.shops[id]
	if !ok || s.throttled {
		return 0
	}
	return max(0, min(s.maxConc-s.running, s.bucket.whole(b.now())))
}

// refresh replaces the book's view with a fresh snapshot, and tells the
// scheduler which shops are runnable and which are eligible (design §6.1). Token
// buckets carry over, so a refresh never refills one.
func (b *book) refresh(snaps []store.ShopSnapshot) {
	now := b.now()
	seen := make(map[string]bool, len(snaps))
	shops := make([]scheduler.Shop, 0, len(snaps))
	for _, snap := range snaps {
		s, ok := b.shops[snap.ID]
		if !ok {
			s = &shopInfo{bucket: newBucket(snap.RatePerSec, snap.RateBurst, now)}
			b.shops[snap.ID] = s
		}
		s.bucket.configure(snap.RatePerSec, snap.RateBurst, now)
		s.weight, s.maxConc, s.running = snap.Weight, snap.MaxConcurrency, snap.Running
		s.runnable, s.throttled = snap.Runnable, snap.Throttled
		seen[snap.ID] = true
		shops = append(shops, scheduler.Shop{
			ID: snap.ID, Weight: snap.Weight, Runnable: snap.Runnable,
			Eligible: snap.Runnable && b.limit(snap.ID) > 0,
		})
	}
	for id := range b.shops {
		if !seen[id] {
			delete(b.shops, id)
		}
	}
	b.sched.Update(shops)
}

// plan is one claim to make: how many of the waiting workers it is for, and
// what it may take.
type plan struct {
	offer   scheduler.Offer
	workers int
	spec    store.ClaimSpec
	// trimmed means workers is fewer than were waiting, because the shop's room or
	// tokens run out first: a claim that fills them all has reached a limit.
	trimmed bool
}

// next returns the next claim for up to waiting workers, or false if no shop can
// claim now.
func (b *book) next(waiting int) (plan, bool) {
	for {
		offer, ok := b.sched.Next()
		if !ok {
			return plan{}, false
		}
		limits := make(map[string]int, len(offer.Shops))
		shops := make([]string, 0, len(offer.Shops))
		for _, id := range offer.Shops {
			if l := b.limit(id); l > 0 {
				limits[id] = l
				shops = append(shops, id)
			}
		}
		if len(shops) == 0 {
			// The scheduler thought a shop was eligible, and the book has since
			// spent its room or its tokens. Say so, and ask again.
			b.sched.Done(offer, scheduler.Result{End: scheduler.Blocked})
			continue
		}
		p := plan{
			offer:   scheduler.Offer{Shops: shops, Allowance: offer.Allowance},
			workers: waiting,
			spec:    store.ClaimSpec{Shops: shops, Limits: limits},
		}
		if offer.Allowance != scheduler.NoAllowance {
			p.spec.MaxCost = offer.Allowance
		}
		if len(shops) == 1 && offer.Allowance != scheduler.NoAllowance && limits[shops[0]] < waiting {
			p.workers, p.trimmed = limits[shops[0]], true
		}
		return p, true
	}
}

// done records what a claim made under p did: it spends the tokens and counts the
// jobs as running, and tells the scheduler how the shop's turn ended.
func (b *book) done(p plan, out store.ClaimOutcome) {
	now := b.now()
	for _, j := range out.Jobs {
		if s, ok := b.shops[j.Shop]; ok {
			s.running++
			s.bucket.spend(1, now)
		}
	}
	res := scheduler.Result{Served: out.Cost}
	switch out.End {
	case store.ClaimFull:
		res.End = scheduler.Continue
		// A claim that took every job it was for, but was for fewer workers than
		// were waiting, or left the shop with no room, has run into a limit.
		if p.trimmed || (len(p.offer.Shops) == 1 && b.limit(p.offer.Shops[0]) == 0) {
			res.End = scheduler.Blocked
		}
	case store.ClaimCost:
		res.End = scheduler.Spent
	case store.ClaimBlocked:
		res.End = scheduler.Blocked
	case store.ClaimDry:
		res.End = scheduler.Empty
	}
	b.sched.Done(p.offer, res)
}
