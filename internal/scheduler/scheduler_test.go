package scheduler_test

import (
	"fmt"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/scheduler"
)

func shop(id string, weight int, runnable, eligible bool) scheduler.Shop {
	return scheduler.Shop{ID: id, Weight: weight, Runnable: runnable, Eligible: eligible}
}

// fataler is what newDRR needs from a *testing.T or a *rapid.T.
type fataler interface {
	Helper()
	Fatal(args ...any)
}

func newDRR(t fataler, quantum int64) *scheduler.DRR {
	t.Helper()
	d, err := scheduler.NewDRR(quantum)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustNext(t testing.TB, s scheduler.Scheduler) scheduler.Offer {
	t.Helper()
	o, ok := s.Next()
	if !ok {
		t.Fatal("Next offered nothing, want an offer")
	}
	return o
}

func deficit(t testing.TB, d *scheduler.DRR, id string) int64 {
	t.Helper()
	v, ok := d.Deficit(id)
	if !ok {
		t.Fatalf("shop %s is not in the list", id)
	}
	return v
}

// The scheduler's names and its quantum are checked.
func TestParseModeAndNew(t *testing.T) {
	t.Parallel()
	for _, m := range []scheduler.Mode{scheduler.ModeDRR, scheduler.ModeFIFO} {
		got, err := scheduler.ParseMode(string(m))
		if err != nil || got != m {
			t.Errorf("ParseMode(%q) = %q, %v", m, got, err)
		}
		if _, err := scheduler.New(m, 1); err != nil {
			t.Errorf("New(%q): %v", m, err)
		}
	}
	if _, err := scheduler.ParseMode("lifo"); err == nil {
		t.Error("ParseMode accepted an unknown scheduler")
	}
	if _, err := scheduler.New("lifo", 1); err == nil {
		t.Error("New accepted an unknown mode")
	}
	if _, err := scheduler.NewDRR(0); err == nil {
		t.Error("NewDRR accepted a quantum of 0")
	}
}

// A turn gives the shop weight × quantum, offers all of it as the allowance, and
// goes on across claims that stopped only for want of workers.
func TestDRRTurnGrantsOneQuantumAndContinuesAcrossClaims(t *testing.T) {
	t.Parallel()
	d := newDRR(t, 3)
	d.Update([]scheduler.Shop{shop("a", 2, true, true), shop("b", 1, true, true)})

	o := mustNext(t, d)
	if o.Shops[0] != "a" || o.Allowance != 6 {
		t.Fatalf("first offer = %+v, want shop a with allowance 2 × 3", o)
	}
	d.Done(o, scheduler.Result{Served: 2, End: scheduler.Continue})

	o = mustNext(t, d)
	if o.Shops[0] != "a" || o.Allowance != 4 {
		t.Fatalf("second offer = %+v, want shop a again with the 4 units left and no new quantum", o)
	}
	d.Done(o, scheduler.Result{Served: 4, End: scheduler.Continue})

	// Nothing is left, so the turn is over: b is next.
	o = mustNext(t, d)
	if o.Shops[0] != "b" || o.Allowance != 3 {
		t.Fatalf("third offer = %+v, want shop b with allowance 3", o)
	}
}

// A turn that ends because the next job doesn't fit keeps the rest of the
// deficit, which the shop's next turn adds to.
func TestDRRSpentTurnKeepsTheRemainderForNextTime(t *testing.T) {
	t.Parallel()
	d := newDRR(t, 5)
	d.Update([]scheduler.Shop{shop("a", 1, true, true), shop("b", 1, true, true)})

	o := mustNext(t, d)
	d.Done(o, scheduler.Result{Served: 4, End: scheduler.Spent})
	if got := deficit(t, d, "a"); got != 1 {
		t.Fatalf("a's deficit after the turn = %d, want 1", got)
	}
	o = mustNext(t, d)
	if o.Shops[0] != "b" {
		t.Fatalf("offer = %+v, want b's turn", o)
	}
	d.Done(o, scheduler.Result{Served: 5, End: scheduler.Spent})
	o = mustNext(t, d)
	if o.Shops[0] != "a" || o.Allowance != 6 {
		t.Fatalf("offer = %+v, want a with the 1 left over plus a new quantum of 5", o)
	}
}

// P4-9: a shop that empties leaves the list with its deficit reset, and starts
// from nothing when it comes back.
func TestDRRShopThatEmptiesLeavesTheListWithItsDeficitReset(t *testing.T) {
	t.Parallel()
	for _, how := range []string{"a claim found it empty", "a refresh showed it not runnable", "a refresh left it out"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			d := newDRR(t, 5)
			both := []scheduler.Shop{shop("a", 1, true, true), shop("b", 1, true, true)}
			d.Update(both)
			o := mustNext(t, d)
			d.Done(o, scheduler.Result{Served: 1, End: scheduler.Spent}) // a keeps 4

			switch how {
			case "a claim found it empty":
				// b's turn, then a's again, and the claim finds a empty mid-turn.
				d.Done(mustNext(t, d), scheduler.Result{Served: 5, End: scheduler.Spent})
				o = mustNext(t, d)
				d.Done(o, scheduler.Result{Served: 0, End: scheduler.Empty})
			case "a refresh showed it not runnable":
				d.Update([]scheduler.Shop{shop("a", 1, false, false), shop("b", 1, true, true)})
			default:
				d.Update([]scheduler.Shop{shop("b", 1, true, true)})
			}
			if _, listed := d.Deficit("a"); listed {
				t.Fatal("a is still in the list")
			}

			d.Update(both) // a has jobs again
			if got := deficit(t, d, "a"); got != 0 {
				t.Fatalf("a came back with deficit %d, want 0", got)
			}
			for {
				o := mustNext(t, d)
				if o.Shops[0] == "a" {
					if o.Allowance != 5 {
						t.Fatalf("a's first turn back has allowance %d, want exactly one quantum (5)", o.Allowance)
					}
					return
				}
				d.Done(o, scheduler.Result{Served: o.Allowance, End: scheduler.Spent})
			}
		})
	}
}

// P4-9: a shop that has jobs but can't claim is skipped without a quantum, and
// keeps its deficit until it is eligible again.
func TestDRRIneligibleShopIsSkippedWithoutQuantumAndKeepsItsDeficit(t *testing.T) {
	t.Parallel()
	d := newDRR(t, 5)
	d.Update([]scheduler.Shop{shop("a", 1, true, true), shop("b", 1, true, true)})
	d.Done(mustNext(t, d), scheduler.Result{Served: 2, End: scheduler.Spent}) // a keeps 3
	d.Update([]scheduler.Shop{shop("a", 1, true, false), shop("b", 1, true, true)})

	for range 4 {
		o := mustNext(t, d)
		if o.Shops[0] != "b" {
			t.Fatalf("offer = %+v, want b: a is ineligible", o)
		}
		d.Done(o, scheduler.Result{Served: o.Allowance, End: scheduler.Spent})
	}
	if got := deficit(t, d, "a"); got != 3 {
		t.Fatalf("a's deficit = %d after being skipped four times, want the 3 it had", got)
	}

	d.Update([]scheduler.Shop{shop("a", 1, true, true), shop("b", 1, true, true)})
	for {
		o := mustNext(t, d)
		if o.Shops[0] == "a" {
			if o.Allowance != 8 {
				t.Fatalf("a's turn after it became eligible has allowance %d, want 3 kept + 5 quantum", o.Allowance)
			}
			return
		}
		d.Done(o, scheduler.Result{Served: o.Allowance, End: scheduler.Spent})
	}
}

func TestDRROffersNothingWhenNoShopIsEligible(t *testing.T) {
	t.Parallel()
	d := newDRR(t, 1)
	if _, ok := d.Next(); ok {
		t.Fatal("an empty scheduler made an offer")
	}
	d.Update([]scheduler.Shop{shop("a", 1, true, false), shop("b", 1, false, false)})
	if o, ok := d.Next(); ok {
		t.Fatalf("offered %+v with no eligible shop", o)
	}
}

// A claim that a cap, a token or a throttle stopped marks the shop ineligible
// until the next refresh, so the dispatcher doesn't repeat it.
func TestDRRBlockedShopIsSkippedUntilTheNextUpdate(t *testing.T) {
	t.Parallel()
	d := newDRR(t, 2)
	shops := []scheduler.Shop{shop("a", 1, true, true), shop("b", 1, true, true)}
	d.Update(shops)
	d.Done(mustNext(t, d), scheduler.Result{Served: 1, End: scheduler.Blocked})
	for range 3 {
		o := mustNext(t, d)
		if o.Shops[0] != "b" {
			t.Fatalf("offer = %+v, want b: a is blocked", o)
		}
		d.Done(o, scheduler.Result{Served: o.Allowance, End: scheduler.Spent})
	}
	if got := deficit(t, d, "a"); got != 1 {
		t.Fatalf("a's deficit = %d, want the 1 left after its blocked claim", got)
	}
	d.Update(shops)
	seen := false
	for range 2 {
		o := mustNext(t, d)
		seen = seen || o.Shops[0] == "a"
		d.Done(o, scheduler.Result{Served: o.Allowance, End: scheduler.Spent})
	}
	if !seen {
		t.Fatal("a was not offered after the next Update")
	}
}

// A refresh in the middle of a turn doesn't end the turn or grant a second quantum.
func TestDRRUpdateKeepsAnOpenTurn(t *testing.T) {
	t.Parallel()
	d := newDRR(t, 4)
	shops := []scheduler.Shop{shop("a", 1, true, true), shop("b", 1, true, true), shop("c", 1, true, true)}
	d.Update(shops)
	o := mustNext(t, d)
	d.Done(o, scheduler.Result{Served: 1, End: scheduler.Continue})

	d.Update(shops)
	o = mustNext(t, d)
	if o.Shops[0] != "a" || o.Allowance != 3 {
		t.Fatalf("offer after a refresh = %+v, want a's open turn with 3 left", o)
	}

	// If the shop whose turn it is leaves, the turn passes to the one after it.
	d.Update([]scheduler.Shop{shop("a", 1, false, false), shop("b", 1, true, true), shop("c", 1, true, true)})
	if o := mustNext(t, d); o.Shops[0] != "b" || o.Allowance != 4 {
		t.Fatalf("offer = %+v, want b's new turn with one quantum", o)
	}
}

func TestDRRIgnoresAResultForAShopThatLeft(t *testing.T) {
	t.Parallel()
	d := newDRR(t, 2)
	d.Update([]scheduler.Shop{shop("a", 1, true, true)})
	o := mustNext(t, d)
	d.Update(nil)
	d.Done(o, scheduler.Result{Served: 1, End: scheduler.Spent}) // must not panic
	if _, ok := d.Next(); ok {
		t.Fatal("offered a shop that left")
	}
}

func TestFIFOOffersEveryEligibleShopWithoutAnAllowance(t *testing.T) {
	t.Parallel()
	f := scheduler.NewFIFO()
	if _, ok := f.Next(); ok {
		t.Fatal("an empty FIFO made an offer")
	}
	f.Update([]scheduler.Shop{shop("a", 1, true, true), shop("b", 9, true, false), shop("c", 1, true, true), shop("d", 1, false, false)})
	o := mustNext(t, f)
	if !slices.Equal(o.Shops, []string{"a", "c"}) || o.Allowance != scheduler.NoAllowance {
		t.Fatalf("offer = %+v, want shops a and c with no allowance", o)
	}
	// Weights play no part: FIFO is not fair, which is the point of the experiment.
	f.Done(o, scheduler.Result{Served: 7, End: scheduler.Continue})
	if _, ok := f.Next(); !ok {
		t.Fatal("FIFO stopped offering after a full claim")
	}
	f.Done(o, scheduler.Result{Served: 1, End: scheduler.Empty})
	if _, ok := f.Next(); ok {
		t.Fatal("FIFO repeated an offer after a claim that came back short")
	}
	f.Update([]scheduler.Shop{shop("a", 1, true, true)})
	if _, ok := f.Next(); !ok {
		t.Fatal("FIFO offered nothing after an Update showed an eligible shop")
	}
}

// claimOf simulates the claim transaction against a shop's queue of job costs:
// jobs oldest first while the cumulative cost stays within the allowance and
// fewer than waiting have been taken.
func claimOf(costs []int64, next *int, waiting int, allowance int64) (served int64, end scheduler.TurnEnd) {
	took := 0
	for took < waiting {
		c := costs[*next%len(costs)]
		if served+c > allowance {
			return served, scheduler.Spent
		}
		served += c
		*next++
		took++
	}
	return served, scheduler.Continue
}

// P4-2, Q6 (NFR-6, D-026, AC-9.2): with random weights, job costs and claim
// batch sizes, under saturation every shop's served cost units stay within
// quantum + C of its weighted share after every claim, where the shop's quantum
// is weight × base quantum and C, the largest job cost, is the base quantum.
func TestQ6EveryBackloggedShopStaysWithinItsWeightedShare(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(2, 7).Draw(rt, "shops")
		c := rapid.Int64Range(1, 9).Draw(rt, "largest job cost, and the base quantum")
		weights := rapid.SliceOfN(rapid.IntRange(1, 6), n, n).Draw(rt, "weights")
		costs := make([][]int64, n)
		shops := make([]scheduler.Shop, n)
		totalWeight := int64(0)
		for i := range n {
			costs[i] = rapid.SliceOfN(rapid.Int64Range(1, c), 1, 40).Draw(rt, fmt.Sprintf("costs of shop %d", i))
			shops[i] = shop(fmt.Sprintf("s%d", i), weights[i], true, true)
			totalWeight += int64(weights[i])
		}
		d := newDRR(rt, c)
		d.Update(shops)

		served := make([]int64, n)
		position := make([]int, n)
		total := int64(0)
		for claim := range rapid.IntRange(1, 250).Draw(rt, "claims") {
			waiting := rapid.IntRange(1, 12).Draw(rt, "waiting workers")
			offer, ok := d.Next()
			if !ok {
				rt.Fatal("a backlogged shop was not offered")
			}
			var i int
			if _, err := fmt.Sscanf(offer.Shops[0], "s%d", &i); err != nil {
				rt.Fatal(err)
			}
			cost, end := claimOf(costs[i], &position[i], waiting, offer.Allowance)
			d.Done(offer, scheduler.Result{Served: cost, End: end})
			served[i] += cost
			total += cost

			for j := range n {
				// served_j ≥ (w_j / W) × total − (w_j × c + c), in integers.
				lhs := served[j] * totalWeight
				rhs := int64(weights[j])*total - (int64(weights[j])*c+c)*totalWeight
				if lhs < rhs {
					rt.Fatalf("after claim %d, shop %d has %d of %d cost units served (weight %d of %d, base quantum %d): below its share by more than its quantum plus the largest cost",
						claim, j, served[j], total, weights[j], totalWeight, c)
				}
			}
		}
	})
}

// The scheduler's own state stays sound under any mix of refreshes and claim
// outcomes: deficits are never negative, an offer's allowance is its deficit,
// only eligible shops are offered, and a shop that is skipped gains nothing.
func TestDRRStateStaysSoundUnderRandomRefreshesAndResults(t *testing.T) {
	t.Parallel()
	ids := []string{"a", "b", "c", "d"}
	rapid.Check(t, func(rt *rapid.T) {
		quantum := rapid.Int64Range(1, 6).Draw(rt, "quantum")
		d := newDRR(rt, quantum)
		eligible := map[string]bool{} // what the next Next may offer
		weight := map[string]int64{}

		check := func() {
			for _, id := range ids {
				if v, ok := d.Deficit(id); ok && v < 0 {
					rt.Fatalf("shop %s has deficit %d", id, v)
				}
			}
		}
		rt.Repeat(map[string]func(*rapid.T){
			"update": func(rt *rapid.T) {
				var shops []scheduler.Shop
				for _, id := range ids {
					if !rapid.Bool().Draw(rt, "reported "+id) {
						eligible[id] = false
						continue
					}
					w := rapid.IntRange(1, 4).Draw(rt, "weight "+id)
					runnable := rapid.Bool().Draw(rt, "runnable "+id)
					ok := runnable && rapid.Bool().Draw(rt, "eligible "+id)
					shops = append(shops, shop(id, w, runnable, ok))
					eligible[id], weight[id] = ok, int64(w)
				}
				d.Update(shops)
				for _, id := range ids {
					if _, listed := d.Deficit(id); listed && !slices.ContainsFunc(shops, func(s scheduler.Shop) bool { return s.ID == id && s.Runnable }) {
						rt.Fatalf("shop %s is listed but not runnable", id)
					}
				}
				check()
			},
			"claim": func(rt *rapid.T) {
				before := map[string]int64{}
				for _, id := range ids {
					if v, ok := d.Deficit(id); ok {
						before[id] = v
					}
				}
				o, ok := d.Next()
				anyEligible := slices.ContainsFunc(ids, func(id string) bool { _, listed := d.Deficit(id); return listed && eligible[id] })
				if ok != anyEligible {
					rt.Fatalf("Next returned %v with eligible shops = %v", ok, anyEligible)
				}
				if !ok {
					check()
					return
				}
				id := o.Shops[0]
				if !eligible[id] {
					rt.Fatalf("offered ineligible shop %s", id)
				}
				now, _ := d.Deficit(id)
				if o.Allowance != now || o.Allowance < 1 {
					rt.Fatalf("allowance %d, deficit %d", o.Allowance, now)
				}
				if gained := now - before[id]; gained != 0 && gained != weight[id]*quantum {
					rt.Fatalf("shop %s gained %d, want 0 (open turn) or %d", id, gained, weight[id]*quantum)
				}
				for _, other := range ids {
					if v, listed := d.Deficit(other); listed && other != id && v != before[other] {
						rt.Fatalf("shop %s changed from %d to %d while shop %s was offered", other, before[other], v, id)
					}
				}
				served := rapid.Int64Range(0, o.Allowance).Draw(rt, "served")
				end := rapid.SampledFrom([]scheduler.TurnEnd{scheduler.Continue, scheduler.Spent, scheduler.Blocked, scheduler.Empty}).Draw(rt, "end")
				d.Done(o, scheduler.Result{Served: served, End: end})
				if end == scheduler.Blocked || end == scheduler.Empty {
					eligible[id] = false
				}
				check()
			},
		})
	})
}
