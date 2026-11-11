package scheduler

import "errors"

// DRR is weighted deficit round robin with job costs (Shreedhar and Varghese,
// design §6.2, D-013).
//
// Runnable shops sit in a round-robin list. When the pointer reaches a shop its
// turn starts and its deficit grows by weight × quantum. Each claim in the turn
// is offered the whole deficit as its allowance, and the deficit drops by the cost
// the claim took. The turn ends when the next job costs more than what is left. A
// claim that stopped only because fewer workers were waiting does not end the turn.
//
// While every shop stays backlogged, a shop's deficit at the end of its turn is
// in [0, C), where C is the largest job cost, so a shop's cost units served never
// fall below its weighted share by more than weight × quantum + C (D-026, Q6).
type DRR struct {
	quantum int64
	list    []*entry // runnable shops in round-robin order
	pos     int      // index of the shop whose turn it is, or comes next
	open    bool     // the shop at pos has had its quantum for this turn
}

type entry struct {
	id       string
	weight   int64
	deficit  int64
	eligible bool
}

// NewDRR returns a DRR scheduler with the given base quantum, which must be
// positive. It should be the largest job cost, so that a shop's first job
// always fits in the first turn it gets.
func NewDRR(quantum int64) (*DRR, error) {
	if quantum < 1 {
		return nil, errors.New("scheduler: the DRR quantum must be at least 1")
	}
	return &DRR{quantum: quantum}, nil
}

// Update implements Scheduler. A shop that is no longer runnable leaves the
// list and loses its deficit, and a shop that becomes runnable joins the end of
// the list with a deficit of 0. Everyone else keeps their place and deficit. An
// open turn survives an Update, unless its shop left.
func (d *DRR) Update(shops []Shop) {
	incoming := make(map[string]Shop, len(shops))
	for _, s := range shops {
		if _, dup := incoming[s.ID]; !dup {
			incoming[s.ID] = s
		}
	}
	kept := make([]*entry, 0, len(d.list))
	pos, currentLeft := 0, false
	for i, e := range d.list {
		s, ok := incoming[e.id]
		if !ok || !s.Runnable {
			currentLeft = currentLeft || i == d.pos
			continue
		}
		e.weight, e.eligible = int64(s.Weight), s.Eligible
		if i < d.pos {
			pos++ // a survivor before the pointer: the pointer moves with it
		}
		kept = append(kept, e)
	}
	if currentLeft {
		d.open = false // pos is now the survivor that followed the one that left
	}
	listed := make(map[string]bool, len(kept))
	for _, e := range kept {
		listed[e.id] = true
	}
	for _, s := range shops {
		if s.Runnable && !listed[s.ID] {
			listed[s.ID] = true
			kept = append(kept, &entry{id: s.ID, weight: int64(s.Weight), eligible: s.Eligible})
		}
	}
	d.list = kept
	d.pos = pos
	d.wrap()
}

// wrap keeps pos inside the list, and closes the turn if the list is empty.
func (d *DRR) wrap() {
	if len(d.list) == 0 {
		d.pos, d.open = 0, false
		return
	}
	if d.pos >= len(d.list) {
		d.pos = 0
		d.open = false
	}
}

// advance ends the current turn and moves the pointer on.
func (d *DRR) advance() {
	d.open = false
	if len(d.list) > 0 {
		d.pos = (d.pos + 1) % len(d.list)
	}
}

// Next implements Scheduler. It offers the shop whose turn it is, starting the
// turn (and granting the quantum) if it hasn't started. An ineligible shop is
// skipped without a quantum.
func (d *DRR) Next() (Offer, bool) {
	for range len(d.list) {
		e := d.list[d.pos]
		if !e.eligible {
			d.advance()
			continue
		}
		if !d.open {
			e.deficit += e.weight * d.quantum
			d.open = true
		}
		return Offer{Shops: []string{e.id}, Allowance: e.deficit}, true
	}
	return Offer{}, false
}

// Done implements Scheduler. The offer's shop pays for what the claim served.
func (d *DRR) Done(offer Offer, r Result) {
	if len(offer.Shops) != 1 {
		return
	}
	i := d.index(offer.Shops[0])
	if i < 0 {
		return // the shop left at an Update after this claim began
	}
	e := d.list[i]
	e.deficit = max(0, e.deficit-r.Served)
	current := i == d.pos && d.open
	switch r.End {
	case Continue:
		if e.deficit == 0 && current {
			d.advance() // every cost is at least 1, so nothing more fits
		}
	case Spent:
		if current {
			d.advance()
		}
	case Blocked:
		e.eligible = false
		if current {
			d.advance()
		}
	case Empty:
		d.list = append(d.list[:i], d.list[i+1:]...)
		if i < d.pos {
			d.pos--
		} else if i == d.pos {
			d.open = false
		}
		d.wrap()
	}
}

func (d *DRR) index(id string) int {
	for i, e := range d.list {
		if e.id == id {
			return i
		}
	}
	return -1
}

// Deficit returns a listed shop's current deficit, and false if the shop is not
// in the list (it has no runnable job, or was never reported).
func (d *DRR) Deficit(id string) (int64, bool) {
	if i := d.index(id); i >= 0 {
		return d.list[i].deficit, true
	}
	return 0, false
}
