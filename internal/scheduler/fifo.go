package scheduler

// FIFO offers every eligible shop at once and no allowance, so the claim takes
// the globally oldest runnable jobs by (run_at, id) across them (design §6.2).
// Caps, token buckets and throttles still apply, because they decide which
// shops are eligible and are enforced again inside the claim transaction.
type FIFO struct {
	eligible []string
	spent    bool
}

// NewFIFO returns a FIFO scheduler.
func NewFIFO() *FIFO { return &FIFO{} }

// Update implements Scheduler.
func (f *FIFO) Update(shops []Shop) {
	f.eligible = f.eligible[:0]
	for _, s := range shops {
		if s.Eligible {
			f.eligible = append(f.eligible, s.ID)
		}
	}
	f.spent = false
}

// Next implements Scheduler. After a claim that stopped for any reason but a
// full set of waiting workers, it offers nothing until the next Update, so the
// dispatcher doesn't repeat a claim that came back short.
func (f *FIFO) Next() (Offer, bool) {
	if f.spent || len(f.eligible) == 0 {
		return Offer{}, false
	}
	return Offer{Shops: append([]string(nil), f.eligible...), Allowance: NoAllowance}, true
}

// Done implements Scheduler.
func (f *FIFO) Done(_ Offer, r Result) {
	if r.End != Continue {
		f.spent = true
	}
}
