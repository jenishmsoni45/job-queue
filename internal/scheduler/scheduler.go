// Package scheduler chooses which shop's jobs the dispatcher claims next
// (design §6.2, D-013). It has two implementations behind one interface, deficit
// round robin (DRR) and FIFO, and both are pure: no I/O, no clock, no goroutines.
// The dispatcher tells a scheduler which shops are eligible, asks it for an
// offer, claims in Postgres under that offer, and reports what the claim did.
//
// Because the claim transaction is the same for both (design §6.3), switching
// from DRR to FIFO changes which jobs are offered and nothing else (AC-9.3).
package scheduler

import "fmt"

// Mode names a scheduler.
type Mode string

// The schedulers.
const (
	ModeDRR  Mode = "drr"
	ModeFIFO Mode = "fifo"
)

// ParseMode reads a scheduler name, such as the QUEUED_SCHEDULER setting.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(s); m {
	case ModeDRR, ModeFIFO:
		return m, nil
	}
	return "", fmt.Errorf("scheduler %q is not %q or %q", s, ModeDRR, ModeFIFO)
}

// New returns a scheduler. quantum is the DRR base quantum: the largest job
// cost in config (design §2). FIFO ignores it.
func New(mode Mode, quantum int64) (Scheduler, error) {
	switch mode {
	case ModeDRR:
		return NewDRR(quantum)
	case ModeFIFO:
		return NewFIFO(), nil
	}
	return nil, fmt.Errorf("scheduler: unknown mode %q", mode)
}

// Shop is what the dispatcher knows about one shop at a refresh (design §6.1).
type Shop struct {
	ID     string
	Weight int
	// Runnable means the shop has a job that can be claimed now. A shop that is
	// not runnable has nothing to schedule: DRR drops it and forgets its deficit.
	Runnable bool
	// Eligible means Runnable, and the shop is below its concurrency cap, has a
	// token, and is not throttled. A runnable shop that is not eligible is
	// skipped: DRR gives it no quantum and keeps its deficit.
	Eligible bool
}

// NoAllowance is the Offer.Allowance of a claim that takes jobs without a cost
// limit, which is what FIFO does.
const NoAllowance = -1

// Offer says what one claim may take.
type Offer struct {
	// Shops are the shops to claim from. DRR offers one; FIFO offers every
	// eligible shop and the claim takes the globally oldest jobs.
	Shops []string
	// Allowance is the most cost units the claim may take, or NoAllowance. The
	// claim takes jobs oldest first while their cumulative cost stays within it.
	Allowance int64
}

// TurnEnd says why a claim stopped, which decides what happens to the shop's turn.
type TurnEnd int

const (
	// Continue: the claim took everything it was offered, and stopped only
	// because no more workers were waiting. The turn goes on with the next claim.
	Continue TurnEnd = iota
	// Spent: the next job costs more than the allowance left. The turn is over
	// and the shop keeps what is left of its deficit.
	Spent
	// Blocked: the shop has runnable jobs but could not claim them: its cap, its
	// tokens or a throttle stopped the claim. It is skipped until the next Update
	// and keeps its deficit.
	Blocked
	// Empty: the shop had no runnable job to claim. DRR drops it and resets its
	// deficit to 0, and it comes back at the next Update that shows it runnable.
	Empty
)

// Result is what a claim did.
type Result struct {
	Served int64 // cost units claimed
	End    TurnEnd
}

// Scheduler picks the next claim. It is not safe for concurrent use: the
// dispatcher's claim loop is its only caller.
type Scheduler interface {
	// Update replaces the scheduler's view of the shops with a fresh one.
	Update(shops []Shop)
	// Next returns the next offer, or false if no shop is eligible.
	Next() (Offer, bool)
	// Done reports the result of the claim made under offer.
	Done(offer Offer, r Result)
}
