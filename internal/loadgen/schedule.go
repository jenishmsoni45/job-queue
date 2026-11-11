// Package loadgen drives the Black Friday experiment (design §17, D-009): an
// open-loop load of order.tag jobs with seeded Poisson arrivals per shop, sent
// as one batch per shop per tick. The schedule depends only on the seed and the
// settings, so the FIFO and DRR runs get the same arrivals.
package loadgen

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
)

// Phase is a part of the timeline.
type Phase string

const (
	// Warmup: every shop sends at the steady rate.
	Warmup Phase = "warmup"
	// Spike: the spike shop sends at the spike rate; the others stay steady.
	Spike Phase = "spike"
	// Tail: every shop is back at the steady rate until the spike's backlog
	// has drained (D-056).
	Tail Phase = "tail"
)

// Tick is one tick's arrivals.
type Tick struct {
	Index int
	// At is when the tick starts, from the start of the run.
	At    time.Duration
	Phase Phase
	// Counts holds each shop's arrivals in the tick, in the order of the shops
	// the schedule was made with.
	Counts []int
}

// Schedule generates the arrivals tick by tick. Each shop's arrivals are a
// Poisson process: the gaps between arrivals are exponential, drawn from the
// shop's own random stream, seeded by the run's seed and the shop's position,
// so one shop's arrivals never depend on another's. The rate changes only at the
// phase boundaries, where a process is restarted, which keeps it exactly
// Poisson. It is not safe for concurrent use.
type Schedule struct {
	cfg   config.Loadgen
	shops []string
	tick  time.Duration
	rngs  []*rand.Rand
	// next is each shop's next arrival time, in seconds from the start.
	next  []float64
	index int
}

// NewSchedule returns the schedule for shops under lg.
func NewSchedule(lg config.Loadgen, shops []string) *Schedule {
	s := &Schedule{
		cfg: lg, shops: shops, tick: time.Duration(lg.TickMS) * time.Millisecond,
		rngs: make([]*rand.Rand, len(shops)), next: make([]float64, len(shops)),
	}
	for i := range shops {
		s.rngs[i] = rand.New(rand.NewPCG(lg.Seed, uint64(i)+1)) //nolint:gosec // a reproducible schedule, not a secret
		s.next[i] = s.draw(i, 0)
	}
	return s
}

// boundaries returns the warm-up's and the spike's end, in seconds.
func (s *Schedule) boundaries() (warmupEnd, spikeEnd float64) {
	warmupEnd = float64(s.cfg.WarmupMS) / 1000
	return warmupEnd, warmupEnd + float64(s.cfg.SpikeMS)/1000
}

// PhaseAt returns the phase at d from the start.
func (s *Schedule) PhaseAt(d time.Duration) Phase {
	warmupEnd, spikeEnd := s.boundaries()
	switch t := d.Seconds(); {
	case t < warmupEnd:
		return Warmup
	case t < spikeEnd:
		return Spike
	default:
		return Tail
	}
}

// rate returns shop i's rate in the phase that holds time t, and when that
// phase ends (+Inf for the tail).
func (s *Schedule) rate(i int, t float64) (perSec, until float64) {
	warmupEnd, spikeEnd := s.boundaries()
	switch {
	case t < warmupEnd:
		return s.cfg.RatePerShop, warmupEnd
	case t < spikeEnd:
		if s.shops[i] == s.cfg.SpikeShop {
			return s.cfg.SpikeRate, spikeEnd
		}
		return s.cfg.RatePerShop, spikeEnd
	default:
		return s.cfg.RatePerShop, math.Inf(1)
	}
}

// draw returns shop i's first arrival after from. A gap that crosses into the
// next phase is thrown away and drawn again from the boundary at the new rate.
func (s *Schedule) draw(i int, from float64) float64 {
	for {
		r, until := s.rate(i, from)
		at := from + s.rngs[i].ExpFloat64()/r
		if at < until {
			return at
		}
		from = until
	}
}

// Next returns the next tick's arrivals.
func (s *Schedule) Next() Tick {
	start := time.Duration(s.index) * s.tick
	end := (start + s.tick).Seconds()
	t := Tick{Index: s.index, At: start, Phase: s.PhaseAt(start), Counts: make([]int, len(s.shops))}
	for i := range s.shops {
		for s.next[i] < end {
			t.Counts[i]++
			s.next[i] = s.draw(i, s.next[i])
		}
	}
	s.index++
	return t
}
