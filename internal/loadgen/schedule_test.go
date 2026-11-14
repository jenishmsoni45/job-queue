package loadgen

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
)

var bf = config.Loadgen{
	Seed: 20261221, TickMS: 100, RatePerShop: 20, WarmupMS: 120_000,
	SpikeShop: "shop-01", SpikeRate: 2000, SpikeMS: 300_000, DrainMaxMS: 1_800_000,
}

var tenShops = []string{"shop-01", "shop-02", "shop-03", "shop-04", "shop-05", "shop-06", "shop-07", "shop-08", "shop-09", "shop-10"}

func ticks(s *Schedule, n int) []Tick {
	out := make([]Tick, n)
	for i := range out {
		out[i] = s.Next()
	}
	return out
}

// Design §17 and the phase 5 loadgen test: the same seed produces the same
// arrival schedule, tick for tick, so the FIFO and DRR runs get the same load.
func TestSameSeedSameSchedule(t *testing.T) {
	const n = 6000 // warm-up, spike and a minute of the tail
	a, b := ticks(NewSchedule(bf, tenShops), n), ticks(NewSchedule(bf, tenShops), n)
	for i := range a {
		if a[i].Index != b[i].Index || a[i].At != b[i].At || a[i].Phase != b[i].Phase || !slices.Equal(a[i].Counts, b[i].Counts) {
			t.Fatalf("tick %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
	other := bf
	other.Seed++
	c := ticks(NewSchedule(other, tenShops), n)
	same := 0
	for i := range a {
		if slices.Equal(a[i].Counts, c[i].Counts) {
			same++
		}
	}
	if same > n/10 {
		t.Errorf("another seed matched %d of %d ticks; the seed should change the schedule", same, n)
	}
}

// Each shop's arrivals come from its own stream: adding a shop, or giving
// another shop the spike, leaves a shop's steady arrivals unchanged.
func TestShopsAreIndependent(t *testing.T) {
	const n = 3000
	base := ticks(NewSchedule(bf, tenShops[:3]), n)
	more := ticks(NewSchedule(bf, append(slices.Clone(tenShops[:3]), "shop-99")), n)
	moved := bf
	moved.SpikeShop = "shop-03"
	spikeMoved := ticks(NewSchedule(moved, tenShops[:3]), n)
	for i := range base {
		if !slices.Equal(base[i].Counts, more[i].Counts[:3]) {
			t.Fatalf("tick %d: adding a shop changed the others: %v vs %v", i, base[i].Counts, more[i].Counts)
		}
		if base[i].Counts[1] != spikeMoved[i].Counts[1] {
			t.Fatalf("tick %d: moving the spike to shop-03 changed shop-02", i)
		}
	}
}

// The phases fall where the settings put them, and every shop's arrivals in
// each phase match its Poisson rate: the spike shop sends 2,000 a second during
// the spike and 20 otherwise, and the rest send 20 throughout. A Poisson count
// with mean m has standard deviation √m, so each total must be within 5√m of m.
func TestPhasesAndRates(t *testing.T) {
	s := NewSchedule(bf, tenShops)
	total := map[Phase][]int{Warmup: make([]int, 10), Spike: make([]int, 10), Tail: make([]int, 10)}
	length := map[Phase]time.Duration{}
	for range 5400 + 1200 { // warm-up, spike and two minutes of tail
		tk := s.Next()
		want := Warmup
		switch {
		case tk.At >= 420*time.Second:
			want = Tail
		case tk.At >= 120*time.Second:
			want = Spike
		}
		if tk.Phase != want {
			t.Fatalf("tick %d at %v is %s, want %s", tk.Index, tk.At, tk.Phase, want)
		}
		if tk.At != time.Duration(tk.Index)*100*time.Millisecond {
			t.Fatalf("tick %d starts at %v", tk.Index, tk.At)
		}
		length[tk.Phase] += 100 * time.Millisecond
		for i, c := range tk.Counts {
			total[tk.Phase][i] += c
		}
	}
	for phase, counts := range total {
		for i, got := range counts {
			rate := bf.RatePerShop
			if phase == Spike && tenShops[i] == bf.SpikeShop {
				rate = bf.SpikeRate
			}
			mean := rate * length[phase].Seconds()
			if math.Abs(float64(got)-mean) > 5*math.Sqrt(mean) {
				t.Errorf("%s, %s: %d arrivals, want about %.0f", phase, tenShops[i], got, mean)
			}
		}
	}
}

// PhaseAt agrees with the boundaries.
func TestPhaseAt(t *testing.T) {
	s := NewSchedule(bf, tenShops)
	for d, want := range map[time.Duration]Phase{
		0: Warmup, 119_999 * time.Millisecond: Warmup, 120 * time.Second: Spike,
		419_999 * time.Millisecond: Spike, 420 * time.Second: Tail, time.Hour: Tail,
	} {
		if got := s.PhaseAt(d); got != want {
			t.Errorf("PhaseAt(%v) = %s, want %s", d, got, want)
		}
	}
}

func TestQuantile(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5}
	for q, want := range map[float64]float64{0: 1, 0.5: 3, 1: 5, 0.25: 2, 0.9: 4.6} {
		if got := Quantile(v, q); math.Abs(got-want) > 1e-9 {
			t.Errorf("Quantile(%v) = %v, want %v", q, got, want)
		}
	}
	if got := Quantile([]float64{7}, 0.99); got != 7 {
		t.Errorf("one value: %v", got)
	}
	if !math.IsNaN(Quantile(nil, 0.5)) {
		t.Error("no values should give NaN")
	}
}
