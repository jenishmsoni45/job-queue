package chaos

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// The settings of a chaos run (design §2, §18, D-023, D-044). They are
// settings, not measurements.
const (
	// FaultWindow is how long a run's faults last: every fault starts and ends
	// inside it, and the workload is sized to last about as long (D-044).
	FaultWindow = 30 * time.Second
	// RunTimeout fails a run whose accepted jobs aren't all terminal by then.
	RunTimeout = 120 * time.Second
	// JobsPerRun is how many jobs a run enqueues across the shops (D-023).
	JobsPerRun = 2000
	// MaxJobSleepMS is the longest a test.effect job sleeps before its effect.
	MaxJobSleepMS = 250
	// PostgresStopTimeout is how long a clean Postgres restart waits for the
	// server to stop before Docker sends SIGKILL.
	PostgresStopTimeout = 10 * time.Second
)

// The services and Toxiproxy proxies the harness acts on (D-042). Each proxy is
// one process's link to Postgres (docker-compose.chaos.yml).
const (
	Worker1    = "worker-1"
	Worker2    = "worker-2"
	Postgres   = "postgres"
	ProxyW1    = "pg-worker-1"
	ProxyW2    = "pg-worker-2"
	ProxyDisp  = "pg-dispatcher"
	EffectKind = "test.effect"
)

var (
	// Workers are the worker services, in a fixed order.
	Workers = []string{Worker1, Worker2}
	// Services are every service the harness may touch.
	Services = []string{Postgres, Worker1, Worker2}
	// Proxies are the Postgres links faults can be injected on.
	Proxies = []string{ProxyDisp, ProxyW1, ProxyW2}
)

// FaultKind is one kind of fault (design §18).
type FaultKind string

// The fault kinds.
const (
	KillWorker      FaultKind = "kill_worker"      // SIGKILL a worker; start it again after the duration
	PauseWorker     FaultKind = "pause_worker"     // freeze a worker past its lease, then thaw it
	RestartPostgres FaultKind = "restart_postgres" // clean restart, or SIGKILL and start (D-043)
	Latency         FaultKind = "latency"          // Toxiproxy latency on one Postgres link
	Timeout         FaultKind = "timeout"          // Toxiproxy timeout: data stops, then the link closes
	ResetPeer       FaultKind = "reset_peer"       // Toxiproxy reset_peer: connections are reset
)

// Fault is one step of a fault schedule.
type Fault struct {
	Kind FaultKind `json:"kind"`
	// Target is a service (worker-1, worker-2, postgres) or a proxy
	// (pg-worker-1, pg-worker-2, pg-dispatcher).
	Target string `json:"target"`
	// AtMS is when the fault starts, in milliseconds from the start of the run.
	AtMS int64 `json:"at_ms"`
	// DurationMS is how long a worker stays down or paused, how long Postgres
	// stays down after a hard kill, or how long a toxic stays in place. A clean
	// Postgres restart has none: it lasts as long as the restart takes.
	DurationMS int64 `json:"duration_ms"`
	// Hard makes a Postgres restart a SIGKILL followed by a start (D-043).
	Hard bool `json:"hard,omitempty"`
	// Stream and Attributes configure a toxic.
	Stream     string           `json:"stream,omitempty"`
	Attributes map[string]int64 `json:"attributes,omitempty"`
}

// PlannedJob is one job of a run's workload.
type PlannedJob struct {
	Key     string `json:"key"`
	Shop    string `json:"shop"`
	SleepMS int    `json:"sleep_ms"`
}

// Plan is everything a seed decides: the workload and the fault schedule.
type Plan struct {
	Seed   uint64       `json:"seed"`
	Jobs   []PlannedJob `json:"-"`
	Faults []Fault      `json:"faults"`
}

// NewPlan returns the plan for seed. It is a pure function of seed and shops:
// the same seed always gives the same plan, so CHAOS_SEED replays a run
// (P3-2, D-023).
func NewPlan(seed uint64, shops []string) Plan {
	jobsRNG := rand.New(rand.NewPCG(seed, 1))   //nolint:gosec // a reproducible test schedule, not a secret
	faultsRNG := rand.New(rand.NewPCG(seed, 2)) //nolint:gosec // a reproducible test schedule, not a secret

	p := Plan{Seed: seed, Jobs: make([]PlannedJob, JobsPerRun)}
	for i := range p.Jobs {
		p.Jobs[i] = PlannedJob{
			Key:     fmt.Sprintf("chaos-%d-%04d", seed, i),
			Shop:    shops[jobsRNG.IntN(len(shops))],
			SleepMS: jobsRNG.IntN(MaxJobSleepMS + 1),
		}
	}
	p.Faults = newSchedule(faultsRNG)
	return p
}

// The relative weights of the fault kinds.
var kindWeights = []struct {
	kind   FaultKind
	weight int
}{
	{KillWorker, 3}, {PauseWorker, 2}, {RestartPostgres, 1}, {Latency, 2}, {Timeout, 1}, {ResetPeer, 2},
}

// between returns a uniform value in [lo, hi].
func between(rng *rand.Rand, lo, hi int64) int64 { return lo + rng.Int64N(hi-lo+1) }

// newSchedule draws 3 to 8 faults. Faults on the same target never overlap, and
// each one starts and ends inside the fault window (D-044).
func newSchedule(rng *rand.Rand) []Fault {
	total := 0
	for _, w := range kindWeights {
		total += w.weight
	}
	type span struct{ from, to int64 }
	busy := map[string][]span{}
	window := FaultWindow.Milliseconds()

	n := 3 + rng.IntN(6)
	var faults []Fault
	for range n {
		pick := rng.IntN(total)
		var kind FaultKind
		for _, w := range kindWeights {
			if pick < w.weight {
				kind = w.kind
				break
			}
			pick -= w.weight
		}

		f := Fault{Kind: kind}
		switch kind {
		case KillWorker:
			f.Target = Workers[rng.IntN(len(Workers))]
			f.DurationMS = between(rng, 500, 3000)
		case PauseWorker:
			f.Target = Workers[rng.IntN(len(Workers))]
			f.DurationMS = between(rng, 2500, 5000) // past the 2 s chaos lease
		case RestartPostgres:
			f.Target = Postgres
			f.Hard = rng.IntN(2) == 0
			if f.Hard {
				f.DurationMS = between(rng, 0, 2000)
			}
		case Latency:
			f.Target = Proxies[rng.IntN(len(Proxies))]
			f.DurationMS = between(rng, 1000, 6000)
			f.Stream = []string{"downstream", "upstream"}[rng.IntN(2)]
			f.Attributes = map[string]int64{"latency": between(rng, 50, 1500), "jitter": between(rng, 0, 200)}
		case Timeout:
			f.Target = Proxies[rng.IntN(len(Proxies))]
			f.DurationMS = between(rng, 1000, 5000)
			f.Stream = []string{"downstream", "upstream"}[rng.IntN(2)]
			f.Attributes = map[string]int64{"timeout": between(rng, 500, 3000)}
		case ResetPeer:
			f.Target = Proxies[rng.IntN(len(Proxies))]
			f.DurationMS = between(rng, 500, 3000)
			f.Stream = []string{"downstream", "upstream"}[rng.IntN(2)]
			f.Attributes = map[string]int64{"timeout": between(rng, 0, 1000)}
		}

		length := f.End() // f.AtMS is still 0
		placed := false
		for range 20 {
			f.AtMS = rng.Int64N(window - length + 1)
			s := span{f.AtMS, f.AtMS + length}
			if !slices.ContainsFunc(busy[f.Target], func(o span) bool { return s.from < o.to && o.from < s.to }) {
				busy[f.Target] = append(busy[f.Target], s)
				placed = true
				break
			}
		}
		if placed {
			faults = append(faults, f)
		}
	}
	slices.SortStableFunc(faults, func(a, b Fault) int { return cmp.Compare(a.AtMS, b.AtMS) })
	return faults
}

// End returns when the fault's planned effect is over, in milliseconds from the
// start of the run. For Postgres it includes time to start up again: crash
// recovery after a hard kill, or the whole stop and start of a clean restart.
func (f Fault) End() int64 {
	switch {
	case f.Kind == RestartPostgres && f.Hard:
		return f.AtMS + f.DurationMS + 3000
	case f.Kind == RestartPostgres:
		return f.AtMS + PostgresStopTimeout.Milliseconds() + 2000
	default:
		return f.AtMS + f.DurationMS
	}
}
