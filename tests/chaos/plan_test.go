package chaos

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

var testShops = []string{"shop-1.chaos.test", "shop-2.chaos.test", "shop-3.chaos.test", "shop-4.chaos.test", "shop-5.chaos.test"}

// P3-2: the same seed always gives the same plan, workload and fault schedule
// alike, so CHAOS_SEED replays a run. Different seeds give different schedules.
func TestPlanIsAFunctionOfTheSeed(t *testing.T) {
	schedules := map[string]uint64{}
	for seed := range uint64(200) {
		a, b := NewPlan(seed, testShops), NewPlan(seed, testShops)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("seed %d gave two different plans", seed)
		}
		key := fmt.Sprint(a.Faults)
		if other, dup := schedules[key]; dup {
			t.Errorf("seeds %d and %d gave the same fault schedule", other, seed)
		}
		schedules[key] = seed
	}
}

// Every schedule follows design §18 and D-044: 3 to 8 faults are drawn (one
// that can't be placed without an overlap is dropped), each inside the fault
// window, none overlapping another on the same target, and every pause longer
// than the 2 s chaos lease.
func TestSchedulesStayInsideTheirRules(t *testing.T) {
	kinds := map[FaultKind]int{}
	targets := map[string]int{}
	hard, clean := 0, 0
	for seed := range uint64(500) {
		p := NewPlan(seed, testShops)
		if n := len(p.Faults); n < 1 || n > 8 {
			t.Errorf("seed %d: %d faults, want 1 to 8", seed, n)
		}
		for i, f := range p.Faults {
			kinds[f.Kind]++
			targets[f.Target]++
			if f.AtMS < 0 || f.End() > FaultWindow.Milliseconds() {
				t.Errorf("seed %d: fault %+v runs from %d to %d ms, outside the %v window", seed, f, f.AtMS, f.End(), FaultWindow)
			}
			if i > 0 && p.Faults[i-1].AtMS > f.AtMS {
				t.Errorf("seed %d: faults are not in time order", seed)
			}
			for _, g := range p.Faults[i+1:] {
				if g.Target == f.Target && f.AtMS < g.End() && g.AtMS < f.End() {
					t.Errorf("seed %d: %+v and %+v overlap on %s", seed, f, g, f.Target)
				}
			}
			switch f.Kind {
			case PauseWorker:
				if f.DurationMS <= 2000 {
					t.Errorf("seed %d: a pause of %d ms doesn't outlast the 2 s lease", seed, f.DurationMS)
				}
				fallthrough
			case KillWorker:
				if !slices.Contains(Workers, f.Target) {
					t.Errorf("seed %d: %s targets %s, not a worker", seed, f.Kind, f.Target)
				}
			case RestartPostgres:
				if f.Target != Postgres {
					t.Errorf("seed %d: a Postgres restart targets %s", seed, f.Target)
				}
				if f.Hard {
					hard++
				} else {
					clean++
				}
			case Latency, Timeout, ResetPeer:
				if !slices.Contains(Proxies, f.Target) || f.Stream == "" || len(f.Attributes) == 0 {
					t.Errorf("seed %d: bad toxic %+v", seed, f)
				}
			default:
				t.Errorf("seed %d: unknown kind %s", seed, f.Kind)
			}
		}
	}
	for _, k := range []FaultKind{KillWorker, PauseWorker, RestartPostgres, Latency, Timeout, ResetPeer} {
		if kinds[k] == 0 {
			t.Errorf("no schedule in 500 seeds has a %s fault", k)
		}
	}
	for _, target := range slices.Concat(Services, Proxies) {
		if targets[target] == 0 {
			t.Errorf("no schedule in 500 seeds targets %s", target)
		}
	}
	if hard == 0 || clean == 0 {
		t.Errorf("Postgres restarts: %d hard, %d clean; D-043 wants both", hard, clean)
	}
}

// The workload has JobsPerRun jobs with unique keys tied to the seed, spread
// over every shop, each sleeping at most MaxJobSleepMS.
func TestPlanWorkload(t *testing.T) {
	p := NewPlan(42, testShops)
	if len(p.Jobs) != JobsPerRun {
		t.Fatalf("%d jobs, want %d", len(p.Jobs), JobsPerRun)
	}
	keys := map[string]bool{}
	orders := map[int64]bool{}
	perShop := map[string]int{}
	for _, j := range p.Jobs {
		if j.OrderID <= 0 || orders[j.OrderID] {
			t.Errorf("job %s has order %d, repeated or not positive", j.Key, j.OrderID)
		}
		orders[j.OrderID] = true
		if keys[j.Key] || !strings.HasPrefix(j.Key, "chaos-42-") {
			t.Errorf("key %q is repeated or not tied to the seed", j.Key)
		}
		keys[j.Key] = true
		perShop[j.Shop]++
		if j.SleepMS < 0 || j.SleepMS > MaxJobSleepMS {
			t.Errorf("job %s sleeps %d ms", j.Key, j.SleepMS)
		}
	}
	for _, s := range testShops {
		if perShop[s] == 0 {
			t.Errorf("shop %s has no jobs", s)
		}
	}
	if NewPlan(43, testShops).Jobs[0].Key == p.Jobs[0].Key {
		t.Error("two seeds share dedupe keys, so a replay in the same database would dedupe against the other")
	}
}

// NFR-11's chaos check: every completed job's key applied once, none more than
// once, and a dead job may have none or one.
func TestMutationErrors(t *testing.T) {
	jobs := []RunJob{{1, "a", "completed"}, {2, "a", "dead"}, {3, "b", "completed"}, {4, "b", "dead"}}
	good := map[string]map[string]int{"a": {"job-1-tag": 1, "job-99-tag": 3}, "b": {"job-3-tag": 1, "job-4-tag": 1}}
	if _, err := MutationErrors(jobs, good); err != nil {
		t.Errorf("good logs: %v (another run's job-99 must not count)", err)
	}
	for name, logs := range map[string]map[string]map[string]int{
		"completed without one": {"a": {}, "b": {"job-3-tag": 1}},
		"completed twice":       {"a": {"job-1-tag": 2}, "b": {"job-3-tag": 1}},
		"dead twice":            {"a": {"job-1-tag": 1, "job-2-tag": 2}, "b": {"job-3-tag": 1}},
		"under another shop":    {"a": {"job-1-tag": 1, "job-3-tag": 1}, "b": {}},
	} {
		if _, err := MutationErrors(jobs, logs); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := MutationErrors([]RunJob{{1, "a", "dead"}}, good); err == nil {
		t.Error("a run with no completed job passed")
	}
}
