package bench

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/loadgen"
)

var t0 = time.Date(2026, 12, 21, 10, 0, 0, 0, time.UTC)

// testConfig is a short timeline: 20 s of warm-up and 20 s of spike, two shops
// at 10 jobs/s, with spike.test sending the spike.
func testConfig() *config.File {
	return &config.File{
		Shops: []config.Shop{{ID: "spike.test"}, {ID: "steady.test"}},
		Loadgen: &config.Loadgen{
			Seed: 1, TickMS: 100, RatePerShop: 10, WarmupMS: 20_000,
			SpikeShop: "spike.test", SpikeRate: 1000, SpikeMS: 20_000, DrainMaxMS: 60_000,
		},
	}
}

func key(id int64) string { return fmt.Sprintf("job-%d-tag", id) }

// gen makes one job that arrived at `at` after t0, waited `wait` for its
// first claim, and completed `run` later.
type gen struct{ next int64 }

func (g *gen) job(shop string, at, wait, run time.Duration) Job {
	g.next++
	arrived := t0.Add(at)
	claimed := arrived.Add(wait)
	done := claimed.Add(run)
	return Job{ID: g.next, Shop: shop, State: "completed", EnqueuedAt: arrived, FirstRunnableAt: arrived,
		FirstClaimedAt: &claimed, TerminalAt: &done, ClaimSeq: 1}
}

// steadyRun makes 10 jobs/s for each shop over 60 s. The steady shop waits
// warmWait in the warm-up and spikeWait in the spike; every job runs 30 ms.
// Every completed job has its one mutation.
func steadyRun(warmWait, spikeWait time.Duration) Input {
	var g gen
	var jobs []Job
	for i := range 600 {
		at := time.Duration(i) * 100 * time.Millisecond
		w := warmWait
		if at >= 20*time.Second && at < 40*time.Second {
			w = spikeWait
		}
		jobs = append(jobs, g.job("steady.test", at, w, 30*time.Millisecond))
		jobs = append(jobs, g.job("spike.test", at, time.Millisecond, 30*time.Millisecond))
	}
	in := Input{
		Scheduler: "drr", ConfigSHA256: "abc", Config: testConfig(), Jobs: jobs,
		Mutations: map[string]map[string]int{"spike.test": {}, "steady.test": {}}, Replays: map[string]int{},
		Shed: map[string]int64{"spike.test": 7}, MutationKey: key,
	}
	for _, j := range jobs {
		in.Mutations[j.Shop][key(j.ID)] = 1
	}
	return in
}

func analyze(t *testing.T, in Input) Run {
	t.Helper()
	r, err := Analyze(in)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func shop(r Run, id string) ShopSummary {
	for _, s := range r.Shops {
		if s.ID == id {
			return s
		}
	}
	return ShopSummary{}
}

// The summary's totals, waits, buckets and shed counts follow from the jobs.
func TestAnalyzeSummarizes(t *testing.T) {
	r := analyze(t, steadyRun(5*time.Millisecond, 20*time.Millisecond))
	if !r.T0.Equal(t0) || r.WarmupS != 20 || r.SpikeS != 20 || r.BucketS != 10 || r.SpikeShop != "spike.test" ||
		r.SpikeRate != 1000 || r.SteadyRate != 10 {
		t.Errorf("run header = %+v", r)
	}
	if r.Totals.Jobs != 1200 || r.Totals.Completed != 1200 || r.Totals.NotTerminal != 0 || r.Totals.Claims != 1200 ||
		r.Totals.ExtraClaims != 0 || r.Totals.Shed != 7 || r.Totals.MutationsApplied != 1200 {
		t.Errorf("totals = %+v", r.Totals)
	}
	s := shop(r, "steady.test")
	if s.WarmupWait.Count != 200 || s.WarmupWait.P99 != 5 || s.SpikeWait.Count != 200 || s.SpikeWait.P99 != 20 || s.EndToEnd.Count != 600 {
		t.Errorf("steady shop = %+v", s)
	}
	buckets := r.Series["steady.test"]
	if len(buckets) != 6 || buckets[2].Index != 2 || buckets[2].StartS != 20 || buckets[2].Count != 100 || buckets[2].P99MS != 20 || buckets[0].P99MS != 5 {
		t.Errorf("series = %+v", buckets)
	}
	if shop(r, "spike.test").Shed != 7 {
		t.Error("shed count lost")
	}
}

// NFR-9 passes while every spike bucket stays within 2 × the warm-up p99 +
// 100 ms, and fails as soon as one bucket goes over; the spike shop is not
// checked.
func TestNFR9(t *testing.T) {
	r := analyze(t, steadyRun(5*time.Millisecond, 110*time.Millisecond)) // bound: 2 × 5 + 100 = 110
	if !r.NFR9.Pass || len(r.NFR9.Shops) != 1 {
		t.Fatalf("NFR-9 = %+v, want a pass at exactly the bound", r.NFR9)
	}
	n := r.NFR9.Shops[0]
	if n.ID != "steady.test" || n.BoundMS != 110 || n.Buckets != 2 || n.Over != 0 || n.WorstP99MS != 110 {
		t.Errorf("check = %+v", n)
	}

	in := steadyRun(5*time.Millisecond, 20*time.Millisecond)
	// Slow down 2 of the 100 jobs that arrived in the last spike bucket, so
	// its p99 passes the bound.
	slowed := 0
	for i := range in.Jobs {
		j := &in.Jobs[i]
		if j.Shop == "steady.test" && j.FirstRunnableAt.Sub(t0) >= 30*time.Second && j.FirstRunnableAt.Sub(t0) < 40*time.Second && slowed < 2 {
			c := j.FirstRunnableAt.Add(2 * time.Second)
			j.FirstClaimedAt = &c
			slowed++
		}
	}
	r = analyze(t, in)
	if r.NFR9.Pass || r.NFR9.Shops[0].Over != 1 || r.NFR9.Shops[0].WorstBucket != 3 {
		t.Errorf("NFR-9 = %+v, want one bucket (3) over", r.NFR9)
	}
}

// NFR-9 can't pass with nothing to check: no warm-up jobs or no spike jobs fail it.
func TestNFR9NeedsData(t *testing.T) {
	in := steadyRun(5*time.Millisecond, 5*time.Millisecond)
	var kept []Job
	for _, j := range in.Jobs {
		if j.Shop == "spike.test" || j.FirstRunnableAt.Sub(t0) >= 20*time.Second {
			kept = append(kept, j)
		}
	}
	in.Jobs = kept
	if r := analyze(t, in); r.NFR9.Pass {
		t.Errorf("NFR-9 passed with no warm-up jobs: %+v", r.NFR9)
	}
}

// NFR-12: the warm-up backlog is swept over every enqueue and terminal
// transition; the limit is 2 s of arrivals (20 jobs at 10 jobs/s).
func TestNFR12(t *testing.T) {
	r := analyze(t, steadyRun(5*time.Millisecond, 5*time.Millisecond))
	if !r.NFR12.Pass || r.NFR12.Limit != 20 || shop(r, "steady.test").WarmupMaxBL != 1 {
		t.Errorf("NFR-12 = %+v, steady max %d", r.NFR12, shop(r, "steady.test").WarmupMaxBL)
	}
	in := steadyRun(5*time.Millisecond, 5*time.Millisecond)
	var g gen
	g.next = 10_000
	for range 21 { // 21 jobs arrive at once in the warm-up and wait a second
		in.Jobs = append(in.Jobs, g.job("steady.test", 5*time.Second, time.Second, time.Millisecond))
	}
	r = analyze(t, in)
	if r.NFR12.Pass || shop(r, "steady.test").WarmupMaxBL < 21 {
		t.Errorf("NFR-12 = %+v, want a fail above 21", r.NFR12)
	}
}

// NFR-12 must not count the spike shop's first spike tick. T0 trails loadgen's
// start, so that tick is enqueued a few ms before T0+warmup (D-057); a backlog
// from an earlier tick still counts.
func TestNFR12IgnoresTheFirstSpikeTick(t *testing.T) {
	tick := time.Duration(testConfig().Loadgen.TickMS) * time.Millisecond
	in := steadyRun(5*time.Millisecond, 5*time.Millisecond)
	var g gen
	g.next = 10_000
	for range 200 { // the first spike tick, 5 ms before the warm-up ends
		in.Jobs = append(in.Jobs, g.job("steady.test", 20*time.Second-5*time.Millisecond, time.Second, time.Millisecond))
	}
	if r := analyze(t, in); !r.NFR12.Pass {
		t.Errorf("NFR-12 counted the first spike tick: %+v", r.NFR12)
	}
	in = steadyRun(5*time.Millisecond, 5*time.Millisecond)
	for range 200 { // a backlog from a tick before the last one still fails
		in.Jobs = append(in.Jobs, g.job("steady.test", 20*time.Second-tick-5*time.Millisecond, time.Second, time.Millisecond))
	}
	if r := analyze(t, in); r.NFR12.Pass {
		t.Errorf("NFR-12 passed with a 200-job backlog a tick before the end: %+v", r.NFR12)
	}
}

// maxBacklog counts a job from its enqueue until it is terminal, counts an
// enqueue before a completion at the same instant, ignores what happens after
// the end, and counts jobs still unfinished at the end.
func TestMaxBacklog(t *testing.T) {
	at := func(s float64) *time.Time { x := t0.Add(time.Duration(s * float64(time.Second))); return &x }
	jobs := []Job{
		{EnqueuedAt: *at(0), TerminalAt: at(2)},
		{EnqueuedAt: *at(1), TerminalAt: at(3)},
		{EnqueuedAt: *at(2), TerminalAt: at(4)}, // at 2 s: one ends as one starts; peak counts it
		{EnqueuedAt: *at(9), TerminalAt: nil},   // never finished
		{EnqueuedAt: *at(9.5), TerminalAt: at(20)},
		{EnqueuedAt: *at(11), TerminalAt: at(12)}, // after the end
	}
	if got := maxBacklog(jobs, t0.Add(10*time.Second)); got != 3 {
		t.Errorf("maxBacklog = %d, want 3", got)
	}
}

// NFR-11: every completed job must have exactly one applied mutation, no key
// more than one, and no mutation may belong to no job.
func TestNFR11(t *testing.T) {
	if r := analyze(t, steadyRun(time.Millisecond, time.Millisecond)); !r.NFR11.Pass || r.NFR11.Completed != 1200 {
		t.Errorf("NFR-11 = %+v", r.NFR11)
	}
	for name, edit := range map[string]func(*Input){
		"missing":   func(in *Input) { delete(in.Mutations["steady.test"], key(1)) },
		"twice":     func(in *Input) { in.Mutations["steady.test"][key(1)] = 2 },
		"stray key": func(in *Input) { in.Mutations["spike.test"]["job-999999-tag"] = 1 },
		"dead twice": func(in *Input) {
			in.Jobs[0].State = "dead"
			in.Mutations["steady.test"][key(in.Jobs[0].ID)] = 2
		},
	} {
		in := steadyRun(time.Millisecond, time.Millisecond)
		edit(&in)
		r := analyze(t, in)
		if r.NFR11.Pass || len(r.NFR11.Examples) == 0 {
			t.Errorf("%s: NFR-11 = %+v, want a fail with an example", name, r.NFR11)
		}
	}
	// A dead job with no mutation is fine: it never got through.
	in := steadyRun(time.Millisecond, time.Millisecond)
	in.Jobs[0].State = "dead"
	delete(in.Mutations["steady.test"], key(in.Jobs[0].ID))
	if r := analyze(t, in); !r.NFR11.Pass {
		t.Errorf("a dead job without a mutation failed NFR-11: %+v", r.NFR11)
	}
}

// Reruns show up as extra claims, and not-terminal jobs are counted.
func TestAnalyzeCountsRerunsAndUnfinished(t *testing.T) {
	in := steadyRun(time.Millisecond, time.Millisecond)
	in.Jobs[0].ClaimSeq = 3
	in.Jobs[1].State, in.Jobs[1].TerminalAt = "queued", nil
	r := analyze(t, in)
	if r.Totals.ExtraClaims != 2 || r.Totals.Claims != 1202 || r.Totals.NotTerminal != 1 {
		t.Errorf("totals = %+v", r.Totals)
	}
}

func TestAnalyzeRejects(t *testing.T) {
	in := steadyRun(time.Millisecond, time.Millisecond)
	in.Config.Loadgen.WarmupMS = 15_000
	if _, err := Analyze(in); err == nil {
		t.Error("a warm-up that isn't whole buckets was accepted")
	}
	in = steadyRun(time.Millisecond, time.Millisecond)
	in.Jobs = nil
	if _, err := Analyze(in); err == nil {
		t.Error("a run with no jobs was accepted")
	}
	in = steadyRun(time.Millisecond, time.Millisecond)
	in.Config.Loadgen = nil
	if _, err := Analyze(in); err == nil {
		t.Error("a config without loadgen was accepted")
	}
}

// quantile interpolates between ranks like percentile_cont.
func TestQuantileMatchesPercentileCont(t *testing.T) {
	v := make([]float64, 100)
	for i := range v {
		v[i] = float64(100 - i) // unsorted on purpose
	}
	if got := quantile(v, 0.99); math.Abs(got-99.01) > 1e-9 {
		t.Errorf("p99 of 1..100 = %v, want 99.01", got)
	}
	if got := quantile(v, 0.5); got != 50.5 {
		t.Errorf("p50 of 1..100 = %v, want 50.5", got)
	}
	if v[0] != 100 {
		t.Error("quantile sorted its input in place")
	}
}

func effective(scheduler string, extra map[string]any) any {
	env := map[string]any{"QUEUED_SCHEDULER": scheduler, "PGPASSWORD": "<redacted>"}
	for k, v := range extra {
		env[k] = v
	}
	return map[string]any{"name": "jobqueue-bench", "services": map[string]any{
		"dispatcher": map[string]any{"environment": env},
		"worker-1":   map[string]any{"environment": map[string]any{"QUEUED_WORKER_SLOTS": "8"}},
	}}
}

func runFiles(t *testing.T, scheduler string, in Input) RunFiles {
	t.Helper()
	in.Scheduler = scheduler
	shops := map[string]*loadgen.ShopReport{}
	for _, s := range in.Config.Shops {
		shops[s.ID] = &loadgen.ShopReport{OfferedByPhase: map[loadgen.Phase]int{loadgen.Warmup: 200, loadgen.Spike: 200}}
	}
	return RunFiles{
		Run:       analyze(t, in),
		Loadgen:   loadgen.Report{Seed: 1, Drained: true, Shops: shops, EnqueueLatencyMS: loadgen.Percentiles{Count: 5, P50: 3, P99: 9}},
		Effective: effective(scheduler, nil),
	}
}

// P5-6: Compare passes only when the effective configs differ in the
// scheduler alone and both runs read the same bench file; NFR-9 is judged on
// the DRR run and NFR-11 and NFR-12 on both.
func TestCompare(t *testing.T) {
	good := steadyRun(5*time.Millisecond, 20*time.Millisecond)
	fifo, drr := runFiles(t, "fifo", good), runFiles(t, "drr", good)
	s := Compare(fifo, drr, map[string]any{"host_cpu": "test"}, t0)
	if !s.Pass || len(s.ConfigDiff) != 1 || s.ConfigDiff[0] != SchedulerSetting {
		t.Fatalf("summary = %+v, want a pass with only the scheduler differing", s)
	}

	// FIFO failing NFR-9 is reported, not targeted.
	fifo2 := runFiles(t, "fifo", steadyRun(5*time.Millisecond, 3*time.Second))
	if s := Compare(fifo2, drr, nil, t0); !s.Pass || fifo2.Run.NFR9.Pass {
		t.Errorf("a FIFO NFR-9 failure failed the report (or FIFO passed NFR-9): pass %v", s.Pass)
	}
	// DRR failing NFR-9 fails it.
	drr2 := runFiles(t, "drr", steadyRun(5*time.Millisecond, 3*time.Second))
	if s := Compare(fifo, drr2, nil, t0); s.Pass {
		t.Error("a DRR NFR-9 failure passed")
	}

	cases := map[string]func(f, d *RunFiles){
		"another setting differs": func(_, d *RunFiles) { d.Effective = effective("drr", map[string]any{"QUEUED_REFRESH_MS": "100"}) },
		"schedulers swapped": func(f, d *RunFiles) {
			f.Effective, d.Effective = effective("drr", nil), effective("fifo", nil)
		},
		"different bench file": func(_, d *RunFiles) { d.Run.ConfigSHA256 = "other" },
		"loadgen late":         func(_, d *RunFiles) { d.Loadgen.LateTicks = 3 },
		"not drained":          func(f, _ *RunFiles) { f.Loadgen.Drained = false },
		"enqueue errors":       func(_, d *RunFiles) { d.Loadgen.Shops["steady.test"].Errors = 5 },
		"different arrivals":   func(_, d *RunFiles) { d.Loadgen.Shops["steady.test"].OfferedByPhase[loadgen.Spike] = 199 },
		"different seed":       func(_, d *RunFiles) { d.Loadgen.Seed = 2 },
		"unfinished jobs":      func(_, d *RunFiles) { d.Run.Totals.NotTerminal = 1 },
		"NFR-11 in FIFO":       func(f, _ *RunFiles) { f.Run.NFR11.Pass = false },
		"NFR-12 in DRR":        func(_, d *RunFiles) { d.Run.NFR12.Pass = false },
	}
	for name, edit := range cases {
		f, d := runFiles(t, "fifo", good), runFiles(t, "drr", good)
		edit(&f, &d)
		if s := Compare(f, d, nil, t0); s.Pass {
			t.Errorf("%s: the report passed", name)
		}
	}
}

func TestDiffAndRedact(t *testing.T) {
	a := map[string]any{"x": 1.0, "y": map[string]any{"z": "a", "w": []any{1.0}}}
	b := map[string]any{"x": 1.0, "y": map[string]any{"z": "b", "w": []any{2.0}, "v": true}}
	if got := Diff(a, b); strings.Join(got, ",") != "y.v,y.w,y.z" {
		t.Errorf("Diff = %v", got)
	}
	doc := Redact(map[string]any{"services": map[string]any{"api": map[string]any{"environment": map[string]any{
		"PGPASSWORD": "dummy", "SHOPIFY_WEBHOOK_SECRET": "dummy", "PGUSER": "queue",
	}}, "list": []any{map[string]any{"POSTGRES_PASSWORD": "x"}}}})
	raw, _ := json.Marshal(doc)
	if strings.Contains(string(raw), "dummy") || strings.Contains(string(raw), `"x"`) || !strings.Contains(string(raw), `"queue"`) {
		t.Errorf("redacted = %s", raw)
	}
}

// The chart is well-formed SVG with both panels, a line per shop, a legend,
// and a break where a bucket had no arrivals.
func TestChart(t *testing.T) {
	in := steadyRun(5*time.Millisecond, 20*time.Millisecond)
	var kept []Job
	for _, j := range in.Jobs { // empty bucket 4 of the steady shop
		if j.Shop == "steady.test" && j.FirstRunnableAt.Sub(t0) >= 40*time.Second && j.FirstRunnableAt.Sub(t0) < 50*time.Second {
			continue
		}
		kept = append(kept, j)
	}
	in.Jobs = kept
	fifo, drr := runFiles(t, "fifo", in), runFiles(t, "drr", in)
	svg := string(Chart(fifo.Run, drr.Run))
	dec := xml.NewDecoder(strings.NewReader(svg))
	for {
		if _, err := dec.Token(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("not well-formed XML: %v", err)
		}
	}
	for _, want := range []string{"<title id=\"title\">", ">FIFO<", ">DRR (weighted deficit round robin)<", "shops 2–10", "sends the spike",
		"prefers-color-scheme: dark", ">spike<", ">1 ms<", ">100 ms<", "minutes since the first arrival"} {
		if !strings.Contains(svg, want) {
			t.Errorf("chart lacks %q", want)
		}
	}
	// Per panel: the steady shop is broken by the empty bucket, and the spike
	// shop has one line plus its ring.
	// Buckets 0–3 are one line; bucket 5, alone after the gap, is a dot.
	if lines, dots := strings.Count(svg, `<polyline class="line others"`), strings.Count(svg, `<circle class="line others"`); lines != 2 || dots != 2 {
		t.Errorf("%d steady-shop lines and %d dots, want one of each per panel, broken at the gap", lines, dots)
	}
	if n := strings.Count(svg, `<polyline class="line spiker"`); n != 2 {
		t.Errorf("%d spike-shop polylines, want 2", n)
	}
}

func TestMarkdown(t *testing.T) {
	good := steadyRun(5*time.Millisecond, 20*time.Millisecond)
	fifo, drr := runFiles(t, "fifo", good), runFiles(t, "drr", good)
	drr.Run.NFR12.Pass = false
	s := Compare(fifo, drr, map[string]any{"host_cpu": "test cpu"}, t0)
	md := Markdown(s, fifo.Run, drr.Run)
	for _, want := range []string{"**Overall: FAIL**", "| NFR-9: Black Friday fairness (DRR run) | PASS |", "| NFR-12: baseline keeps up (both runs) | FAIL |",
		"host_cpu: test cpu", "| steady.test | 5.0 | 110.0 | 20.0 | PASS | 20.0 |", "blackfriday.svg", "`" + SchedulerSetting + "`"} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q", want)
		}
	}
}
