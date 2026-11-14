package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/loadgen"
)

// SchedulerSetting is the one path where the two runs' effective configs may
// differ (AC-9.3, AC-16.1).
const SchedulerSetting = "services.dispatcher.environment.QUEUED_SCHEDULER"

// RunFiles is everything one run left in its results directory.
type RunFiles struct {
	Run     Run
	Loadgen loadgen.Report
	// Effective is the run's effective Compose config, secrets redacted, as
	// generic JSON.
	Effective any
}

// Check is one PASS/FAIL line of the report.
type Check struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail"`
}

// Summary is summary.json: the targets, the validity checks, the machine and
// each run's numbers without its series (those stay in run.json).
type Summary struct {
	GeneratedAt time.Time          `json:"generated_at"`
	Machine     map[string]any     `json:"machine,omitempty"`
	Pass        bool               `json:"pass"`
	Targets     []Check            `json:"targets"`
	Validity    []Check            `json:"validity"`
	ConfigDiff  []string           `json:"config_diff"`
	Runs        map[string]RunLite `json:"runs"`
}

// RunLite is a run's summary without the per-bucket series.
type RunLite struct {
	Scheduler string         `json:"scheduler"`
	T0        time.Time      `json:"t0"`
	Totals    Totals         `json:"totals"`
	Shops     []ShopSummary  `json:"shops"`
	NFR9      NFR9           `json:"nfr9"`
	NFR11     NFR11          `json:"nfr11"`
	NFR12     NFR12          `json:"nfr12"`
	Loadgen   loadgen.Report `json:"loadgen"`
}

func lite(f RunFiles) RunLite {
	r := f.Run
	return RunLite{Scheduler: r.Scheduler, T0: r.T0, Totals: r.Totals, Shops: r.Shops, NFR9: r.NFR9,
		NFR11: r.NFR11, NFR12: r.NFR12, Loadgen: f.Loadgen}
}

// Compare checks the FIFO and DRR runs against the targets (requirements §3):
//
//   - P5-6: the runs' effective configs differ only in the scheduler, and both
//     ran from the same bench config file;
//   - NFR-9 in the DRR run (the FIFO run is reported, not targeted, D-027);
//   - NFR-11 and NFR-12 in both runs.
//
// The validity checks say whether the runs can be trusted: loadgen kept its
// schedule, both runs got the same warm-up and spike arrivals, every accepted
// job finished, and no enqueue failed.
func Compare(fifo, drr RunFiles, machine map[string]any, now time.Time) Summary {
	s := Summary{GeneratedAt: now.UTC(), Machine: machine, Runs: map[string]RunLite{"fifo": lite(fifo), "drr": lite(drr)}}

	s.ConfigDiff = Diff(fifo.Effective, drr.Effective)
	onlyScheduler := slices.Equal(s.ConfigDiff, []string{SchedulerSetting}) &&
		lookup(fifo.Effective, SchedulerSetting) == "fifo" && lookup(drr.Effective, SchedulerSetting) == "drr"
	sameFile := fifo.Run.ConfigSHA256 != "" && fifo.Run.ConfigSHA256 == drr.Run.ConfigSHA256
	s.Targets = append(s.Targets, Check{
		Name: "P5-6: the runs differ only in the scheduler", Pass: onlyScheduler && sameFile && fifo.Run.Scheduler == "fifo" && drr.Run.Scheduler == "drr",
		Detail: fmt.Sprintf("effective configs differ at %v; bench config sha256 %s vs %s", s.ConfigDiff, short(fifo.Run.ConfigSHA256), short(drr.Run.ConfigSHA256)),
	})
	s.Targets = append(s.Targets,
		Check{Name: "NFR-9: Black Friday fairness (DRR run)", Pass: drr.Run.NFR9.Pass, Detail: nfr9Detail(drr.Run.NFR9)},
		Check{Name: "NFR-11: one effect per job (both runs)", Pass: fifo.Run.NFR11.Pass && drr.Run.NFR11.Pass,
			Detail: fmt.Sprintf("FIFO: %s; DRR: %s", nfr11Detail(fifo.Run.NFR11), nfr11Detail(drr.Run.NFR11))},
		Check{Name: "NFR-12: baseline keeps up (both runs)", Pass: fifo.Run.NFR12.Pass && drr.Run.NFR12.Pass,
			Detail: fmt.Sprintf("limit %d jobs; worst FIFO %d, worst DRR %d", drr.Run.NFR12.Limit, worstBacklog(fifo.Run.NFR12), worstBacklog(drr.Run.NFR12))},
	)

	for _, f := range []RunFiles{fifo, drr} {
		name := strings.ToUpper(f.Run.Scheduler)
		lg := f.Loadgen
		errs := 0
		for _, sr := range lg.Shops {
			errs += sr.Errors
		}
		s.Validity = append(s.Validity,
			Check{Name: name + ": loadgen drained the spike's backlog", Pass: lg.Drained && !lg.Interrupted,
				Detail: fmt.Sprintf("drained %v, drain timed out %v, interrupted %v", lg.Drained, lg.DrainTimedOut, lg.Interrupted)},
			Check{Name: name + ": loadgen kept its schedule", Pass: lg.LateTicks == 0, Detail: fmt.Sprintf("%d late ticks", lg.LateTicks)},
			Check{Name: name + ": no enqueue failed", Pass: errs == 0, Detail: fmt.Sprintf("%d jobs in failed batches", errs)},
			Check{Name: name + ": every accepted job finished", Pass: f.Run.Totals.NotTerminal == 0,
				Detail: fmt.Sprintf("%d of %d jobs not completed or dead", f.Run.Totals.NotTerminal, f.Run.Totals.Jobs)},
		)
	}
	same, detail := sameArrivals(fifo.Loadgen, drr.Loadgen)
	s.Validity = append(s.Validity, Check{Name: "Both runs got the same warm-up and spike arrivals", Pass: same, Detail: detail})

	s.Pass = true
	for _, c := range slices.Concat(s.Targets, s.Validity) {
		s.Pass = s.Pass && c.Pass
	}
	return s
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

func nfr9Detail(n NFR9) string {
	over, worst := 0, ""
	worstRatio := -1.0
	for _, s := range n.Shops {
		over += s.Over
		if s.BoundMS > 0 && s.WorstP99MS/s.BoundMS > worstRatio {
			worstRatio = s.WorstP99MS / s.BoundMS
			worst = fmt.Sprintf("%s: worst bucket p99 %.1f ms against a bound of %.1f ms", s.ID, s.WorstP99MS, s.BoundMS)
		}
	}
	return fmt.Sprintf("%d spike buckets over their bound; closest: %s", over, worst)
}

func nfr11Detail(n NFR11) string {
	return fmt.Sprintf("%d completed, %d without exactly one mutation, %d keys applied more than once, %d mutations for no job",
		n.Completed, n.CompletedNotOne, n.KeysOverOne, n.UnknownMutations)
}

func worstBacklog(n NFR12) int {
	w := 0
	for _, s := range n.Shops {
		w = max(w, s.MaxBacklog)
	}
	return w
}

// sameArrivals reports whether both runs offered each shop the same jobs in
// the warm-up and the spike: the same seed must give the same schedule.
func sameArrivals(a, b loadgen.Report) (bool, string) {
	if a.Seed != b.Seed {
		return false, fmt.Sprintf("seeds differ: %d and %d", a.Seed, b.Seed)
	}
	var diffs []string
	for _, id := range slices.Sorted(maps.Keys(a.Shops)) {
		x, y := a.Shops[id], b.Shops[id]
		if y == nil {
			diffs = append(diffs, id+" missing from the DRR run")
			continue
		}
		for _, p := range []loadgen.Phase{loadgen.Warmup, loadgen.Spike} {
			if x.OfferedByPhase[p] != y.OfferedByPhase[p] {
				diffs = append(diffs, fmt.Sprintf("%s %s: %d vs %d", id, p, x.OfferedByPhase[p], y.OfferedByPhase[p]))
			}
		}
	}
	if len(a.Shops) == 0 || len(a.Shops) != len(b.Shops) {
		diffs = append(diffs, fmt.Sprintf("%d shops vs %d", len(a.Shops), len(b.Shops)))
	}
	if len(diffs) > 0 {
		return false, strings.Join(diffs, "; ")
	}
	return true, fmt.Sprintf("seed %d; identical per-shop counts in the warm-up and the spike", a.Seed)
}

// Diff returns the dotted paths where two JSON documents differ, sorted.
func Diff(a, b any) []string {
	var out []string
	var walk func(path string, x, y any)
	walk = func(path string, x, y any) {
		xm, xok := x.(map[string]any)
		ym, yok := y.(map[string]any)
		if xok && yok {
			keys := map[string]bool{}
			for k := range xm {
				keys[k] = true
			}
			for k := range ym {
				keys[k] = true
			}
			for _, k := range slices.Sorted(maps.Keys(keys)) {
				p := k
				if path != "" {
					p = path + "." + k
				}
				walk(p, xm[k], ym[k])
			}
			return
		}
		if !reflect.DeepEqual(x, y) {
			out = append(out, path)
		}
	}
	walk("", a, b)
	return out
}

// lookup returns the string at a dotted path, or "".
func lookup(doc any, path string) string {
	cur := doc
	for _, k := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[k]
	}
	s, _ := cur.(string)
	return s
}

// Redact replaces the value of every environment variable whose name says it
// holds a password or a secret, so a committed effective config holds none,
// even the dummy ones (design §15).
func Redact(doc any) any {
	switch v := doc.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, x := range v {
			up := strings.ToUpper(k)
			if _, isString := x.(string); isString && (strings.Contains(up, "PASSWORD") || strings.Contains(up, "SECRET")) {
				out[k] = "<redacted>"
				continue
			}
			out[k] = Redact(x)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = Redact(x)
		}
		return out
	default:
		return v
	}
}

// Markdown renders report.md.
func Markdown(s Summary, fifo, drr Run) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	verdict := func(ok bool) string {
		if ok {
			return "PASS"
		}
		return "FAIL"
	}
	w("# Black Friday: FIFO vs DRR\n\n")
	w("Written by `make bench` (design §17) on %s. Every number here comes from this directory's `run.json` and `summary.json`.\n\n", s.GeneratedAt.Format(time.RFC3339))
	w("**Overall: %s**\n\n", verdict(s.Pass))

	w("## Targets\n\n| Check | Result | Detail |\n|---|---|---|\n")
	for _, c := range s.Targets {
		w("| %s | %s | %s |\n", c.Name, verdict(c.Pass), escape(c.Detail))
	}
	w("\n## Validity\n\n| Check | Result | Detail |\n|---|---|---|\n")
	for _, c := range s.Validity {
		w("| %s | %s | %s |\n", c.Name, verdict(c.Pass), escape(c.Detail))
	}

	w("\n## Machine\n\n")
	if len(s.Machine) == 0 {
		w("Not recorded.\n")
	}
	for _, k := range slices.Sorted(maps.Keys(s.Machine)) {
		w("- %s: %v\n", k, s.Machine[k])
	}
	w("\nEverything shares one machine, including Docker Desktop's VM, so absolute numbers are specific to it (design §17).\n")

	w("\n## NFR-9: p99 queue wait in the spike, by shop\n\n")
	w("Each shop's worst 10 s bucket of the spike, bucketed by arrival (D-051), against its bound of 2 × its warm-up p99 + 100 ms (D-027). The target applies to DRR; FIFO is shown for comparison.\n\n")
	w("| Shop | Warm-up p99 (ms) | Bound (ms) | DRR worst bucket p99 (ms) | DRR | FIFO worst bucket p99 (ms) |\n|---|---|---|---|---|---|\n")
	fifoBy := map[string]NFR9Shop{}
	for _, n := range fifo.NFR9.Shops {
		fifoBy[n.ID] = n
	}
	for _, n := range drr.NFR9.Shops {
		w("| %s | %.1f | %.1f | %.1f | %s | %.1f |\n", n.ID, n.WarmupP99MS, n.BoundMS, n.WorstP99MS, verdict(n.Pass), fifoBy[n.ID].WorstP99MS)
	}

	w("\n## NFR-12: most jobs in a shop's backlog during the warm-up\n\nLimit: %d jobs (2 s of arrivals).\n\n| Shop | FIFO | DRR |\n|---|---|---|\n", drr.NFR12.Limit)
	fifo12 := map[string]int{}
	for _, n := range fifo.NFR12.Shops {
		fifo12[n.ID] = n.MaxBacklog
	}
	for _, n := range drr.NFR12.Shops {
		w("| %s | %d | %d |\n", n.ID, fifo12[n.ID], n.MaxBacklog)
	}

	w("\n## NFR-11: one effect per job\n\n| Run | Completed jobs | Without exactly one mutation | Keys applied more than once | Mutations for no job | Claims (runs) | Reruns | Mutations applied |\n|---|---|---|---|---|---|---|---|\n")
	for _, r := range []Run{fifo, drr} {
		w("| %s | %d | %d | %d | %d | %d | %d | %d |\n", strings.ToUpper(r.Scheduler), r.NFR11.Completed, r.NFR11.CompletedNotOne,
			r.NFR11.KeysOverOne, r.NFR11.UnknownMutations, r.Totals.Claims, r.Totals.ExtraClaims, r.Totals.MutationsApplied)
	}
	for _, r := range []Run{fifo, drr} {
		for _, e := range r.NFR11.Examples {
			w("- %s: %s\n", strings.ToUpper(r.Scheduler), escape(e))
		}
	}

	w("\n## Runs\n\nPublished numbers with no target (requirements §3).\n\n")
	w("| Run | Jobs | Completed | Dead | Shed (429) | Duration (s) | Completed per s | Queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range []Run{fifo, drr} {
		t := r.Totals
		w("| %s | %d | %d | %d | %d | %.1f | %.1f | %.1f / %.1f | %.1f / %.1f |\n", strings.ToUpper(r.Scheduler), t.Jobs, t.Completed, t.Dead,
			t.Shed, t.DurationS, t.Throughput, t.QueueWait.P50, t.QueueWait.P99, t.EndToEnd.P50, t.EndToEnd.P99)
	}
	for _, r := range []Run{fifo, drr} {
		w("\n### %s, per shop\n\n| Shop | Jobs | Shed (429) | Queue wait p50 / p99 (ms) | Spike queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |\n|---|---|---|---|---|---|\n", strings.ToUpper(r.Scheduler))
		for _, sh := range r.Shops {
			w("| %s | %d | %d | %.1f / %.1f | %.1f / %.1f | %.1f / %.1f |\n", sh.ID, sh.Jobs, sh.Shed, sh.QueueWait.P50, sh.QueueWait.P99,
				sh.SpikeWait.P50, sh.SpikeWait.P99, sh.EndToEnd.P50, sh.EndToEnd.P99)
		}
	}
	w("\n## Enqueue latency (loadgen's batch requests)\n\n| Run | Batches | p50 (ms) | p99 (ms) | Max (ms) |\n|---|---|---|---|---|\n")
	for _, name := range []string{"fifo", "drr"} {
		l := s.Runs[name].Loadgen.EnqueueLatencyMS
		w("| %s | %d | %.1f | %.1f | %.1f |\n", strings.ToUpper(name), l.Count, l.P50, l.P99, l.Max)
	}

	w("\n## Config diff\n\nThe runs' effective Compose configs (secrets redacted) differ at: `%s`.\n", strings.Join(s.ConfigDiff, "`, `"))
	w("\n## Chart\n\n![p99 queue wait per 10 s bucket, FIFO and DRR](blackfriday.svg)\n")
	return b.String()
}

func escape(s string) string { return strings.ReplaceAll(s, "|", "\\|") }

// MarshalIndent writes v as indented JSON with a trailing newline, without
// escaping <, > and &, so committed results stay readable.
func MarshalIndent(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
