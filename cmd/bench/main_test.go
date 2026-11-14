package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/bench"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/loadgen"
)

func env(vars map[string]string) func(string) string { return func(k string) string { return vars[k] } }

// writeRun writes a minimal run directory for report.
func writeRun(t *testing.T, dir, scheduler string, r bench.Run) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	shops := map[string]*loadgen.ShopReport{}
	for _, s := range r.Shops {
		shops[s.ID] = &loadgen.ShopReport{OfferedByPhase: map[loadgen.Phase]int{loadgen.Warmup: 10, loadgen.Spike: 10}}
	}
	files := map[string]any{
		"run.json":     r,
		"loadgen.json": loadgen.Report{Seed: 1, Drained: true, Shops: shops},
		"effective-config.json": map[string]any{"services": map[string]any{
			"dispatcher": map[string]any{"environment": map[string]any{"QUEUED_SCHEDULER": scheduler}}}},
	}
	for name, v := range files {
		if err := writeJSON(filepath.Join(dir, name), v); err != nil {
			t.Fatal(err)
		}
	}
}

func sampleRun(t *testing.T, scheduler string) bench.Run {
	t.Helper()
	cfg, err := config.Load("../../bench/blackfriday.json")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Shops = cfg.Shops[:2]
	cfg.Loadgen.SpikeShop = cfg.Shops[0].ID
	t0 := time.Date(2026, 12, 21, 10, 0, 0, 0, time.UTC)
	var jobs []bench.Job
	mut := map[string]map[string]int{}
	for i := range 4200 { // 420 s of 10 jobs/s for each shop
		for k, s := range cfg.Shops {
			id := int64(2*i + k + 1)
			at := t0.Add(time.Duration(i) * 100 * time.Millisecond)
			claimed, done := at.Add(2*time.Millisecond), at.Add(40*time.Millisecond)
			jobs = append(jobs, bench.Job{ID: id, Shop: s.ID, State: "completed", EnqueuedAt: at, FirstRunnableAt: at,
				FirstClaimedAt: &claimed, TerminalAt: &done, ClaimSeq: 1})
			if mut[s.ID] == nil {
				mut[s.ID] = map[string]int{}
			}
			mut[s.ID][keyOf(id)] = 1
		}
	}
	r, err := bench.Analyze(bench.Input{Scheduler: scheduler, ConfigSHA256: "same", Config: cfg, Jobs: jobs,
		Mutations: mut, Replays: map[string]int{}, Shed: map[string]int64{}, MutationKey: keyOf})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func keyOf(id int64) string { return "job-" + jsonInt(id) + "-tag" }

func jsonInt(id int64) string { b, _ := json.Marshal(id); return string(b) }

// report reads both runs and writes summary.json, report.md and
// blackfriday.svg; it exits 0 when everything passes and 1 when anything fails.
func TestReportWritesTheOutputs(t *testing.T) {
	dir := t.TempDir()
	writeRun(t, filepath.Join(dir, "fifo"), "fifo", sampleRun(t, "fifo"))
	writeRun(t, filepath.Join(dir, "drr"), "drr", sampleRun(t, "drr"))
	var stdout, stderr bytes.Buffer
	if code := run(t.Context(), []string{"report", "-dir", dir}, env(nil), &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	for _, f := range []string{"summary.json", "report.md", "blackfriday.svg"} {
		if info, err := os.Stat(filepath.Join(dir, f)); err != nil || info.Size() == 0 {
			t.Errorf("%s: %v", f, err)
		}
	}
	if !strings.Contains(stdout.String(), "PASS  NFR-9") {
		t.Errorf("stdout = %s", stdout.String())
	}

	// A config that differs in more than the scheduler fails, with exit 1.
	writeRun(t, filepath.Join(dir, "drr"), "fifo", sampleRun(t, "drr"))
	stdout.Reset()
	if code := run(t.Context(), []string{"report", "-dir", dir}, env(nil), &stdout, &stderr); code != 1 {
		t.Errorf("exit %d, want 1 when the configs don't differ by the scheduler alone", code)
	}
	if !strings.Contains(stdout.String(), "FAIL  P5-6") {
		t.Errorf("stdout = %s", stdout.String())
	}
}

// collect redacts every password and secret in the effective config it saves.
func TestWriteEffectiveRedacts(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "compose.json")
	if err := os.WriteFile(in, []byte(`{"services":{"api":{"environment":{"PGPASSWORD":"pw","SHOPIFY_WEBHOOK_SECRET":"s","PGUSER":"queue"}}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "effective.json")
	if err := writeEffective(in, out); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(out) //nolint:gosec // the test's own temp file
	if strings.Contains(string(raw), `"pw"`) || strings.Contains(string(raw), `"s"`) || !strings.Contains(string(raw), "<redacted>") {
		t.Errorf("effective config = %s", raw)
	}
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"bogus"}} {
		if code := run(t.Context(), args, env(nil), &bytes.Buffer{}, &bytes.Buffer{}); code != 2 {
			t.Errorf("run(%q) = %d, want 2", args, code)
		}
	}
	if code := run(t.Context(), []string{"collect"}, env(nil), &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Errorf("collect without flags = %d, want 1", code)
	}
	if code := run(t.Context(), []string{"report"}, env(nil), &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Errorf("report without -dir = %d, want 1", code)
	}
}
