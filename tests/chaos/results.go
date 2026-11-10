package chaos

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Batch summarizes one `make chaos`, `make chaos-one` or `make chaos-smoke`.
// It is written as summary.json and summary.md next to the run files.
type Batch struct {
	Started  time.Time       `json:"started"`
	Finished time.Time       `json:"finished"`
	BaseSeed *uint64         `json:"base_seed,omitempty"` // nil for a single replayed run
	Settings Settings        `json:"settings"`
	Runs     []RunSummary    `json:"runs"`
	Passed   int             `json:"passed"`
	Failed   int             `json:"failed"`
	Recovery *RecoveryResult `json:"recovery,omitempty"`
}

// Settings records the settings the runs used (design §2, §18).
type Settings struct {
	LeaseMS       int64 `json:"lease_ms"`
	HeartbeatMS   int64 `json:"heartbeat_ms"`
	FaultWindowMS int64 `json:"fault_window_ms"`
	RunTimeoutMS  int64 `json:"run_timeout_ms"`
	JobsPerRun    int   `json:"jobs_per_run"`
	Shops         int   `json:"shops"`
	MaxJobSleepMS int   `json:"max_job_sleep_ms"`
}

// RunSummary is one run's line in the batch summary.
type RunSummary struct {
	Seed       uint64   `json:"seed"`
	Passed     bool     `json:"passed"`
	DurationMS int64    `json:"duration_ms"`
	Completed  int      `json:"completed"`
	Dead       int      `json:"dead"`
	Faults     int      `json:"faults"`
	Failures   []string `json:"failures,omitempty"` // failed checks, or the harness error
	File       string   `json:"file"`
}

// Summarize returns the run's line in the batch summary.
func (r RunResult) Summarize(file string) RunSummary {
	s := RunSummary{Seed: r.Seed, Passed: r.Passed, DurationMS: r.DurationMS, Completed: r.Jobs.Completed,
		Dead: r.Jobs.Dead, Faults: len(r.Faults), File: file}
	if r.Error != "" {
		s.Failures = append(s.Failures, r.Error)
	}
	for _, c := range r.Checks {
		if !c.Passed {
			s.Failures = append(s.Failures, c.Name)
		}
	}
	return s
}

// Seeds returns the seeds to run (D-046): CHAOS_SEED alone if set, otherwise
// CHAOS_RUNS seeds (defaultRuns if unset) counting up from CHAOS_BASE_SEED, or
// from a fresh random base seed. base is nil for a single replayed seed.
func Seeds(getenv func(string) string, defaultRuns int) (seeds []uint64, base *uint64, err error) {
	if s := getenv("CHAOS_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("chaos: CHAOS_SEED must be a whole number, got %q", s)
		}
		return []uint64{seed}, nil, nil
	}
	runs := defaultRuns
	if s := getenv("CHAOS_RUNS"); s != "" {
		if runs, err = strconv.Atoi(s); err != nil || runs < 1 {
			return nil, nil, fmt.Errorf("chaos: CHAOS_RUNS must be a positive number, got %q", s)
		}
	}
	var b uint64
	if s := getenv("CHAOS_BASE_SEED"); s != "" {
		if b, err = strconv.ParseUint(s, 10, 64); err != nil {
			return nil, nil, fmt.Errorf("chaos: CHAOS_BASE_SEED must be a whole number, got %q", s)
		}
	} else {
		var raw [8]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, nil, err
		}
		// Short enough to type back into CHAOS_SEED.
		b = 1 + binary.LittleEndian.Uint64(raw[:])%1_000_000_000
	}
	for i := range runs {
		seeds = append(seeds, b+uint64(i))
	}
	return seeds, &b, nil
}

// WriteJSON writes v, indented, to path, creating its directory.
func WriteJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

// WriteMarkdown writes the batch summary as a small Markdown report.
func (b *Batch) WriteMarkdown(path string) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Chaos runs, %s\n\n", b.Started.Format(time.RFC3339))
	fmt.Fprintf(&sb, "Written by `tests/chaos` (design §18). Each run's file has its fault schedule and check details.\n\n")
	if b.BaseSeed != nil {
		fmt.Fprintf(&sb, "- Base seed: %d (`CHAOS_BASE_SEED=%d make chaos` repeats this batch)\n", *b.BaseSeed, *b.BaseSeed)
	}
	fmt.Fprintf(&sb, "- Runs: %d passed, %d failed\n", b.Passed, b.Failed)
	fmt.Fprintf(&sb, "- Settings: %d jobs over %d shops per run, lease %d ms, heartbeat %d ms, fault window %d ms, run timeout %d ms\n",
		b.Settings.JobsPerRun, b.Settings.Shops, b.Settings.LeaseMS, b.Settings.HeartbeatMS, b.Settings.FaultWindowMS, b.Settings.RunTimeoutMS)
	if r := b.Recovery; r != nil {
		verdict := "PASS"
		if !r.Passed {
			verdict = "FAIL"
		}
		fmt.Fprintf(&sb, "- Crash recovery (NFR-10): %s. %d orphaned jobs of %s were claimed again %d ms after the kill; target %d ms (lease %d ms + reaper scan %d ms)",
			verdict, r.Orphans, r.Killed, r.RecoveryMS, r.TargetMS, r.LeaseMS, r.ReaperScanMS)
		if r.Error != "" {
			fmt.Fprintf(&sb, ". Error: %s", r.Error)
		}
		sb.WriteString("\n")
	}
	sb.WriteString("\n| Seed | Result | Duration (ms) | Completed | Dead | Faults | Failures |\n|---|---|---|---|---|---|---|\n")
	for _, r := range b.Runs {
		verdict := "PASS"
		if !r.Passed {
			verdict = "FAIL"
		}
		fmt.Fprintf(&sb, "| %d | %s | %d | %d | %d | %d | %s |\n", r.Seed, verdict, r.DurationMS, r.Completed, r.Dead, r.Faults,
			strings.ReplaceAll(strings.Join(r.Failures, "; "), "|", "/"))
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(sb.String()), 0o600)
}
