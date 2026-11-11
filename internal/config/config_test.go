package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
)

const valid = `{
  "dedupe_window_ms": 604800000,
  "mockshop": {
    "retry_horizon_ms": 3600000, "retry_base_ms": 1000, "retry_cap_ms": 60000, "delivery_timeout_ms": 5000,
    "duplicate_probability": 0.05, "duplicate_delay_max_ms": 10000, "concurrent_duplicate_probability": 0.02
  },
  "loadgen": {
    "seed": 7, "tick_ms": 100, "rate_per_shop": 20, "warmup_ms": 120000,
    "spike_shop": "shop-01.mockshop.test", "spike_rate": 2000, "spike_ms": 300000, "drain_max_ms": 1800000
  },
  "shops": [
    {"id": "shop-01.mockshop.test", "weight": 1, "max_concurrency": 4, "rate_per_sec": 50,
     "rate_burst": 50, "backlog_limit": 10000, "backlog_retry_after_ms": 5000,
     "mock_api_capacity": 40, "mock_api_leak_per_sec": 2, "webhooks_per_sec": 1}
  ],
  "kinds": [
    {"kind": "order.tag", "cost": 1, "lease_ms": 30000, "heartbeat_ms": 10000, "timeout_ms": 300000,
     "max_attempts": 10, "max_throttles": 50, "handler_sleep_ms": 30},
    {"kind": "test.sleep", "cost": 2, "lease_ms": 2000, "heartbeat_ms": 500, "timeout_ms": 1000,
     "max_attempts": 3, "max_throttles": 0}
  ]
}`

func TestParseValid(t *testing.T) {
	cfg, err := config.Parse(strings.NewReader(valid))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Shops) != 1 || cfg.Shops[0].ID != "shop-01.mockshop.test" || cfg.Shops[0].RatePerSec != 50 {
		t.Errorf("shops = %+v", cfg.Shops)
	}
	if len(cfg.Kinds) != 2 || cfg.Kinds[0].HandlerSleepMS != 30 || cfg.Kinds[1].Cost != 2 {
		t.Errorf("kinds = %+v", cfg.Kinds)
	}
	if got := cfg.MinHeartbeat(); got != 500*time.Millisecond {
		t.Errorf("MinHeartbeat = %v, want 500ms", got)
	}
	if got := cfg.DedupeWindow(); got != 7*24*time.Hour {
		t.Errorf("DedupeWindow = %v, want 7 days", got)
	}
	if s := cfg.Shops[0]; s.MockAPICapacity != 40 || s.MockAPILeakPerSec != 2 || s.WebhooksPerSec != 1 {
		t.Errorf("mockshop shop settings = %+v", s)
	}
	if m := cfg.Mockshop; m.RetryHorizonMS != 3600000 || m.DuplicateProbability != 0.05 || m.ConcurrentDuplicateProbability != 0.02 {
		t.Errorf("mockshop = %+v", m)
	}
	if l := cfg.Loadgen; l == nil || l.Seed != 7 || l.SpikeShop != "shop-01.mockshop.test" || l.SpikeRate != 2000 {
		t.Errorf("loadgen = %+v", l)
	}
	if _, ok := cfg.Shop("shop-01.mockshop.test"); !ok {
		t.Error("Shop did not find shop-01")
	}
	if k, ok := cfg.Kind("test.sleep"); !ok || k.Cost != 2 {
		t.Errorf("Kind(test.sleep) = %+v, %v", k, ok)
	}
}

// The loadgen section is optional: only bench files have it (D-050).
func TestLoadgenSectionIsOptional(t *testing.T) {
	if _, err := config.Parse(strings.NewReader(replaceBetween(valid, `"loadgen": {`, `},`+"\n", ""))); err == nil {
		t.Fatal("a loadgen section emptied of its settings was accepted")
	}
	withoutLoadgen := valid[:strings.Index(valid, `  "loadgen"`)] + valid[strings.Index(valid, `  "shops"`):]
	cfg, err := config.Parse(strings.NewReader(withoutLoadgen))
	if err != nil {
		t.Fatalf("Parse without loadgen: %v", err)
	}
	if cfg.Loadgen != nil {
		t.Errorf("Loadgen = %+v, want nil", cfg.Loadgen)
	}
}

// The committed config files must load: the Compose default, the test and
// chaos files, and the bench file, which alone has a loadgen section.
func TestCommittedConfigFilesLoad(t *testing.T) {
	for _, path := range []string{"../../config/shops.json", "../../tests/testdata/shops.json",
		"../../tests/chaos/shops.json", "../../bench/blackfriday.json"} {
		cfg, err := config.Load(path)
		if err != nil {
			t.Errorf("Load(%s): %v", path, err)
			continue
		}
		if wantLoadgen := strings.HasPrefix(path, "../../bench/"); (cfg.Loadgen != nil) != wantLoadgen {
			t.Errorf("%s: has a loadgen section = %v, want %v", path, cfg.Loadgen != nil, wantLoadgen)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := config.Load(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("Load of a missing file returned nil error")
	}
}

func TestLoadRejectsOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.json")
	if err := os.WriteFile(path, []byte(`{"shops": [], "kinds": [], "x": "`+strings.Repeat("a", 1<<20)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(path); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("Load = %v, want a size error", err)
	}
}

// Every invalid setting fails fast, and the error names the problem (design §11.4).
func TestParseRejects(t *testing.T) {
	tests := map[string]struct {
		edit    func(string) string
		wantErr string
	}{
		"malformed JSON":     {func(s string) string { return s[:len(s)-2] }, "parse"},
		"trailing data":      {func(s string) string { return s + ` {}` }, "unexpected data"},
		"unknown field":      {func(s string) string { return strings.Replace(s, `"weight": 1`, `"wieght": 1`, 1) }, "unknown field"},
		"no shops":           {func(s string) string { return replaceBetween(s, `"shops": [`, `],`, "") }, "no shops"},
		"zero weight":        {func(s string) string { return strings.Replace(s, `"weight": 1`, `"weight": 0`, 1) }, "weight"},
		"negative cap":       {func(s string) string { return strings.Replace(s, `"max_concurrency": 4`, `"max_concurrency": -1`, 1) }, "max_concurrency"},
		"zero rate":          {func(s string) string { return strings.Replace(s, `"rate_per_sec": 50`, `"rate_per_sec": 0`, 1) }, "rate_per_sec"},
		"zero burst":         {func(s string) string { return strings.Replace(s, `"rate_burst": 50`, `"rate_burst": 0`, 1) }, "rate_burst"},
		"zero backlog limit": {func(s string) string { return strings.Replace(s, `"backlog_limit": 10000`, `"backlog_limit": 0`, 1) }, "backlog_limit"},
		"zero retry after": {func(s string) string {
			return strings.Replace(s, `"backlog_retry_after_ms": 5000`, `"backlog_retry_after_ms": 0`, 1)
		}, "backlog_retry_after_ms"},
		"bad shop id": {func(s string) string {
			return strings.Replace(s, `"id": "shop-01.mockshop.test"`, `"id": "Shop 01"`, 1)
		}, "id must be"},
		"empty shop id":      {func(s string) string { return strings.Replace(s, `"id": "shop-01.mockshop.test"`, `"id": ""`, 1) }, "id must be"},
		"duplicate shop":     {func(s string) string { return duplicateFirst(s, "shops") }, "duplicate id"},
		"no kinds":           {func(s string) string { return replaceBetween(s, `"kinds": [`, `]`+"\n}", "") }, "no kinds"},
		"bad kind name":      {func(s string) string { return strings.Replace(s, `"order.tag"`, `"Order Tag"`, 1) }, "kind must be"},
		"duplicate kind":     {func(s string) string { return strings.Replace(s, `"test.sleep"`, `"order.tag"`, 1) }, "duplicate kind"},
		"zero cost":          {func(s string) string { return strings.Replace(s, `"cost": 1`, `"cost": 0`, 1) }, "cost"},
		"heartbeat == lease": {func(s string) string { return strings.Replace(s, `"heartbeat_ms": 10000`, `"heartbeat_ms": 30000`, 1) }, "shorter than lease_ms"},
		"heartbeat > lease":  {func(s string) string { return strings.Replace(s, `"heartbeat_ms": 500`, `"heartbeat_ms": 5000`, 1) }, "shorter than lease_ms"},
		"zero heartbeat":     {func(s string) string { return strings.Replace(s, `"heartbeat_ms": 500`, `"heartbeat_ms": 0`, 1) }, "heartbeat_ms"},
		"zero timeout":       {func(s string) string { return strings.Replace(s, `"timeout_ms": 1000`, `"timeout_ms": 0`, 1) }, "timeout_ms"},
		"zero max attempts":  {func(s string) string { return strings.Replace(s, `"max_attempts": 3`, `"max_attempts": 0`, 1) }, "max_attempts"},
		"negative throttles": {func(s string) string { return strings.Replace(s, `"max_throttles": 0`, `"max_throttles": -1`, 1) }, "max_throttles"},
		"negative sleep": {func(s string) string {
			return strings.Replace(s, `"handler_sleep_ms": 30`, `"handler_sleep_ms": -1`, 1)
		}, "handler_sleep_ms"},
		"lease beyond int32":   {func(s string) string { return strings.Replace(s, `"lease_ms": 30000`, `"lease_ms": 3000000000`, 1) }, "lease_ms"},
		"fractional cost":      {func(s string) string { return strings.Replace(s, `"cost": 1`, `"cost": 1.5`, 1) }, "parse"},
		"string where number":  {func(s string) string { return strings.Replace(s, `"weight": 1`, `"weight": "1"`, 1) }, "parse"},
		"top level not object": {func(string) string { return `[]` }, "parse"},
		// D-050: the dedupe window, mockshop's settings and loadgen's.
		"zero dedupe window": {func(s string) string {
			return strings.Replace(s, `"dedupe_window_ms": 604800000`, `"dedupe_window_ms": 0`, 1)
		}, "dedupe_window_ms"},
		"zero mock capacity": {func(s string) string {
			return strings.Replace(s, `"mock_api_capacity": 40`, `"mock_api_capacity": 0`, 1)
		}, "mock_api_capacity"},
		"zero mock leak": {func(s string) string {
			return strings.Replace(s, `"mock_api_leak_per_sec": 2`, `"mock_api_leak_per_sec": 0`, 1)
		}, "mock_api_leak_per_sec"},
		"negative webhook rate": {func(s string) string {
			return strings.Replace(s, `"webhooks_per_sec": 1`, `"webhooks_per_sec": -1`, 1)
		}, "webhooks_per_sec"},
		// D-018: every redelivery must land inside the dedupe window.
		"horizon == window": {func(s string) string {
			return strings.Replace(s, `"retry_horizon_ms": 3600000`, `"retry_horizon_ms": 604800000`, 1)
		}, "shorter than dedupe_window_ms"},
		"horizon > window": {func(s string) string {
			return strings.Replace(s, `"dedupe_window_ms": 604800000`, `"dedupe_window_ms": 60000`, 1)
		}, "shorter than dedupe_window_ms"},
		"zero horizon": {func(s string) string {
			return strings.Replace(s, `"retry_horizon_ms": 3600000`, `"retry_horizon_ms": 0`, 1)
		}, "retry_horizon_ms"},
		"no mockshop section": {func(s string) string {
			return replaceBetween(s, `"mockshop": {`, `},`+"\n", "")
		}, "retry_horizon_ms"},
		"cap below base": {func(s string) string {
			return strings.Replace(s, `"retry_cap_ms": 60000`, `"retry_cap_ms": 10`, 1)
		}, "retry_cap_ms"},
		"zero delivery timeout": {func(s string) string {
			return strings.Replace(s, `"delivery_timeout_ms": 5000`, `"delivery_timeout_ms": 0`, 1)
		}, "delivery_timeout_ms"},
		"probability above 1": {func(s string) string {
			return strings.Replace(s, `"duplicate_probability": 0.05`, `"duplicate_probability": 1.5`, 1)
		}, "duplicate_probability"},
		"negative probability": {func(s string) string {
			return strings.Replace(s, `"concurrent_duplicate_probability": 0.02`, `"concurrent_duplicate_probability": -0.1`, 1)
		}, "concurrent_duplicate_probability"},
		"negative duplicate delay": {func(s string) string {
			return strings.Replace(s, `"duplicate_delay_max_ms": 10000`, `"duplicate_delay_max_ms": -1`, 1)
		}, "duplicate_delay_max_ms"},
		"zero tick":        {func(s string) string { return strings.Replace(s, `"tick_ms": 100`, `"tick_ms": 0`, 1) }, "tick_ms"},
		"zero steady rate": {func(s string) string { return strings.Replace(s, `"rate_per_shop": 20`, `"rate_per_shop": 0`, 1) }, "rate_per_shop"},
		"zero warm-up":     {func(s string) string { return strings.Replace(s, `"warmup_ms": 120000`, `"warmup_ms": 0`, 1) }, "warmup_ms"},
		"unknown spike shop": {func(s string) string {
			return strings.Replace(s, `"spike_shop": "shop-01.mockshop.test"`, `"spike_shop": "nope.test"`, 1)
		}, "spike_shop"},
		"zero spike rate":   {func(s string) string { return strings.Replace(s, `"spike_rate": 2000`, `"spike_rate": 0`, 1) }, "spike_rate"},
		"zero spike length": {func(s string) string { return strings.Replace(s, `"spike_ms": 300000`, `"spike_ms": 0`, 1) }, "spike_ms"},
		"zero drain cap": {func(s string) string {
			return strings.Replace(s, `"drain_max_ms": 1800000`, `"drain_max_ms": 0`, 1)
		}, "drain_max_ms"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			input := tt.edit(valid)
			if input == valid {
				t.Fatal("the edit changed nothing; fix the test")
			}
			_, err := config.Parse(strings.NewReader(input))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Parse = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}

// Validate reports every problem, not just the first.
func TestValidateReportsAllProblems(t *testing.T) {
	input := strings.Replace(strings.Replace(valid, `"weight": 1`, `"weight": 0`, 1), `"cost": 1`, `"cost": 0`, 1)
	_, err := config.Parse(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "weight") || !strings.Contains(err.Error(), "cost") {
		t.Fatalf("Parse = %v, want errors naming both weight and cost", err)
	}
}

func replaceBetween(s, start, end, with string) string {
	i := strings.Index(s, start)
	j := strings.Index(s[i+len(start):], end)
	return s[:i+len(start)] + with + s[i+len(start)+j:]
}

// duplicateFirst repeats the first element of the named array.
func duplicateFirst(s, array string) string {
	start := strings.Index(s, `"`+array+`": [`) + len(array) + 5
	end := start + strings.Index(s[start:], "}") + 1
	elem := strings.TrimSpace(s[start:end])
	return s[:end] + ", " + elem + s[end:]
}

// The DRR base quantum is the largest job cost in the file (design §2).
func TestMaxCost(t *testing.T) {
	t.Parallel()
	cfg := &config.File{Kinds: []config.Kind{{Kind: "a", Cost: 2}, {Kind: "b", Cost: 7}, {Kind: "c", Cost: 3}}}
	if got := cfg.MaxCost(); got != 7 {
		t.Fatalf("MaxCost = %d, want 7", got)
	}
	if got := (&config.File{}).MaxCost(); got != 1 {
		t.Fatalf("MaxCost with no kinds = %d, want 1", got)
	}
}
