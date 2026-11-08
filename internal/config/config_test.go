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
  "shops": [
    {"id": "shop-01.mockshop.test", "weight": 1, "max_concurrency": 4, "rate_per_sec": 50,
     "rate_burst": 50, "backlog_limit": 10000, "backlog_retry_after_ms": 5000}
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
}

// The committed config files must load: the Compose default and the test file.
func TestCommittedConfigFilesLoad(t *testing.T) {
	for _, path := range []string{"../../config/shops.json", "../../tests/testdata/shops.json"} {
		if _, err := config.Load(path); err != nil {
			t.Errorf("Load(%s): %v", path, err)
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
		"bad shop id":        {func(s string) string { return strings.Replace(s, `shop-01.mockshop.test`, `Shop 01`, 1) }, "id must be"},
		"empty shop id":      {func(s string) string { return strings.Replace(s, `shop-01.mockshop.test`, ``, 1) }, "id must be"},
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
