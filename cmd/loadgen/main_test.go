package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/loadgen"
)

func env(vars map[string]string) func(string) string { return func(k string) string { return vars[k] } }

// loadgen runs the config file's loadgen section against the api and prints
// its report as JSON on stdout.
func TestRunsAndPrintsTheReport(t *testing.T) {
	cfg, err := config.Load("../../bench/blackfriday.json")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Shops = cfg.Shops[:2]
	cfg.Loadgen.SpikeShop = cfg.Shops[0].ID
	cfg.Loadgen.TickMS, cfg.Loadgen.WarmupMS, cfg.Loadgen.SpikeMS, cfg.Loadgen.DrainMaxMS = 10, 50, 50, 1000
	cfg.Loadgen.RatePerShop, cfg.Loadgen.SpikeRate = 200, 2000
	path := filepath.Join(t.TempDir(), "bench.json")
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var next atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"state": "completed"}`))
			return
		}
		var req struct{ Jobs []json.RawMessage }
		_ = json.NewDecoder(r.Body).Decode(&req)
		ids := make([]int64, len(req.Jobs))
		for i := range ids {
			ids[i] = next.Add(1)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"ids": ids})
	}))
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	if code := run(t.Context(), nil, env(map[string]string{"LOADGEN_CONFIG": path, "LOADGEN_API_URL": srv.URL}), &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	var rep loadgen.Report
	if err := json.Unmarshal(stdout.Bytes(), &rep); err != nil {
		t.Fatalf("stdout is not a report: %v\n%s", err, stdout.String())
	}
	if !rep.Drained || len(rep.Shops) != 2 || rep.Seed != cfg.Loadgen.Seed {
		t.Errorf("report = %+v", rep)
	}
	for id, sr := range rep.Shops {
		if sr.Offered == 0 || sr.Accepted != sr.Offered {
			t.Errorf("%s: %+v", id, sr)
		}
	}
}

func TestFailsFast(t *testing.T) {
	for name, vars := range map[string]map[string]string{
		"missing file": {"LOADGEN_CONFIG": filepath.Join(t.TempDir(), "absent.json")},
		"no loadgen":   {"LOADGEN_CONFIG": "../../config/shops.json"},
		"invalid":      {"LOADGEN_CONFIG": "../../go.mod"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(t.Context(), nil, env(vars), &stdout, &stderr); code != 1 || stdout.Len() != 0 {
			t.Errorf("%s: exit %d, stdout %q", name, code, stdout.String())
		}
	}
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"extra"}, env(nil), &bytes.Buffer{}, &stderr); code != 2 || !strings.Contains(stderr.String(), "usage") {
		t.Errorf("usage error: exit %d, %q", code, stderr.String())
	}
}
