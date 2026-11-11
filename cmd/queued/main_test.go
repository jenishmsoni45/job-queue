package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/httpserver"
	"github.com/jenishmsoni45/job-queue/internal/pgtest"
	"github.com/jenishmsoni45/job-queue/internal/scheduler"
	"github.com/jenishmsoni45/job-queue/migrations"
)

const (
	testConfig = "../../tests/testdata/shops.json"
	testSecret = "test-webhook-secret" //nolint:gosec // a dummy test secret, not a credential
)

// freeAddr returns a loopback address with a port that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

// roleEnv is the environment a role needs to start against db.
func roleEnv(db *pgtest.DB, extra map[string]string) map[string]string {
	vars := map[string]string{
		"QUEUED_CONFIG":           testConfig,
		"PGDATABASE":              db.Name,
		"QUEUED_DISPATCHER_URL":   "http://127.0.0.1:1", // nothing listens; tests that need one set it
		"QUEUED_REFRESH_MS":       "50",
		"QUEUED_CLAIM_TIMEOUT_MS": "1000",
		"QUEUED_REAPER_SCAN_MS":   "100",
		"QUEUED_BACKOFF_BASE_MS":  "10",
		"QUEUED_BACKOFF_CAP_MS":   "100",
		"QUEUED_WORKER_SLOTS":     "2",
		"SHOPIFY_WEBHOOK_SECRET":  testSecret,
	}
	maps.Copy(vars, extra)
	return vars
}

// lockedBuffer collects a role's log output across goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startRole runs `queued <role>` in the background with vars and waits until
// its healthcheck command passes. It stops the role when the test ends and
// checks that it exited cleanly.
func startRole(t *testing.T, role string, vars map[string]string) string {
	t.Helper()
	addr := freeAddr(t)
	vars = maps.Clone(vars)
	vars["QUEUED_HTTP_ADDR"] = addr
	getenv := env(vars)
	logs := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan int, 1)
	go func() { exited <- run(ctx, []string{role}, getenv, logs) }()
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-exited:
			if code != 0 {
				t.Errorf("queued %s exited with %d after shutdown:\n%s", role, code, logs)
			}
		case <-time.After(15 * time.Second):
			t.Errorf("queued %s did not stop", role)
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		if code := run(t.Context(), []string{"healthcheck"}, getenv, io.Discard); code == 0 {
			return addr
		}
		select {
		case code := <-exited:
			t.Fatalf("queued %s exited early with %d:\n%s", role, code, logs)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued %s never became healthy:\n%s", role, logs)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// P1-7: each queued role answers GET /healthz with 200.
func TestEachRoleServesHealthz(t *testing.T) {
	db := pgtest.NewMigrated(t)
	for _, role := range []string{"api", "dispatcher", "worker"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			addr := startRole(t, role, roleEnv(db, nil))
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/healthz", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET /healthz: status %d, want 200", resp.StatusCode)
			}
			var body struct{ Status, Service string }
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if want := "queued " + role; body.Service != want || body.Status != "ok" {
				t.Errorf("body = %+v, want status ok and service %q", body, want)
			}
		})
	}
}

// End to end through the three roles, in process: a job posted to api is
// claimed by the dispatcher for a worker, which runs a test handler (enabled
// by QUEUED_TEST_HANDLERS=1) and writes its fenced result.
func TestRolesRunAJobEndToEnd(t *testing.T) {
	db := pgtest.NewMigrated(t)
	apiAddr := startRole(t, "api", roleEnv(db, nil))
	dispatcherAddr := startRole(t, "dispatcher", roleEnv(db, nil))
	startRole(t, "worker", roleEnv(db, map[string]string{
		"QUEUED_DISPATCHER_URL": "http://" + dispatcherAddr,
		"QUEUED_TEST_HANDLERS":  "1",
	}))

	for _, tt := range []struct {
		body, state string
	}{
		{`{"shop": "shop-a.test", "kind": "test.sleep", "payload": {"ms": 20}}`, "completed"},
		{`{"shop": "shop-b.test", "kind": "test.fail", "payload": {"fail_times": 1}}`, "completed"},
		{`{"shop": "shop-c.test", "kind": "test.fail", "payload": {"fail_times": -1, "permanent": true}}`, "dead"},
	} {
		var created struct{ ID int64 }
		if status := call(t, http.MethodPost, "http://"+apiAddr+"/v1/jobs", tt.body, &created); status != http.StatusCreated {
			t.Fatalf("POST %s: status %d", tt.body, status)
		}
		var job struct {
			State    string
			Attempts int
		}
		deadline := time.Now().Add(15 * time.Second)
		for job.State != tt.state {
			if time.Now().After(deadline) {
				t.Fatalf("job %d is %q after 15s, want %q", created.ID, job.State, tt.state)
			}
			time.Sleep(20 * time.Millisecond)
			if status := call(t, http.MethodGet, fmt.Sprintf("http://%s/v1/jobs/%d", apiAddr, created.ID), "", &job); status != http.StatusOK {
				t.Fatalf("GET job %d: status %d", created.ID, status)
			}
		}
	}
}

func call(t *testing.T, method, url, body string, out any) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && resp.StatusCode < 300 {
		t.Fatalf("%s %s: decode: %v", method, url, err)
	}
	return resp.StatusCode
}

// The api role refuses to start without the webhook secret, since it could
// check no webhook's HMAC (AC-8.1).
func TestAPIRoleNeedsTheWebhookSecret(t *testing.T) {
	db := pgtest.NewMigrated(t)
	vars := roleEnv(db, map[string]string{"SHOPIFY_WEBHOOK_SECRET": "", "QUEUED_HTTP_ADDR": freeAddr(t)})
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"api"}, env(vars), &stderr); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "SHOPIFY_WEBHOOK_SECRET") {
		t.Errorf("stderr = %q, want it to name the missing secret", stderr.String())
	}
}

// Design §11.4: every role fails fast on a bad config file or setting.
func TestRolesFailFastOnBadConfig(t *testing.T) {
	db := pgtest.NewMigrated(t)
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`{"shops": [], "kinds": []}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, vars := range map[string]map[string]string{
		"missing config file": {"QUEUED_CONFIG": filepath.Join(t.TempDir(), "absent.json")},
		"invalid config file": {"QUEUED_CONFIG": bad},
		"bad slot count":      {"QUEUED_WORKER_SLOTS": "many"},
		"backoff cap < base":  {"QUEUED_BACKOFF_BASE_MS": "500", "QUEUED_BACKOFF_CAP_MS": "100"},
	} {
		for _, role := range []string{"api", "dispatcher", "worker"} {
			vars := roleEnv(db, vars)
			vars["QUEUED_HTTP_ADDR"] = freeAddr(t)
			var stderr bytes.Buffer
			if code := run(t.Context(), []string{role}, env(vars), &stderr); code != 1 {
				t.Errorf("%s, %s: exit %d, want 1", name, role, code)
			}
			if !strings.Contains(stderr.String(), "role failed") {
				t.Errorf("%s, %s: stderr = %q, want a failure log", name, role, stderr.String())
			}
		}
	}
}

func TestSettingsDefaults(t *testing.T) {
	s, err := loadSettings(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if s.configPath != "/etc/queued/shops.json" || s.dispatcherURL != "http://dispatcher:8080" || s.slots != 8 ||
		s.refresh != 250*time.Millisecond || s.longPoll != 25*time.Second || s.reaperScan != 5*time.Second ||
		s.backoff.Base != 2*time.Second || s.backoff.Cap != 5*time.Minute || s.drain != 8*time.Second || s.testHandlers ||
		s.pruneInterval != time.Minute || s.retention.Completed != 24*time.Hour || s.retention.Discarded != 24*time.Hour {
		t.Errorf("defaults = %+v, want the design §2 settings", s)
	}
}

// The pruner's settings come from the environment, and a bad one fails fast.
func TestPruneSettings(t *testing.T) {
	s, err := loadSettings(env(map[string]string{
		"QUEUED_PRUNE_INTERVAL_MS":      "1000",
		"QUEUED_COMPLETED_RETENTION_MS": "2000",
		"QUEUED_DISCARDED_RETENTION_MS": "3000",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.pruneInterval != time.Second || s.retention.Completed != 2*time.Second || s.retention.Discarded != 3*time.Second {
		t.Errorf("settings = %+v", s)
	}
	if _, err := loadSettings(env(map[string]string{"QUEUED_COMPLETED_RETENTION_MS": "0"})); err == nil {
		t.Error("a zero retention window was accepted")
	}
}

// D-050: the pruner's dedupe window is the config file's, the one mockshop's
// retry horizon is checked against, and no environment setting overrides it.
func TestDedupeWindowComesFromTheConfigFile(t *testing.T) {
	s, err := loadSettings(env(map[string]string{"QUEUED_DEDUPE_WINDOW_MS": "4000"}))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(testConfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DedupeWindowMS = 9_000_000
	r := retention(s, cfg)
	if r.DedupeKeys != 9_000*time.Second || r.Completed != 24*time.Hour || r.Discarded != 24*time.Hour {
		t.Errorf("retention = %+v, want the file's dedupe window and the default job windows", r)
	}
}

// P4-5, AC-9.3: the scheduler is one environment setting, DRR unless it says
// FIFO, and a name that is neither fails fast.
func TestSchedulerSetting(t *testing.T) {
	for value, want := range map[string]scheduler.Mode{"": scheduler.ModeDRR, "drr": scheduler.ModeDRR, "fifo": scheduler.ModeFIFO} {
		s, err := loadSettings(env(map[string]string{"QUEUED_SCHEDULER": value}))
		if err != nil || s.scheduler != want {
			t.Errorf("QUEUED_SCHEDULER=%q: scheduler %q, err %v; want %q", value, s.scheduler, err, want)
		}
	}
	if _, err := loadSettings(env(map[string]string{"QUEUED_SCHEDULER": "lifo"})); err == nil {
		t.Error("an unknown scheduler was accepted")
	}
}

func TestHealthcheckFailsWhenNothingListens(t *testing.T) {
	var stderr bytes.Buffer
	code := run(t.Context(), []string{"healthcheck"}, env(map[string]string{"QUEUED_HTTP_ADDR": freeAddr(t)}), &stderr)
	if code != 1 {
		t.Fatalf("healthcheck with nothing listening exited %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unhealthy") {
		t.Errorf("stderr = %q, want it to say unhealthy", stderr.String())
	}
}

func TestHealthcheckUsesDefaultAddr(t *testing.T) {
	if got, err := httpserver.HealthURL(defaultAddr); err != nil || got != "http://127.0.0.1:8080/healthz" {
		t.Fatalf("HealthURL(defaultAddr) = %q, %v", got, err)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{nil, {"bogus"}, {"api", "extra"}} {
		var stderr bytes.Buffer
		if code := run(t.Context(), args, env(nil), &stderr); code != 2 {
			t.Errorf("run(%q) = %d, want 2", args, code)
		}
		if !strings.Contains(stderr.String(), "usage: queued") {
			t.Errorf("run(%q) printed %q, want the usage text", args, stderr.String())
		}
	}
}

func TestServeFailsOnBusyPort(t *testing.T) {
	db := pgtest.NewMigrated(t)
	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	for _, role := range []string{"api", "dispatcher", "worker"} {
		vars := roleEnv(db, map[string]string{"QUEUED_HTTP_ADDR": ln.Addr().String()})
		var stderr bytes.Buffer
		if code := run(t.Context(), []string{role}, env(vars), &stderr); code != 1 {
			t.Errorf("%s on a busy port exited %d, want 1", role, code)
		}
		if !strings.Contains(stderr.String(), "listen") {
			t.Errorf("%s: stderr = %q, want a listen error", role, stderr.String())
		}
	}
}

// The migrate command applies the embedded migrations to the database in PGDATABASE.
func TestMigrateCommand(t *testing.T) {
	db := pgtest.New(t)
	t.Setenv("PGDATABASE", db.Name) // pgx reads the PG* variables itself

	for i := range 2 {
		var stderr bytes.Buffer
		if code := run(t.Context(), []string{"migrate"}, env(nil), &stderr); code != 0 {
			t.Fatalf("migrate run %d exited %d: %s", i+1, code, stderr.String())
		}
	}
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	var recorded int
	conn := db.Connect(t)
	if err := conn.QueryRow(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != len(files) {
		t.Fatalf("schema_migrations has %d rows, want one per migration file (%d)", recorded, len(files))
	}
}

func TestMigrateCommandFailsWithoutPostgres(t *testing.T) {
	t.Setenv("PGHOST", "127.0.0.1")
	t.Setenv("PGPORT", portOf(t, freeAddr(t)))
	t.Setenv("PGCONNECT_TIMEOUT", "2")
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"migrate"}, env(nil), &stderr); code != 1 {
		t.Fatalf("migrate with no Postgres exited %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "migrate failed") {
		t.Errorf("stderr = %q, want a migrate failure log", stderr.String())
	}
}

func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return port
}
