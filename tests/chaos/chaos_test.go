package chaos

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jenishmsoni45/job-queue/internal/config"
)

// ComposeProject is the chaos stack's Compose project (D-042). It is fixed, not
// a setting, so the harness can never be pointed at the `make up` stack.
const ComposeProject = "jobqueue-chaos"

// defaultRuns is how many runs `make chaos` makes (P3-1).
const defaultRuns = 100

// TestChaos runs seeded chaos runs against the chaos stack (design §18), and,
// with CHAOS_RECOVERY=1, the crash-recovery scenario. It runs only in the
// chaos container that scripts/chaos.sh starts, which sets CHAOS_RUN=1:
//
//	make chaos                      100 runs from a fresh base seed, then crash recovery
//	CHAOS_SEED=<n> make chaos-one   replay one run
//	make chaos-smoke                3 runs (CI)
//
// Settings: CHAOS_SEED, CHAOS_RUNS, CHAOS_BASE_SEED (D-046), CHAOS_RECOVERY,
// CHAOS_RESULTS_DIR, CHAOS_CONFIG, CHAOS_API_URL, CHAOS_MOCKSHOP_URL,
// CHAOS_TOXIPROXY_URL, CHAOS_DOCKER_SOCKET and CHAOS_REAPER_SCAN_MS.
func TestChaos(t *testing.T) {
	if os.Getenv("CHAOS_RUN") != "1" {
		t.Skip("chaos runs need the chaos stack: use `make chaos`, `make chaos-one` or `make chaos-smoke`")
	}
	ctx := t.Context()

	cfg, err := config.Load(setting("CHAOS_CONFIG", "shops.json"))
	if err != nil {
		t.Fatal(err)
	}
	var shops []string
	for _, s := range cfg.Shops {
		shops = append(shops, s.ID)
	}
	effect, ok := findKind(cfg, EffectKind)
	if !ok {
		t.Fatalf("the chaos config has no %s kind", EffectKind)
	}
	sleepKind, ok := findKind(cfg, RecoveryKind)
	if !ok {
		t.Fatalf("the chaos config has no %s kind", RecoveryKind)
	}
	reaperScanMS, err := strconv.ParseInt(setting("CHAOS_REAPER_SCAN_MS", "5000"), 10, 64)
	if err != nil {
		t.Fatalf("CHAOS_REAPER_SCAN_MS: %v", err)
	}

	seeds, base, err := Seeds(os.Getenv, defaultRuns)
	if err != nil {
		t.Fatal(err)
	}
	poolCfg, err := pgxpool.ParseConfig("") // PG* variables: Postgres directly, not through Toxiproxy
	if err != nil {
		t.Fatal(err)
	}
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "chaos-harness"
	poolCfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	env := &Env{
		DB:       pool,
		Docker:   NewDocker(setting("CHAOS_DOCKER_SOCKET", "/var/run/docker.sock"), ComposeProject, Services...),
		Toxi:     NewToxiproxy(&http.Client{Timeout: 10 * time.Second}, setting("CHAOS_TOXIPROXY_URL", "http://toxiproxy:8474")),
		API:      setting("CHAOS_API_URL", "http://api:8080"),
		Mockshop: setting("CHAOS_MOCKSHOP_URL", "http://mockshop:8090"),
		HTTP:     &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: enqueueConcurrency}},
		Shops:    shops,
		Logf:     t.Logf,
	}

	started := time.Now().UTC()
	dir := filepath.Join(setting("CHAOS_RESULTS_DIR", filepath.Join("..", "..", "bench", "results", "chaos")),
		started.Format("20060102T150405Z"))
	batch := &Batch{
		Started:  started,
		BaseSeed: base,
		Settings: Settings{
			LeaseMS: int64(effect.LeaseMS), HeartbeatMS: int64(effect.HeartbeatMS),
			FaultWindowMS: FaultWindow.Milliseconds(), RunTimeoutMS: RunTimeout.Milliseconds(),
			JobsPerRun: JobsPerRun, Shops: len(shops), MaxJobSleepMS: MaxJobSleepMS,
		},
	}
	defer func() {
		batch.Finished = time.Now().UTC()
		if err := WriteJSON(filepath.Join(dir, "summary.json"), batch); err != nil {
			t.Error(err)
		}
		if err := batch.WriteMarkdown(filepath.Join(dir, "summary.md")); err != nil {
			t.Error(err)
		}
		t.Logf("results: %s (%d runs passed, %d failed)", dir, batch.Passed, batch.Failed)
	}()
	if base != nil {
		t.Logf("%d runs from base seed %d (CHAOS_BASE_SEED=%d repeats them)", len(seeds), *base, *base)
	}

	if err := env.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	for i, seed := range seeds {
		res, err := env.Run(ctx, NewPlan(seed, shops))
		file := fmt.Sprintf("run-%03d-seed-%d.json", i+1, seed)
		if werr := WriteJSON(filepath.Join(dir, file), res); werr != nil {
			t.Error(werr)
		}
		if err != nil {
			t.Fatalf("run %d (seed %d): the stack could not be restored, so no later run can be trusted: %v", i+1, seed, err)
		}
		batch.Runs = append(batch.Runs, res.Summarize(file))
		if res.Passed {
			batch.Passed++
		} else {
			batch.Failed++
			t.Errorf("run %d (seed %d) FAILED: %v (replay with CHAOS_SEED=%d make chaos-one)",
				i+1, seed, batch.Runs[len(batch.Runs)-1].Failures, seed)
		}
		t.Logf("run %d/%d seed %d: passed=%v in %d ms; %d completed, %d dead; %d faults; %d enqueue retries",
			i+1, len(seeds), seed, res.Passed, res.DurationMS, res.Jobs.Completed, res.Jobs.Dead, len(res.Faults), res.Jobs.EnqueueRetries)
	}

	if os.Getenv("CHAOS_RECOVERY") == "1" {
		r := env.Recovery(ctx, int64(sleepKind.LeaseMS), reaperScanMS)
		batch.Recovery = &r
		if err := WriteJSON(filepath.Join(dir, "recovery.json"), r); err != nil {
			t.Error(err)
		}
		t.Logf("crash recovery: %d orphans of %s claimed again after %d ms; target %d ms; passed=%v %s",
			r.Orphans, r.Killed, r.RecoveryMS, r.TargetMS, r.Passed, r.Error)
		if !r.Passed {
			t.Errorf("crash recovery (NFR-10) FAILED: %+v", r)
		}
	}

	// Leave the stack clean for whatever runs next.
	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	if err := env.Restore(restoreCtx); err != nil {
		t.Error(err)
	}
}

func setting(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func findKind(cfg *config.File, kind string) (config.Kind, bool) {
	for _, k := range cfg.Kinds {
		if k.Kind == kind {
			return k, true
		}
	}
	return config.Kind{}, false
}
