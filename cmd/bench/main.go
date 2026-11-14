// Command bench is the Black Friday runner's Go half (design §17); the shell
// half, scripts/bench.sh, brings the jobqueue-bench stack up for each scheduler
// and runs loadgen. bench runs in the bench container on that stack.
//
//	bench collect -run DIR -scheduler fifo|drr -compose FILE
//	    wait until every accepted job is completed or dead, then export the run:
//	    DIR/raw/jobs.csv.gz and DIR/raw/mutations.json.gz (git-ignored, D-052),
//	    DIR/effective-config.json (the Compose config, secrets redacted) and
//	    DIR/run.json (the run's summary and checks)
//	bench report -dir DIR
//	    compare DIR/fifo and DIR/drr, and write DIR/summary.json, DIR/report.md
//	    and DIR/blackfriday.svg; exit 1 if any target or validity check fails
//
// Settings: the PG* variables locate Postgres, BENCH_MOCKSHOP_URL locates
// mockshop (http://mockshop:8090), and BENCH_CONFIG is the bench config file
// (bench/blackfriday.json).
package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jenishmsoni45/job-queue/internal/bench"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/handlers"
	"github.com/jenishmsoni45/job-queue/internal/idem"
	"github.com/jenishmsoni45/job-queue/internal/mockshop"
)

const usage = `usage:
  bench collect -run DIR -scheduler fifo|drr -compose FILE [-timeout 45m]
  bench report -dir DIR [-machine FILE]
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "collect":
		err = collectCmd(ctx, args[1:], getenv, log)
	case "report":
		var pass bool
		pass, err = reportCmd(args[1:], stdout)
		if err == nil && !pass {
			_, _ = fmt.Fprintln(stderr, "bench: a target or validity check FAILED; see report.md. This is a stop-and-ask (P5-7).")
			return 1
		}
	default:
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	if err != nil {
		log.Error("bench failed", "err", err.Error())
		return 1
	}
	return 0
}

func collectCmd(ctx context.Context, args []string, getenv func(string) string, log *slog.Logger) error {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	dir := fs.String("run", "", "the run's results directory")
	scheduler := fs.String("scheduler", "", "fifo or drr")
	composeFile := fs.String("compose", "", "the run's effective Compose config, as `docker compose config --format json` wrote it")
	timeout := fs.Duration("timeout", 45*time.Minute, "how long to wait for every job to finish")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || (*scheduler != "fifo" && *scheduler != "drr") || *composeFile == "" {
		return errors.New("collect needs -run, -scheduler fifo|drr and -compose")
	}
	cfgPath := getenv("BENCH_CONFIG")
	if cfgPath == "" {
		cfgPath = "bench/blackfriday.json"
	}
	cfg, sum, err := loadConfig(cfgPath)
	if err != nil {
		return err
	}
	mockURL := getenv("BENCH_MOCKSHOP_URL")
	if mockURL == "" {
		mockURL = "http://mockshop:8090"
	}
	pool, err := pgxpool.New(ctx, "")
	if err != nil {
		return fmt.Errorf("connect to Postgres: %w", err)
	}
	defer pool.Close()

	if err := waitTerminal(ctx, pool, *timeout, log); err != nil {
		return err
	}
	jobs, err := readJobs(ctx, pool)
	if err != nil {
		return err
	}
	shed, err := readShed(ctx, pool)
	if err != nil {
		return err
	}
	logs, err := readMutations(ctx, mockURL, cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(*dir, "raw"), 0o750); err != nil {
		return err
	}
	if err := writeJobsCSV(filepath.Join(*dir, "raw", "jobs.csv.gz"), jobs); err != nil {
		return err
	}
	if err := writeGzipJSON(filepath.Join(*dir, "raw", "mutations.json.gz"), logs); err != nil {
		return err
	}
	if err := writeEffective(*composeFile, filepath.Join(*dir, "effective-config.json")); err != nil {
		return err
	}

	in := bench.Input{
		Scheduler: *scheduler, ConfigSHA256: sum, Config: cfg, Jobs: jobs, Shed: shed,
		Mutations: map[string]map[string]int{}, Replays: map[string]int{},
		MutationKey: func(id int64) string { return idem.Key(id, handlers.TagStep) },
	}
	for _, m := range logs {
		in.Mutations[m.Shop], in.Replays[m.Shop] = m.Applied, m.Replays
	}
	r, err := bench.Analyze(in)
	if err != nil {
		return err
	}
	log.Info("collected run", "scheduler", *scheduler, "jobs", r.Totals.Jobs, "nfr9", r.NFR9.Pass, "nfr11", r.NFR11.Pass, "nfr12", r.NFR12.Pass)
	return writeJSON(filepath.Join(*dir, "run.json"), r)
}

func loadConfig(path string) (*config.File, string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // the bench config the operator named
	if err != nil {
		return nil, "", err
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, "", err
	}
	if cfg.Loadgen == nil {
		return nil, "", errors.New("the bench config has no loadgen section")
	}
	h := sha256.Sum256(raw)
	return cfg, hex.EncodeToString(h[:]), nil
}

// waitTerminal waits until no job is queued or running (design §17: "waits
// until every accepted job is terminal").
func waitTerminal(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration, log *slog.Logger) error {
	deadline := time.Now().Add(timeout)
	lastLog := time.Time{}
	for {
		var left int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')`).Scan(&left); err != nil {
			return fmt.Errorf("count unfinished jobs: %w", err)
		}
		if left == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d jobs still unfinished after %v", left, timeout)
		}
		if time.Since(lastLog) > 30*time.Second {
			log.Info("waiting for jobs to finish", "unfinished", left)
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func readJobs(ctx context.Context, pool *pgxpool.Pool) ([]bench.Job, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, shop_id, state, enqueued_at, first_runnable_at, first_claimed_at,
		       coalesce(completed_at, dead_at), attempts, throttles, claim_seq
		  FROM jobs ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("read jobs: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (bench.Job, error) {
		var j bench.Job
		err := row.Scan(&j.ID, &j.Shop, &j.State, &j.EnqueuedAt, &j.FirstRunnableAt, &j.FirstClaimedAt,
			&j.TerminalAt, &j.Attempts, &j.Throttles, &j.ClaimSeq)
		return j, err
	})
}

func readShed(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `SELECT shop_id, shed_total FROM shop_state`)
	if err != nil {
		return nil, fmt.Errorf("read shed counts: %w", err)
	}
	out := map[string]int64{}
	var shop string
	var n int64
	_, err = pgx.ForEachRow(rows, []any{&shop, &n}, func() error {
		out[shop] = n
		return nil
	})
	return out, err
}

func readMutations(ctx context.Context, base string, cfg *config.File) ([]mockshop.Mutations, error) {
	var out []mockshop.Mutations
	for _, s := range cfg.Shops {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+mockshop.MutationsPath+"?shop="+url.QueryEscape(s.ID), nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("read mockshop's mutation log: %w", err)
		}
		var m mockshop.Mutations
		err = json.NewDecoder(resp.Body).Decode(&m)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("mockshop's mutation log for %s: status %d", s.ID, resp.StatusCode)
		}
		if err != nil {
			return nil, fmt.Errorf("mockshop's mutation log for %s: %w", s.ID, err)
		}
		out = append(out, m)
	}
	return out, nil
}

func writeJobsCSV(path string, jobs []bench.Job) error {
	f, err := os.Create(path) //nolint:gosec // under the results directory
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(f)
	cw := csv.NewWriter(gz)
	ts := func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return t.UTC().Format(time.RFC3339Nano)
	}
	_ = cw.Write([]string{"id", "shop_id", "state", "enqueued_at", "first_runnable_at", "first_claimed_at", "terminal_at", "attempts", "throttles", "claim_seq"})
	for _, j := range jobs {
		_ = cw.Write([]string{strconv.FormatInt(j.ID, 10), j.Shop, j.State, ts(&j.EnqueuedAt), ts(&j.FirstRunnableAt),
			ts(j.FirstClaimedAt), ts(j.TerminalAt), strconv.Itoa(int(j.Attempts)), strconv.Itoa(int(j.Throttles)), strconv.Itoa(int(j.ClaimSeq))})
	}
	cw.Flush()
	return errors.Join(cw.Error(), gz.Close(), f.Close())
}

func writeGzipJSON(path string, v any) error {
	f, err := os.Create(path) //nolint:gosec // under the results directory
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(f)
	err = json.NewEncoder(gz).Encode(v)
	return errors.Join(err, gz.Close(), f.Close())
}

// writeEffective copies the run's Compose config with every password and
// secret value redacted (design §15), so it can be committed and diffed.
func writeEffective(from, to string) error {
	raw, err := os.ReadFile(from) //nolint:gosec // the file scripts/bench.sh wrote
	if err != nil {
		return err
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("parse %s: %w", from, err)
	}
	return writeJSON(to, bench.Redact(doc))
}

func writeJSON(path string, v any) error {
	b, err := bench.MarshalIndent(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644) //nolint:gosec // committed results, meant to be read
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path) //nolint:gosec // a file in the results directory
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func readRun(dir string) (bench.RunFiles, error) {
	var f bench.RunFiles
	if err := readJSON(filepath.Join(dir, "run.json"), &f.Run); err != nil {
		return f, err
	}
	if err := readJSON(filepath.Join(dir, "loadgen.json"), &f.Loadgen); err != nil {
		return f, err
	}
	err := readJSON(filepath.Join(dir, "effective-config.json"), &f.Effective)
	return f, err
}

func reportCmd(args []string, stdout io.Writer) (bool, error) {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dir := fs.String("dir", "", "the bench results directory, holding fifo/ and drr/")
	machinePath := fs.String("machine", "", "machine.json (default DIR/machine.json)")
	if err := fs.Parse(args); err != nil {
		return false, err
	}
	if *dir == "" {
		return false, errors.New("report needs -dir")
	}
	fifo, err := readRun(filepath.Join(*dir, "fifo"))
	if err != nil {
		return false, err
	}
	drr, err := readRun(filepath.Join(*dir, "drr"))
	if err != nil {
		return false, err
	}
	if *machinePath == "" {
		*machinePath = filepath.Join(*dir, "machine.json")
	}
	var machine map[string]any
	if err := readJSON(*machinePath, &machine); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	s := bench.Compare(fifo, drr, machine, time.Now())
	if err := writeJSON(filepath.Join(*dir, "summary.json"), s); err != nil {
		return false, err
	}
	if err := os.WriteFile(filepath.Join(*dir, "report.md"), []byte(bench.Markdown(s, fifo.Run, drr.Run)), 0o644); err != nil { //nolint:gosec // committed results
		return false, err
	}
	if err := os.WriteFile(filepath.Join(*dir, "blackfriday.svg"), bench.Chart(fifo.Run, drr.Run), 0o644); err != nil { //nolint:gosec // committed results
		return false, err
	}
	for _, c := range append(s.Targets, s.Validity...) {
		mark := "PASS"
		if !c.Pass {
			mark = "FAIL"
		}
		_, _ = fmt.Fprintf(stdout, "%s  %s: %s\n", mark, c.Name, c.Detail)
	}
	return s.Pass, nil
}
