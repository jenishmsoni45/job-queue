// Command loadgen drives the Black Friday experiment (design §17, D-009): it
// sends seeded, open-loop Poisson arrivals of order.tag jobs for every shop to
// the api role's batch endpoint, one batch per shop per tick, through the
// warm-up, the spike and the tail, and stops once the spike's backlog has
// drained (D-056). It prints what it did as JSON on stdout; logs go to stderr.
//
// Settings:
//
//	LOADGEN_CONFIG   the shared config file (/etc/queued/shops.json); it must
//	                 have a loadgen section (D-050)
//	LOADGEN_API_URL  the api role (http://api:8080)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/loadgen"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run executes loadgen and returns the process exit code: 0 when the run
// finished, drained or not (the report says which), 1 on a bad setting, 2 on a
// usage error.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		_, _ = fmt.Fprintln(stderr, "usage: loadgen (settings come from LOADGEN_CONFIG and LOADGEN_API_URL)")
		return 2
	}
	log := slog.New(slog.NewJSONHandler(stderr, nil)).With("service", "loadgen")
	rep, err := generate(ctx, getenv, log)
	if err != nil {
		log.Error("loadgen failed", "err", err.Error())
		return 1
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		log.Error("write the report", "err", err.Error())
		return 1
	}
	return 0
}

func generate(ctx context.Context, getenv func(string) string, log *slog.Logger) (loadgen.Report, error) {
	path := getenv("LOADGEN_CONFIG")
	if path == "" {
		path = "/etc/queued/shops.json"
	}
	api := getenv("LOADGEN_API_URL")
	if api == "" {
		api = "http://api:8080"
	}
	cfg, err := config.Load(path)
	if err != nil {
		return loadgen.Report{}, err
	}
	if cfg.Loadgen == nil {
		return loadgen.Report{}, errors.New("the config file has no loadgen section (D-050)")
	}
	shops := make([]string, len(cfg.Shops))
	for i, s := range cfg.Shops {
		shops[i] = s.ID
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 256
	r := &loadgen.Runner{
		API: api, HTTP: &http.Client{Transport: tr, Timeout: 60 * time.Second},
		Cfg: *cfg.Loadgen, Shops: shops, Log: log,
	}
	log.Info("loadgen starting", "api", api, "shops", len(shops), "seed", cfg.Loadgen.Seed)
	rep := r.Run(ctx)
	log.Info("loadgen finished", "drained", rep.Drained, "drain_timed_out", rep.DrainTimedOut,
		"interrupted", rep.Interrupted, "late_ticks", rep.LateTicks)
	return rep, nil
}
