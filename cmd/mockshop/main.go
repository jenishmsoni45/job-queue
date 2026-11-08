// Command mockshop is a local simulation of Shopify: it will send signed,
// sometimes-duplicated webhooks and serve a rate-limited Admin API (design §12,
// phase 5). It never calls the real Shopify.
//
//	mockshop              serve
//	mockshop healthcheck  exit 0 if this container's server answers GET /healthz
//
// In phase 1 it serves GET /healthz and nothing else. MOCKSHOP_HTTP_ADDR is the
// listen address (default ":8090").
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/httpserver"
)

const (
	defaultAddr        = ":8090"
	healthcheckTimeout = 2 * time.Second
)

const usage = `usage: mockshop [healthcheck]

  (no command)  serve the simulated Shopify
  healthcheck   exit 0 if this process's /healthz answers 200
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stderr)
	stop()
	os.Exit(code)
}

// run executes one command and returns the process exit code.
func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) int {
	addr := getenv("MOCKSHOP_HTTP_ADDR")
	if addr == "" {
		addr = defaultAddr
	}
	switch {
	case len(args) == 0:
		log := slog.New(slog.NewJSONHandler(stderr, nil)).With("service", "mockshop")
		if err := httpserver.ListenAndServe(ctx, addr, httpserver.NewMux("mockshop"), log); err != nil {
			log.Error("server failed", "err", err)
			return 1
		}
		return 0
	case len(args) == 1 && args[0] == "healthcheck":
		url, err := httpserver.HealthURL(addr)
		if err == nil {
			hctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
			err = httpserver.Probe(hctx, url)
			cancel()
		}
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "unhealthy:", err)
			return 1
		}
		return 0
	default:
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
}
