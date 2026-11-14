// Command mockshop is a local simulation of Shopify (design §12, D-010): it
// sends signed, sometimes-duplicated orders/create webhooks to the api role and
// serves an Admin API with a leaky bucket per shop, Idempotency-Key replay and a
// mutation log. It never calls the real Shopify.
//
//	mockshop              serve
//	mockshop healthcheck  exit 0 if this container's server answers GET /healthz
//
// Settings:
//
//	MOCKSHOP_HTTP_ADDR      listen address (":8090")
//	MOCKSHOP_CONFIG         the shared config file (/etc/queued/shops.json): each shop's
//	                        bucket and webhook rate, and the sender's settings (D-050)
//	MOCKSHOP_WEBHOOK_URL    where webhooks go (http://api:8080/webhooks/shopify)
//	SHOPIFY_WEBHOOK_SECRET  the dummy HMAC secret; required if any shop sends webhooks
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/httpserver"
	"github.com/jenishmsoni45/job-queue/internal/mockshop"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

const (
	defaultAddr        = ":8090"
	defaultConfig      = "/etc/queued/shops.json"
	defaultWebhookURL  = "http://api:8080" + webhook.Path
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
		if err := serve(ctx, addr, getenv, log); err != nil {
			log.Error("server failed", "err", err.Error())
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

// serve validates the config file, then serves the Admin API and runs the
// webhook sender until ctx ends.
func serve(ctx context.Context, addr string, getenv func(string) string, log *slog.Logger) error {
	path := getenv("MOCKSHOP_CONFIG")
	if path == "" {
		path = defaultConfig
	}
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	secret := getenv("SHOPIFY_WEBHOOK_SECRET")
	sends := false
	for _, s := range cfg.Shops {
		sends = sends || s.WebhooksPerSec > 0
	}
	if sends && secret == "" {
		return mockshop.ErrNoSecret
	}
	target := getenv("MOCKSHOP_WEBHOOK_URL")
	if target == "" {
		target = defaultWebhookURL
	}

	mux := httpserver.NewMux("mockshop")
	mockshop.NewAPI(cfg, nil).Register(mux)
	sender := &mockshop.Sender{
		Target: target, Secret: []byte(secret), Settings: cfg.Mockshop, Shops: cfg.Shops,
		HTTP: &http.Client{}, Log: log,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		sender.Run(runCtx)
	}()
	log.Info("mockshop starting", "shops", len(cfg.Shops), "sends_webhooks", sends, "webhook_url", target)
	err = httpserver.ListenAndServe(runCtx, addr, mux, log)
	cancel()
	<-sent
	return err
}
