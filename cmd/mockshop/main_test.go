package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/mockshop"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

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

// P1-7: mockshop answers GET /healthz with 200, and its healthcheck command agrees.
func TestServesHealthz(t *testing.T) {
	addr := freeAddr(t)
	getenv := env(map[string]string{"MOCKSHOP_HTTP_ADDR": addr, "MOCKSHOP_CONFIG": testConfig})
	ctx, cancel := context.WithCancel(t.Context())
	exited := make(chan int, 1)
	go func() { exited <- run(ctx, nil, getenv, io.Discard) }()
	defer func() {
		cancel()
		select {
		case code := <-exited:
			if code != 0 {
				t.Errorf("mockshop exited with %d after shutdown", code)
			}
		case <-time.After(10 * time.Second):
			t.Error("mockshop did not stop")
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for run(t.Context(), []string{"healthcheck"}, getenv, io.Discard) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("mockshop never became healthy")
		}
		time.Sleep(20 * time.Millisecond)
	}

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
	if body.Status != "ok" || body.Service != "mockshop" {
		t.Errorf("body = %+v, want status ok and service mockshop", body)
	}
}

const testConfig = "../../tests/testdata/shops.json"

// startMockshop runs mockshop with vars until the test ends and waits until it
// is healthy. It returns the base URL.
func startMockshop(t *testing.T, vars map[string]string) string {
	t.Helper()
	addr := freeAddr(t)
	vars["MOCKSHOP_HTTP_ADDR"] = addr
	getenv := env(vars)
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan int, 1)
	go func() { exited <- run(ctx, nil, getenv, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			t.Error("mockshop did not stop")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for run(t.Context(), []string{"healthcheck"}, getenv, io.Discard) != 0 {
		select {
		case code := <-exited:
			t.Fatalf("mockshop exited with %d", code)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("mockshop never became healthy")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "http://" + addr
}

// The running mockshop serves the Admin API and the mutation log (design §12).
func TestServesTheAdminAPI(t *testing.T) {
	base := startMockshop(t, map[string]string{"MOCKSHOP_CONFIG": testConfig})
	c := &adminapi.Client{BaseURL: base, HTTP: http.DefaultClient}
	for range 2 {
		if err := c.TagOrder(t.Context(), "shop-a.test", 42, "queued", "job-42-tag"); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+mockshop.MutationsPath+"?shop=shop-a.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var m mockshop.Mutations
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Applied["job-42-tag"] != 1 || m.Replays != 1 {
		t.Errorf("mutations = %+v, want one applied and one replay", m)
	}
}

// A shop with a webhook rate makes mockshop send signed orders/create
// webhooks to MOCKSHOP_WEBHOOK_URL (design §12, D-054).
func TestSendsSignedWebhooks(t *testing.T) {
	cfg, err := config.Load(testConfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Shops[0].WebhooksPerSec = 50
	path := filepath.Join(t.TempDir(), "shops.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 100)
	rcv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch {
		case !webhook.Verify([]byte("dummy"), body, r.Header.Get(webhook.HeaderHMAC)):
			got <- errors.New("bad HMAC")
		case r.Header.Get(webhook.HeaderShopDomain) != cfg.Shops[0].ID || r.Header.Get(webhook.HeaderTopic) != webhook.TopicOrdersCreate ||
			r.Header.Get(webhook.HeaderWebhookID) == "":
			got <- fmt.Errorf("headers = %v", r.Header)
		default:
			got <- nil
		}
	}))
	defer rcv.Close()
	startMockshop(t, map[string]string{"MOCKSHOP_CONFIG": path, "MOCKSHOP_WEBHOOK_URL": rcv.URL, "SHOPIFY_WEBHOOK_SECRET": "dummy"})
	for range 3 {
		select {
		case err := <-got:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no webhook arrived")
		}
	}
}

// mockshop fails fast on a bad config, and refuses to send unsigned webhooks.
func TestServeFailsFast(t *testing.T) {
	cfg, err := config.Load(testConfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Shops[0].WebhooksPerSec = 1
	sending := filepath.Join(t.TempDir(), "sending.json")
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(sending, data, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, vars := range map[string]map[string]string{
		"missing config": {"MOCKSHOP_CONFIG": filepath.Join(t.TempDir(), "absent.json")},
		"no secret":      {"MOCKSHOP_CONFIG": sending},
	} {
		vars["MOCKSHOP_HTTP_ADDR"] = freeAddr(t)
		var stderr bytes.Buffer
		if code := run(t.Context(), nil, env(vars), &stderr); code != 1 {
			t.Errorf("%s: exit %d, want 1", name, code)
		}
		if !strings.Contains(stderr.String(), "server failed") {
			t.Errorf("%s: stderr = %q", name, stderr.String())
		}
	}
}

func TestHealthcheckFailsWhenNothingListens(t *testing.T) {
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"healthcheck"}, env(map[string]string{"MOCKSHOP_HTTP_ADDR": freeAddr(t)}), &stderr); code != 1 {
		t.Fatalf("healthcheck with nothing listening exited %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unhealthy") {
		t.Errorf("stderr = %q, want it to say unhealthy", stderr.String())
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{{"bogus"}, {"healthcheck", "extra"}} {
		var stderr bytes.Buffer
		if code := run(t.Context(), args, env(nil), &stderr); code != 2 {
			t.Errorf("run(%q) = %d, want 2", args, code)
		}
		if !strings.Contains(stderr.String(), "usage: mockshop") {
			t.Errorf("run(%q) printed %q, want the usage text", args, stderr.String())
		}
	}
}
