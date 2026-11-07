package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
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
	getenv := env(map[string]string{"MOCKSHOP_HTTP_ADDR": addr})
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
