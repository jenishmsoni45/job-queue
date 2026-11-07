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

	"github.com/jenishmsoni45/job-queue/internal/httpserver"
	"github.com/jenishmsoni45/job-queue/internal/pgtest"
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

// startRole runs `queued <role>` in the background and waits until its
// healthcheck command passes. It stops the role when the test ends.
func startRole(t *testing.T, role string) string {
	t.Helper()
	addr := freeAddr(t)
	getenv := env(map[string]string{"QUEUED_HTTP_ADDR": addr})
	ctx, cancel := context.WithCancel(t.Context())
	exited := make(chan int, 1)
	go func() { exited <- run(ctx, []string{role}, getenv, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-exited:
			if code != 0 {
				t.Errorf("queued %s exited with %d after shutdown", role, code)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("queued %s did not stop", role)
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if code := run(t.Context(), []string{"healthcheck"}, getenv, io.Discard); code == 0 {
			return addr
		}
		select {
		case code := <-exited:
			t.Fatalf("queued %s exited early with %d", role, code)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued %s never became healthy", role)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// P1-7: each queued role answers GET /healthz with 200.
func TestEachRoleServesHealthz(t *testing.T) {
	for _, role := range []string{"api", "dispatcher", "worker"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			addr := startRole(t, role)

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

			// Nothing else is served yet.
			if err := httpserver.Probe(t.Context(), "http://"+addr+"/v1/jobs"); err == nil {
				t.Error("GET /v1/jobs succeeded, want 404 in phase 1")
			}
		})
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
	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	var stderr bytes.Buffer
	code := run(t.Context(), []string{"api"}, env(map[string]string{"QUEUED_HTTP_ADDR": ln.Addr().String()}), &stderr)
	if code != 1 {
		t.Fatalf("api on a busy port exited %d, want 1", code)
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
	var versions string
	conn := db.Connect(t)
	if err := conn.QueryRow(t.Context(),
		"SELECT string_agg(version::text, ',' ORDER BY version) FROM schema_migrations").Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != "1" {
		t.Fatalf("schema_migrations versions = %q, want \"1\"", versions)
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
