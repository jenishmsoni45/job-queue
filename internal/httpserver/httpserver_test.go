package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestMuxServesHealthz(t *testing.T) {
	srv := httptest.NewServer(NewMux("svc-under-test"))
	defer srv.Close()

	resp := get(t, srv.URL+"/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz: status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body struct{ Status, Service string }
	if err := json.Unmarshal(resp.body, &body); err != nil {
		t.Fatalf("decode body %q: %v", resp.body, err)
	}
	if body.Status != "ok" || body.Service != "svc-under-test" {
		t.Errorf("body = %+v, want status ok and service svc-under-test", body)
	}
}

func TestMuxServesNothingElse(t *testing.T) {
	srv := httptest.NewServer(NewMux("svc"))
	defer srv.Close()

	for _, path := range []string{"/", "/metrics", "/v1/jobs", "/healthz/extra"} {
		if resp := get(t, srv.URL+path); resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 404", path, resp.StatusCode)
		}
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz: status %d, want 405", resp.StatusCode)
	}
}

func TestServeAnswersThenShutsDownCleanly(t *testing.T) {
	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, ln, NewMux("svc"), discardLogger()) }()

	if err := Probe(t.Context(), "http://"+ln.Addr().String()+"/healthz"); err != nil {
		t.Fatalf("probe while serving: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve after cancel = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after its context was cancelled")
	}
	if err := Probe(t.Context(), "http://"+ln.Addr().String()+"/healthz"); err == nil {
		t.Fatal("probe after shutdown succeeded, want an error")
	}
}

func TestListenAndServeRejectsBadAddress(t *testing.T) {
	err := ListenAndServe(t.Context(), "not-an-address", NewMux("svc"), discardLogger())
	if err == nil {
		t.Fatal("ListenAndServe with a bad address returned nil")
	}
}

func TestHealthURL(t *testing.T) {
	tests := []struct {
		addr, want string
		wantErr    bool
	}{
		{addr: ":8080", want: "http://127.0.0.1:8080/healthz"},
		{addr: "0.0.0.0:8090", want: "http://127.0.0.1:8090/healthz"},
		{addr: "[::]:8090", want: "http://127.0.0.1:8090/healthz"},
		{addr: "127.0.0.1:9000", want: "http://127.0.0.1:9000/healthz"},
		{addr: "[::1]:9000", want: "http://[::1]:9000/healthz"},
		{addr: "8080", wantErr: true},
		{addr: ":0", wantErr: true},
		{addr: "", wantErr: true},
	}
	for _, tt := range tests {
		got, err := HealthURL(tt.addr)
		if (err != nil) != tt.wantErr {
			t.Errorf("HealthURL(%q) error = %v, wantErr %v", tt.addr, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("HealthURL(%q) = %q, want %q", tt.addr, got, tt.want)
		}
	}
}

func TestProbe(t *testing.T) {
	status := func(code int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
	}
	ok := status(http.StatusOK)
	defer ok.Close()
	if err := Probe(t.Context(), ok.URL); err != nil {
		t.Errorf("Probe(200) = %v, want nil", err)
	}

	for _, code := range []int{http.StatusNoContent, http.StatusNotFound, http.StatusServiceUnavailable} {
		srv := status(code)
		if err := Probe(t.Context(), srv.URL); err == nil {
			t.Errorf("Probe(%d) = nil, want an error", code)
		}
		srv.Close()
	}

	closed := status(http.StatusOK)
	url := closed.URL
	closed.Close()
	if err := Probe(t.Context(), url); err == nil {
		t.Error("Probe(closed server) = nil, want an error")
	}
}

func TestProbeHonoursContext(t *testing.T) {
	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(hang)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := Probe(ctx, srv.URL); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Probe against a hanging server = %v, want context.DeadlineExceeded", err)
	}
}

type response struct {
	*http.Response
	body []byte
}

func get(t *testing.T, url string) response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return response{resp, body}
}
