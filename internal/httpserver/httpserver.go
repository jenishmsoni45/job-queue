// Package httpserver runs the HTTP server each process exposes and the probe its
// container healthcheck uses. In phase 1 the only endpoint is GET /healthz.
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

const (
	readHeaderTimeout = 5 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// HealthHandler answers with 200 and a small JSON body naming the service.
// It reports that the process is up and serving HTTP; it checks no dependency.
func HealthHandler(service string) http.Handler {
	body, err := json.Marshal(struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}{"ok", service})
	if err != nil {
		panic(fmt.Sprintf("httpserver: marshal health body: %v", err)) // a struct of two strings always marshals
	}
	body = append(body, '\n')
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	})
}

// NewMux returns a mux that serves GET /healthz and nothing else.
func NewMux(service string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", HealthHandler(service))
	return mux
}

// ListenAndServe listens on addr and serves h until ctx is done (see Serve).
func ListenAndServe(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen on %q: %w", addr, err)
	}
	log.Info("listening", "addr", ln.Addr().String())
	return Serve(ctx, ln, h, log)
}

// Serve serves h on ln until ctx is done, then shuts down gracefully, letting
// in-flight requests finish for up to shutdownTimeout. It closes ln. It returns
// nil after a clean shutdown and an error if serving or shutting down failed.
func Serve(ctx context.Context, ln net.Listener, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	select {
	case err := <-served:
		return fmt.Errorf("httpserver: serve: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("httpserver: shutdown: %w", err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("httpserver: serve: %w", err)
	}
	log.Info("stopped")
	return nil
}

// HealthURL returns the /healthz URL for a server listening on listenAddr, such
// as ":8080". An empty or unspecified host (0.0.0.0, ::) becomes 127.0.0.1.
func HealthURL(listenAddr string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("httpserver: listen address %q: %w", listenAddr, err)
	}
	if port == "" || port == "0" {
		return "", fmt.Errorf("httpserver: listen address %q has no fixed port", listenAddr)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz", nil
}

// Probe sends GET url and returns nil only for a 200 response. The container
// healthchecks run it, because the app image has no shell or curl.
func Probe(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("httpserver: probe request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("httpserver: probe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("httpserver: probe %s: status %d", url, resp.StatusCode)
	}
	return nil
}
