package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
)

// Toxiproxy is a minimal client for Toxiproxy's HTTP API, with net/http (D-006).
type Toxiproxy struct {
	client *http.Client
	base   string
}

// NewToxiproxy returns a client for the Toxiproxy API at baseURL.
func NewToxiproxy(client *http.Client, baseURL string) *Toxiproxy {
	return &Toxiproxy{client: client, base: baseURL}
}

// Toxic is one Toxiproxy toxic.
type Toxic struct {
	Name       string           `json:"name"`
	Type       string           `json:"type"`   // latency, timeout or reset_peer
	Stream     string           `json:"stream"` // downstream (server to client) or upstream
	Toxicity   float64          `json:"toxicity"`
	Attributes map[string]int64 `json:"attributes"`
}

// Proxies returns the names of the configured proxies.
func (t *Toxiproxy) Proxies(ctx context.Context) ([]string, error) {
	var proxies map[string]json.RawMessage
	if err := t.do(ctx, http.MethodGet, "/proxies", nil, &proxies, http.StatusOK); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(proxies))
	for name := range proxies {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

// Reset enables every proxy and removes every toxic.
func (t *Toxiproxy) Reset(ctx context.Context) error {
	return t.do(ctx, http.MethodPost, "/reset", nil, nil, http.StatusNoContent, http.StatusOK)
}

// AddToxic adds a toxic to a proxy.
func (t *Toxiproxy) AddToxic(ctx context.Context, proxy string, toxic Toxic) error {
	return t.do(ctx, http.MethodPost, "/proxies/"+url.PathEscape(proxy)+"/toxics", toxic, nil, http.StatusOK)
}

// RemoveToxic removes a toxic from a proxy. A toxic that is already gone is not an error.
func (t *Toxiproxy) RemoveToxic(ctx context.Context, proxy, name string) error {
	return t.do(ctx, http.MethodDelete, "/proxies/"+url.PathEscape(proxy)+"/toxics/"+url.PathEscape(name), nil, nil,
		http.StatusNoContent, http.StatusOK, http.StatusNotFound)
}

func (t *Toxiproxy) do(ctx context.Context, method, path string, in, out any, ok ...int) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("chaos: toxiproxy %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if !slices.Contains(ok, resp.StatusCode) {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("chaos: toxiproxy %s %s: %s: %s", method, path, resp.Status, msg)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("chaos: toxiproxy %s %s: decode: %w", method, path, err)
	}
	return nil
}
