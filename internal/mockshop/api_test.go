package mockshop

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
	"github.com/jenishmsoni45/job-queue/internal/config"
)

// fakeClock is a settable clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newAPI(t *testing.T, capacity int, leak float64) (*API, *httptest.Server, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: t0}
	cfg := &config.File{Shops: []config.Shop{
		{ID: "shop-a.test", MockAPICapacity: capacity, MockAPILeakPerSec: leak},
		{ID: "shop-b.test", MockAPICapacity: capacity, MockAPILeakPerSec: leak},
	}}
	a := NewAPI(cfg, clk.Now)
	mux := http.NewServeMux()
	a.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return a, srv, clk
}

type answer struct {
	status     int
	callLimit  string
	retryAfter string
	replayed   string
	body       string
}

func tag(t *testing.T, srv *httptest.Server, shop, order, key, body string) answer {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/admin/api/mock/shops/"+shop+"/orders/"+order+"/tags", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set(adminapi.HeaderIdempotencyKey, key)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	return answer{resp.StatusCode, resp.Header.Get(adminapi.HeaderCallLimit), resp.Header.Get(adminapi.HeaderRetryAfter),
		resp.Header.Get(HeaderReplayed), b.String()}
}

// Design §12 over HTTP: every call costs a slot and carries the call limit; a
// full bucket answers 429 with Retry-After in decimal seconds, the time until one
// slot frees; and one shop's bucket never throttles another's.
func TestTagCallsAreRateLimited(t *testing.T) {
	_, srv, clk := newAPI(t, 2, 4) // capacity 2, leaking 4 per second
	body := `{"tag":"queued"}`
	if a := tag(t, srv, "shop-a.test", "1", "", body); a.status != 200 || a.callLimit != "1/2" {
		t.Fatalf("first call = %+v, want 200 with 1/2", a)
	}
	if a := tag(t, srv, "shop-a.test", "2", "", body); a.status != 200 || a.callLimit != "2/2" {
		t.Fatalf("second call = %+v, want 200 with 2/2", a)
	}
	a := tag(t, srv, "shop-a.test", "3", "", body)
	if a.status != http.StatusTooManyRequests || a.retryAfter != "0.250" || a.callLimit != "2/2" {
		t.Fatalf("third call = %+v, want 429 with Retry-After 0.250 and 2/2", a)
	}
	if a := tag(t, srv, "shop-b.test", "1", "", body); a.status != 200 {
		t.Fatalf("another shop's call = %+v, want 200", a)
	}
	clk.Add(250 * time.Millisecond)
	if a := tag(t, srv, "shop-a.test", "3", "", body); a.status != 200 {
		t.Fatalf("after waiting the Retry-After = %+v, want 200", a)
	}
}

// P5-5, AC-7.3: a repeated Idempotency-Key replays the first answer and applies
// nothing; a different key applies again; calls without a key always apply.
func TestIdempotencyKeyReplays(t *testing.T) {
	a, srv, _ := newAPI(t, 100, 100)
	first := tag(t, srv, "shop-a.test", "7", "job-7-tag", `{"tag":"queued"}`)
	if first.status != 200 || first.replayed != "" {
		t.Fatalf("first = %+v", first)
	}
	again := tag(t, srv, "shop-a.test", "7", "job-7-tag", `{"tag":"queued"}`)
	if again.status != 200 || again.replayed != "true" || again.body != first.body {
		t.Fatalf("repeat = %+v, want the first answer replayed", again)
	}
	// A repeat with a different body is still a replay: the key decides.
	if r := tag(t, srv, "shop-a.test", "7", "job-7-tag", `{"tag":"other"}`); r.replayed != "true" || r.body != first.body {
		t.Fatalf("repeat with another body = %+v", r)
	}
	tag(t, srv, "shop-a.test", "8", "job-8-tag", `{"tag":"queued"}`)
	tag(t, srv, "shop-a.test", "9", "", `{"tag":"queued"}`)
	tag(t, srv, "shop-a.test", "9", "", `{"tag":"queued"}`)

	m, ok := a.Mutations("shop-a.test")
	if !ok {
		t.Fatal("no log for shop-a")
	}
	if m.Applied["job-7-tag"] != 1 || m.Applied["job-8-tag"] != 1 || len(m.Applied) != 2 || m.Unkeyed != 2 || m.Replays != 2 {
		t.Errorf("log = %+v, want job-7 and job-8 applied once, 2 unkeyed, 2 replays", m)
	}
	// The same key is separate per shop.
	tag(t, srv, "shop-b.test", "7", "job-7-tag", `{"tag":"queued"}`)
	if m, _ := a.Mutations("shop-b.test"); m.Applied["job-7-tag"] != 1 {
		t.Errorf("shop-b log = %+v", m)
	}
}

// AC-7.3 under concurrency: many calls with one key at once apply it once.
func TestConcurrentRepeatsApplyOnce(t *testing.T) {
	a, srv, _ := newAPI(t, 1000, 1000)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if r := tag(t, srv, "shop-a.test", "5", "job-5-tag", `{"tag":"queued"}`); r.status != 200 {
				t.Errorf("status %d", r.status)
			}
		})
	}
	wg.Wait()
	if m, _ := a.Mutations("shop-a.test"); m.Applied["job-5-tag"] != 1 || m.Replays != 49 {
		t.Errorf("log = %+v, want 1 applied and 49 replays", m)
	}
}

// A throttled call is not applied, and its key stays free: the retry applies.
func TestThrottledCallLeavesTheKeyFree(t *testing.T) {
	a, srv, clk := newAPI(t, 1, 1)
	tag(t, srv, "shop-a.test", "1", "", `{"tag":"queued"}`)
	if r := tag(t, srv, "shop-a.test", "2", "job-2-tag", `{"tag":"queued"}`); r.status != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", r.status)
	}
	clk.Add(time.Second)
	if r := tag(t, srv, "shop-a.test", "2", "job-2-tag", `{"tag":"queued"}`); r.status != 200 || r.replayed != "" {
		t.Fatalf("retry = %+v, want a fresh 200", r)
	}
	if m, _ := a.Mutations("shop-a.test"); m.Applied["job-2-tag"] != 1 {
		t.Errorf("log = %+v", m)
	}
}

func TestTagErrors(t *testing.T) {
	_, srv, _ := newAPI(t, 100, 100)
	cases := []struct {
		shop, order, body string
		want              int
	}{
		{"nope.test", "1", `{"tag":"x"}`, http.StatusNotFound},
		{"shop-a.test", "0", `{"tag":"x"}`, http.StatusNotFound},
		{"shop-a.test", "abc", `{"tag":"x"}`, http.StatusNotFound},
		{"shop-a.test", "1", `{}`, http.StatusUnprocessableEntity},
		{"shop-a.test", "1", `{"tag":""}`, http.StatusUnprocessableEntity},
		{"shop-a.test", "1", `{"tag":"x","extra":1}`, http.StatusUnprocessableEntity},
		{"shop-a.test", "1", `not json`, http.StatusUnprocessableEntity},
		{"shop-a.test", "1", `{"tag":"` + strings.Repeat("x", 256) + `"}`, http.StatusUnprocessableEntity},
	}
	for _, c := range cases {
		if a := tag(t, srv, c.shop, c.order, "", c.body); a.status != c.want {
			t.Errorf("POST %s/%s %s = %d, want %d", c.shop, c.order, c.body, a.status, c.want)
		}
	}
}

// GET /mock/v1/mutations?shop= returns the shop's log as JSON.
func TestMutationsEndpoint(t *testing.T) {
	_, srv, _ := newAPI(t, 100, 100)
	tag(t, srv, "shop-a.test", "1", "job-1-tag", `{"tag":"queued"}`)
	get := func(q string) (int, Mutations) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+MutationsPath+q, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var m Mutations
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	if status, m := get("?shop=shop-a.test"); status != 200 || m.Shop != "shop-a.test" || m.Applied["job-1-tag"] != 1 {
		t.Errorf("GET = %d %+v", status, m)
	}
	if status, _ := get("?shop=nope.test"); status != http.StatusNotFound {
		t.Errorf("unknown shop = %d, want 404", status)
	}
	if status, _ := get(""); status != http.StatusUnprocessableEntity {
		t.Errorf("no shop = %d, want 422", status)
	}
}
