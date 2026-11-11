package adminapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
)

// Retry-After parsing (design §16): decimal seconds as mockshop sends them, or
// an HTTP date; anything else is refused.
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 12, 21, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"2", 2 * time.Second, true},
		{"0.25", 250 * time.Millisecond, true},
		{"0.0001", 100 * time.Microsecond, true},
		{" 1.5 ", 1500 * time.Millisecond, true},
		{"0", 0, true},
		{"Mon, 21 Dec 2026 10:00:03 GMT", 3 * time.Second, true},
		{"Mon, 21 Dec 2026 09:59:00 GMT", 0, true}, // in the past: no wait
		{"99999999999", 24 * time.Hour, true},      // capped
		{"", 0, false},
		{"-1", 0, false},
		{"soon", 0, false},
		{"NaN", 0, false},
		{"Inf", 0, false},
		{"1e3", 0, false},
		{"0x10", 0, false},
	}
	for _, c := range cases {
		got, ok := adminapi.ParseRetryAfter(c.in, now)
		if ok != c.ok || got != c.want {
			t.Errorf("ParseRetryAfter(%q) = %v, %v; want %v, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestTagOrder(t *testing.T) {
	type seen struct {
		path, key, tag string
	}
	var last seen
	status, retryAfter := http.StatusOK, ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body adminapi.TagRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		last = seen{r.URL.EscapedPath(), r.Header.Get(adminapi.HeaderIdempotencyKey), body.Tag}
		if retryAfter != "" {
			w.Header().Set(adminapi.HeaderRetryAfter, retryAfter)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"errors":"nope"}`))
	}))
	defer srv.Close()
	c := &adminapi.Client{BaseURL: srv.URL + "/", HTTP: srv.Client()}
	ctx := context.Background()

	if err := c.TagOrder(ctx, "shop-01.mockshop.test", 1001, "queued", "job-7-tag"); err != nil {
		t.Fatalf("200: %v", err)
	}
	if want := (seen{"/admin/api/mock/shops/shop-01.mockshop.test/orders/1001/tags", "job-7-tag", "queued"}); last != want {
		t.Errorf("request = %+v, want %+v", last, want)
	}

	status, retryAfter = http.StatusTooManyRequests, "0.125"
	var th *adminapi.ThrottledError
	if err := c.TagOrder(ctx, "s.test", 1, "queued", ""); !errors.As(err, &th) || th.RetryAfter != 125*time.Millisecond {
		t.Errorf("429 with Retry-After: %v, want a ThrottledError of 125ms", err)
	}
	if last.key != "" {
		t.Errorf("an empty key was sent as %q", last.key)
	}
	retryAfter = ""
	if err := c.TagOrder(ctx, "s.test", 1, "queued", ""); !errors.As(err, &th) || th.RetryAfter != adminapi.DefaultRetryAfter {
		t.Errorf("429 without Retry-After: %v, want the default wait", err)
	}

	var se *adminapi.StatusError
	status = http.StatusNotFound
	if err := c.TagOrder(ctx, "s.test", 1, "queued", ""); !errors.As(err, &se) || !se.ClientError() || se.Body != `{"errors":"nope"}` {
		t.Errorf("404: %v, want a client StatusError with the body", err)
	}
	status = http.StatusServiceUnavailable
	if err := c.TagOrder(ctx, "s.test", 1, "queued", ""); !errors.As(err, &se) || se.ClientError() {
		t.Errorf("503: %v, want a server StatusError", err)
	}

	srv.Close()
	if err := c.TagOrder(ctx, "s.test", 1, "queued", ""); err == nil || errors.As(err, &se) || errors.As(err, &th) {
		t.Errorf("no server: %v, want a transport error", err)
	}
}
