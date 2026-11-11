package handlers_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/handlers"
	"github.com/jenishmsoni45/job-queue/internal/mockshop"
	"github.com/jenishmsoni45/job-queue/internal/queue"
)

const shop = "shop-a.test"

func newMockshop(t *testing.T, capacity int, leak float64) (*mockshop.API, *adminapi.Client) {
	t.Helper()
	api := mockshop.NewAPI(&config.File{Shops: []config.Shop{{ID: shop, MockAPICapacity: capacity, MockAPILeakPerSec: leak}}}, nil)
	mux := http.NewServeMux()
	api.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return api, &adminapi.Client{BaseURL: srv.URL, HTTP: srv.Client()}
}

func run(id int64, payload string) *queue.Run {
	return &queue.Run{Job: queue.Job{ID: id, Shop: shop, Kind: handlers.OrderTag, Payload: []byte(payload), ClaimSeq: 1}}
}

// D-020, AC-7.3: order.tag tags the order with the key job-<id>-tag, and a
// second run of the same job applies nothing more.
func TestOrderTagTagsOncePerJob(t *testing.T) {
	api, client := newMockshop(t, 100, 100)
	h := handlers.OrderTagHandler(client, 0)
	for range 2 {
		if err := h(t.Context(), run(42, `{"order_id": 1001}`)); err != nil {
			t.Fatal(err)
		}
	}
	m, _ := api.Mutations(shop)
	if m.Applied["job-42-tag"] != 1 || len(m.Applied) != 1 || m.Replays != 1 || m.Unkeyed != 0 {
		t.Errorf("mutation log = %+v, want job-42-tag applied once and one replay", m)
	}
}

// A full bucket makes the handler return Throttled with mockshop's Retry-After
// (D-004), not an ordinary failure.
func TestOrderTagThrottled(t *testing.T) {
	_, client := newMockshop(t, 1, 2)
	h := handlers.OrderTagHandler(client, 0)
	if err := h(t.Context(), run(1, `{"order_id": 1}`)); err != nil {
		t.Fatal(err)
	}
	err := h(t.Context(), run(2, `{"order_id": 2}`))
	d, ok := queue.IsThrottled(err)
	if !ok || d <= 0 || d > 500*time.Millisecond {
		t.Fatalf("second call: %v, want Throttled with a wait in (0, 500ms]", err)
	}
	if queue.IsPermanent(err) {
		t.Error("a throttle was reported as permanent")
	}
}

// Design §8 and §14: a 4xx other than 429 is permanent; a 5xx or no answer is
// a temporary failure; a bad payload is permanent.
func TestOrderTagErrors(t *testing.T) {
	status := http.StatusNotFound
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	defer srv.Close()
	h := handlers.OrderTagHandler(&adminapi.Client{BaseURL: srv.URL, HTTP: srv.Client()}, 0)

	if err := h(t.Context(), run(1, `{"order_id": 1}`)); !queue.IsPermanent(err) {
		t.Errorf("404: %v, want permanent", err)
	}
	status = http.StatusServiceUnavailable
	if err := h(t.Context(), run(1, `{"order_id": 1}`)); err == nil || queue.IsPermanent(err) {
		t.Errorf("503: %v, want a temporary failure", err)
	}
	if _, ok := queue.IsThrottled(handlers.Result(errors.New("boom"))); ok {
		t.Error("a transport error became a throttle")
	}
	srv.Close()
	if err := h(t.Context(), run(1, `{"order_id": 1}`)); err == nil || queue.IsPermanent(err) {
		t.Errorf("mockshop down: %v, want a temporary failure", err)
	}
	for _, p := range []string{`{}`, `{"order_id": 0}`, `{"order_id": -1}`, `{"order_id": "1"}`, `{"order_id": 1, "x": 2}`, `[]`} {
		if err := h(t.Context(), run(1, p)); !queue.IsPermanent(err) {
			t.Errorf("payload %s: %v, want permanent", p, err)
		}
	}
	if err := handlers.OrderTagHandler(nil, 0)(t.Context(), run(1, `{"order_id": 1}`)); !queue.IsPermanent(err) {
		t.Errorf("no client: %v, want permanent", err)
	}
}

// The handler sleeps its kind's handler_sleep_ms first, and stops sleeping when
// its context is cancelled (a lost lease or the hard timeout).
func TestOrderTagSleepsAndStopsOnCancel(t *testing.T) {
	_, client := newMockshop(t, 100, 100)
	reg := queue.NewRegistry()
	handlers.Register(reg, &config.File{Kinds: []config.Kind{{Kind: handlers.OrderTag, HandlerSleepMS: 60}}}, client)
	h, ok := reg.Lookup(handlers.OrderTag)
	if !ok {
		t.Fatal("order.tag not registered")
	}
	start := time.Now()
	if err := h(t.Context(), run(1, `{"order_id": 1}`)); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 60*time.Millisecond {
		t.Errorf("handler took %v, want at least the 60ms sleep", took)
	}

	cause := errors.New("lease lost")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	if err := handlers.OrderTagHandler(client, time.Hour)(ctx, run(2, `{"order_id": 2}`)); !errors.Is(err, cause) {
		t.Errorf("cancelled: %v, want the context's cause", err)
	}
}
