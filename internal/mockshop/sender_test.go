package mockshop

import (
	"context"
	"encoding/json"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

var settings = config.Mockshop{
	RetryHorizonMS: 10_000, RetryBaseMS: 1000, RetryCapMS: 4000, DeliveryTimeoutMS: 200,
}

// delivery is one POST the test receiver saw.
type delivery struct {
	id, shop, topic, hmac string
	body                  []byte
}

// receiver answers each POST with the next status in script (the last one
// repeats), with retryAfter as Retry-After, and records what it got.
type receiver struct {
	mu         sync.Mutex
	script     []int
	retryAfter string
	got        []delivery
	srv        *httptest.Server
}

func newReceiver(t *testing.T, retryAfter string, script ...int) *receiver {
	t.Helper()
	r := &receiver{script: script, retryAfter: retryAfter}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.got = append(r.got, delivery{req.Header.Get(webhook.HeaderWebhookID), req.Header.Get(webhook.HeaderShopDomain),
			req.Header.Get(webhook.HeaderTopic), req.Header.Get(webhook.HeaderHMAC), body})
		status := r.script[min(len(r.got)-1, len(r.script)-1)]
		r.mu.Unlock()
		if r.retryAfter != "" {
			w.Header().Set("Retry-After", r.retryAfter)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) deliveries() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

// fakeTime makes Sleep advance a fake clock instead of waiting, and records
// every wait.
type fakeTime struct {
	mu    sync.Mutex
	now   time.Time
	waits []time.Duration
}

func (f *fakeTime) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeTime) Sleep(ctx context.Context, d time.Duration) bool {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.waits = append(f.waits, d)
	f.mu.Unlock()
	return ctx.Err() == nil
}

func newSender(r *receiver, m config.Mockshop, ft *fakeTime) *Sender {
	return &Sender{
		Target: r.srv.URL, Secret: []byte("dummy"), Settings: m, HTTP: r.srv.Client(),
		Rand: mrand.New(mrand.NewPCG(1, 2)), Sleep: ft.Sleep, Now: ft.Now, //nolint:gosec // seeded test odds
	}
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// A webhook is signed over its exact body with the secret, and carries the
// Shopify headers and a random v4 UUID as its ID (design §12).
func TestWebhookIsSigned(t *testing.T) {
	r := newReceiver(t, "", 200)
	ft := &fakeTime{now: t0}
	s := newSender(r, settings, ft)
	if err := s.Emit(t.Context(), "shop-a.test", 1001); err != nil {
		t.Fatal(err)
	}
	got := r.deliveries()
	if len(got) != 1 {
		t.Fatalf("%d deliveries, want 1", len(got))
	}
	d := got[0]
	if !webhook.Verify([]byte("dummy"), d.body, d.hmac) || d.shop != "shop-a.test" || d.topic != webhook.TopicOrdersCreate || !uuidPattern.MatchString(d.id) {
		t.Errorf("delivery = %+v", d)
	}
	var body OrderBody
	if err := json.Unmarshal(d.body, &body); err != nil || body.ID != 1001 || body.Name != "#1001" {
		t.Errorf("body = %s (%v)", d.body, err)
	}
	if st := s.Stats(); st.Webhooks != 1 || st.Delivered != 1 || st.Attempts != 1 || st.Duplicates != 0 {
		t.Errorf("stats = %+v", st)
	}
	if w2, _ := NewWebhook([]byte("dummy"), "shop-a.test", 1001, t0); w2.ID == d.id {
		t.Error("two webhooks got the same ID")
	}
}

// A failed delivery is retried with exponential backoff, and every retry is the
// same webhook: same ID, body and signature.
func TestDeliveryRetriesWithBackoff(t *testing.T) {
	r := newReceiver(t, "", 500, 503, 200)
	ft := &fakeTime{now: t0}
	s := newSender(r, settings, ft)
	wh, _ := NewWebhook(s.Secret, "shop-a.test", 1, t0)
	out := s.Deliver(t.Context(), wh)
	if !out.Delivered || out.Attempts != 3 || out.Status != 200 {
		t.Fatalf("outcome = %+v", out)
	}
	if want := []time.Duration{time.Second, 2 * time.Second}; !slices.Equal(ft.waits, want) {
		t.Errorf("waits = %v, want %v", ft.waits, want)
	}
	for _, d := range r.deliveries() {
		if d.id != wh.ID || d.hmac != wh.HMAC || string(d.body) != string(wh.Body) {
			t.Errorf("a retry differed from the webhook: %+v", d)
		}
	}
	if st := s.Stats(); st.Retries != 2 || st.Attempts != 3 || st.Delivered != 1 {
		t.Errorf("stats = %+v", st)
	}
}

// A 429's Retry-After wins over a shorter backoff (design §12).
func TestDeliveryHonoursRetryAfter(t *testing.T) {
	r := newReceiver(t, "5", 429, 200)
	ft := &fakeTime{now: t0}
	s := newSender(r, settings, ft)
	wh, _ := NewWebhook(s.Secret, "shop-a.test", 1, t0)
	if out := s.Deliver(t.Context(), wh); !out.Delivered {
		t.Fatalf("outcome = %+v", out)
	}
	if !slices.Equal(ft.waits, []time.Duration{5 * time.Second}) {
		t.Errorf("waits = %v, want [5s]", ft.waits)
	}
}

// Retries stop when the next attempt would start past the horizon, counted from
// the first attempt: waits of 1, 2 and 4 s fit in 10 s, and a fourth 4 s wait
// would end at 11 s.
func TestDeliveryGivesUpAtTheHorizon(t *testing.T) {
	r := newReceiver(t, "", 500)
	ft := &fakeTime{now: t0}
	s := newSender(r, settings, ft)
	wh, _ := NewWebhook(s.Secret, "shop-a.test", 1, t0)
	out := s.Deliver(t.Context(), wh)
	if out.Delivered || out.Attempts != 4 || out.Status != 500 {
		t.Fatalf("outcome = %+v, want 4 failed attempts", out)
	}
	if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}; !slices.Equal(ft.waits, want) {
		t.Errorf("waits = %v, want %v", ft.waits, want)
	}
	if st := s.Stats(); st.GaveUp != 1 || st.Delivered != 0 {
		t.Errorf("stats = %+v", st)
	}
}

// A receiver slower than the delivery timeout counts as a failure, and is retried.
func TestDeliveryTimeoutIsRetried(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			select {
			case <-req.Context().Done():
			case <-time.After(2 * time.Second):
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	ft := &fakeTime{now: t0}
	s := &Sender{Target: srv.URL, Secret: []byte("x"), Settings: settings, HTTP: srv.Client(), Sleep: ft.Sleep, Now: ft.Now}
	wh, _ := NewWebhook(s.Secret, "shop-a.test", 1, t0)
	if out := s.Deliver(t.Context(), wh); !out.Delivered || out.Attempts != 2 {
		t.Fatalf("outcome = %+v, want delivered on the second attempt", out)
	}
}

// With both odds at 1, a webhook goes out three times: once, once more at the
// same time, and once after a delay of at most duplicate_delay_max_ms, all as
// the same webhook. With both at 0 it goes out once.
func TestDuplicates(t *testing.T) {
	r := newReceiver(t, "", 200)
	ft := &fakeTime{now: t0}
	m := settings
	m.DuplicateProbability, m.ConcurrentDuplicateProbability, m.DuplicateDelayMaxMS = 1, 1, 3000
	s := newSender(r, m, ft)
	if err := s.Emit(t.Context(), "shop-a.test", 9); err != nil {
		t.Fatal(err)
	}
	got := r.deliveries()
	if len(got) != 3 {
		t.Fatalf("%d deliveries, want 3", len(got))
	}
	for _, d := range got[1:] {
		if d.id != got[0].id || d.hmac != got[0].hmac || string(d.body) != string(got[0].body) {
			t.Errorf("a duplicate differed: %+v vs %+v", d, got[0])
		}
	}
	if len(ft.waits) != 1 || ft.waits[0] < 0 || ft.waits[0] > 3*time.Second {
		t.Errorf("the later copy waited %v, want one wait of at most 3s", ft.waits)
	}
	if st := s.Stats(); st.Webhooks != 1 || st.Duplicates != 2 || st.Delivered != 3 {
		t.Errorf("stats = %+v", st)
	}

	r2 := newReceiver(t, "", 200)
	s2 := newSender(r2, settings, &fakeTime{now: t0})
	for i := range 20 {
		_ = s2.Emit(t.Context(), "shop-a.test", int64(i))
	}
	if n := len(r2.deliveries()); n != 20 {
		t.Errorf("with zero odds: %d deliveries for 20 webhooks", n)
	}
}

// Run sends for every shop with a positive rate, and none for a shop at 0.
func TestRunSendsAtTheShopRates(t *testing.T) {
	r := newReceiver(t, "", 200)
	s := &Sender{
		Target: r.srv.URL, Secret: []byte("x"), Settings: settings, HTTP: r.srv.Client(),
		Shops: []config.Shop{{ID: "fast.test", WebhooksPerSec: 100}, {ID: "off.test", WebhooksPerSec: 0}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	s.Run(ctx)
	shops := map[string]int{}
	for _, d := range r.deliveries() {
		shops[d.shop]++
	}
	if shops["fast.test"] < 5 || shops["off.test"] != 0 {
		t.Errorf("deliveries per shop = %v, want several for fast.test and none for off.test", shops)
	}
}
