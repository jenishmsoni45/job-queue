package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

func serveWebhooks(t *testing.T, env *queuetest.Env) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	api.New(env.Pool, queuetest.Logger(t)).WithWebhookSecret([]byte(queuetest.WebhookSecret)).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// AC-8.3: a valid, new orders/create webhook creates one order.tag job with
// payload {order_id}, keyed by its webhook ID, and returns 200.
func TestWebhookCreatesAnOrderTagJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serveWebhooks(t, env)

	status, header, body := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{
		Shop: shopA, ID: "b54557e4-bdd9-4b37-8a5f-bf7d70bcd043",
		Body: []byte(`{"id": 820982911946154508, "name": "#1001", "line_items": [{"id": 1}], "note": null}`),
	})
	if status != http.StatusOK || body["duplicate"] != nil {
		t.Fatalf("status %d, body %v; want 200 without duplicate", status, body)
	}
	id := int64(body["id"].(float64))
	if header.Get("Location") == "" {
		t.Error("no Location header")
	}
	j := env.Job(t, id)
	var payload api.OrderTagPayload
	if err := json.Unmarshal(j.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if j.Kind != api.OrderTagKind || j.Shop != shopA || j.State != "queued" || payload.OrderID != 820982911946154508 {
		t.Errorf("job = %+v, payload %+v", j, payload)
	}
	var key string
	if err := env.Pool.QueryRow(t.Context(), `SELECT key FROM dedupe_keys WHERE job_id = $1`, id).Scan(&key); err != nil || key != "b54557e4-bdd9-4b37-8a5f-bf7d70bcd043" {
		t.Errorf("dedupe key = %q (%v), want the webhook ID", key, err)
	}
}

// P5-3, AC-8.1: a missing HMAC, a wrong one, a tampered body and a wrong secret
// each get 401 and create nothing. The check runs before the body is parsed or
// the shop looked up, so even a garbage body or an unknown shop gets 401.
func TestWebhookHMACFailuresAre401(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serveWebhooks(t, env)
	good := []byte(`{"id": 5}`)
	cases := map[string]queuetest.Webhook{
		"missing HMAC":  {Shop: shopA, ID: "w-1", Body: good, HMAC: "-"},
		"wrong HMAC":    {Shop: shopA, ID: "w-2", Body: good, HMAC: webhook.Sign([]byte(queuetest.WebhookSecret), []byte(`{"id": 6}`))},
		"tampered body": {Shop: shopA, ID: "w-3", Body: []byte(`{"id": 50}`), HMAC: webhook.Sign([]byte(queuetest.WebhookSecret), good)},
		"wrong secret":  {Shop: shopA, ID: "w-4", Body: good, HMAC: webhook.Sign([]byte("not-the-secret"), good)},
		"not base64":    {Shop: shopA, ID: "w-5", Body: good, HMAC: "!!!"},
		"garbage body":  {Shop: shopA, ID: "w-6", Body: []byte(`not json`), HMAC: webhook.Sign([]byte("other"), []byte(`not json`))},
		"unknown shop":  {Shop: "nope.test", ID: "w-7", Body: good, HMAC: "-"},
	}
	for name, w := range cases {
		if status, _, _ := queuetest.PostWebhook(t, srv.Client(), srv.URL, w); status != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", name, status)
		}
	}
	if n, k := countJobs(t, env), countKeys(t, env); n != 0 || k != 0 {
		t.Errorf("%d jobs and %d keys created, want none", n, k)
	}
}

// A server with no secret refuses every webhook, even one signed with the empty key.
func TestWebhookWithoutASecretIs401(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serve(t, env) // no WithWebhookSecret
	body := []byte(`{"id": 1}`)
	if status, _, _ := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{
		Shop: shopA, ID: "w", Body: body, HMAC: webhook.Sign(nil, body),
	}); status != http.StatusUnauthorized {
		t.Errorf("status %d, want 401", status)
	}
}

// Signed but unusable webhooks: 422 for a topic other than orders/create, bad
// headers or a body without a positive integer id; 404 for an unknown shop; 413
// for an oversized body. None creates anything.
func TestWebhookValidation(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serveWebhooks(t, env)
	cases := []struct {
		name string
		w    queuetest.Webhook
		want int
	}{
		{"other topic", queuetest.Webhook{Shop: shopA, ID: "v-1", Topic: "orders/updated"}, 422},
		{"no shop header", queuetest.Webhook{ID: "v-2"}, 422},
		{"no webhook ID", queuetest.Webhook{Shop: shopA}, 422},
		{"webhook ID too long", queuetest.Webhook{Shop: shopA, ID: strings.Repeat("x", 256)}, 422},
		{"unknown shop", queuetest.Webhook{Shop: "nope.test", ID: "v-3"}, 404},
		{"no id", queuetest.Webhook{Shop: shopA, ID: "v-4", Body: []byte(`{"name": "#1"}`)}, 422},
		{"zero id", queuetest.Webhook{Shop: shopA, ID: "v-5", Body: []byte(`{"id": 0}`)}, 422},
		{"negative id", queuetest.Webhook{Shop: shopA, ID: "v-6", Body: []byte(`{"id": -3}`)}, 422},
		{"fractional id", queuetest.Webhook{Shop: shopA, ID: "v-7", Body: []byte(`{"id": 1.5}`)}, 422},
		{"string id", queuetest.Webhook{Shop: shopA, ID: "v-8", Body: []byte(`{"id": "7"}`)}, 422},
		{"id beyond int64", queuetest.Webhook{Shop: shopA, ID: "v-9", Body: []byte(`{"id": 99999999999999999999}`)}, 422},
		{"array body", queuetest.Webhook{Shop: shopA, ID: "v-10", Body: []byte(`[{"id": 1}]`)}, 422},
		{"trailing data", queuetest.Webhook{Shop: shopA, ID: "v-11", Body: []byte(`{"id": 1} {}`)}, 422},
		{"malformed JSON", queuetest.Webhook{Shop: shopA, ID: "v-12", Body: []byte(`{"id": 1`)}, 422},
		{"oversized body", queuetest.Webhook{Shop: shopA, ID: "v-13", Body: []byte(`{"id": 1, "pad": "` + strings.Repeat("x", api.MaxBodyBytes) + `"}`)}, 413},
	}
	for _, c := range cases {
		if status, _, _ := queuetest.PostWebhook(t, srv.Client(), srv.URL, c.w); status != c.want {
			t.Errorf("%s: status %d, want %d", c.name, status, c.want)
		}
	}
	if n, k := countJobs(t, env), countKeys(t, env); n != 0 || k != 0 {
		t.Errorf("%d jobs and %d keys created, want none", n, k)
	}
}

// AC-8.4: at the shop's backlog limit, a new webhook gets 429 with Retry-After
// and creates nothing, and a redelivery of an existing webhook still gets 200
// with its job.
func TestWebhookAtTheBacklogLimit(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, queuetest.WithShop(shopA, func(s *config.Shop) {
		s.BacklogLimit, s.BacklogRetryAfterMS = 1, 3000
	})))
	srv := serveWebhooks(t, env)
	status, _, first := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{Shop: shopA, ID: "first"})
	if status != http.StatusOK {
		t.Fatalf("first webhook: %d", status)
	}
	status, header, _ := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{Shop: shopA, ID: "second", Body: []byte(`{"id": 2}`)})
	if status != http.StatusTooManyRequests || header.Get("Retry-After") != "3" {
		t.Errorf("new webhook at the limit: %d, Retry-After %q; want 429, 3", status, header.Get("Retry-After"))
	}
	status, _, dup := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{Shop: shopA, ID: "first"})
	if status != http.StatusOK || dup["id"] != first["id"] || dup["duplicate"] != true {
		t.Errorf("redelivery at the limit: %d %v, want 200 with the first job %v", status, dup, first["id"])
	}
	if n, k := countJobs(t, env), countKeys(t, env); n != 1 || k != 1 {
		t.Errorf("%d jobs and %d keys, want 1 and 1", n, k)
	}
}
