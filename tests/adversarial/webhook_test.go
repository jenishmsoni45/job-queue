package adversarial

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

func webhookAPI(t *testing.T, env *queuetest.Env) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	api.New(env.Pool, queuetest.Logger(t)).WithWebhookSecret([]byte(queuetest.WebhookSecret)).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func rowCounts(t testing.TB, env *queuetest.Env) (jobs, keys int) {
	t.Helper()
	if err := env.Pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM dedupe_keys)`).Scan(&jobs, &keys); err != nil {
		t.Fatal(err)
	}
	return jobs, keys
}

// P5-3, AC-8.1 as a property: take a signed webhook body and tamper with it, by
// flipping a byte, cutting it short, adding to it or swapping in another body,
// and send it with the original signature. Every one gets 401, and nothing is
// ever created. Signing the original with the wrong secret fails the same way.
func TestTamperedWebhooksAreRefused(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := webhookAPI(t, env)
	secret := []byte(queuetest.WebhookSecret)

	rapid.Check(t, func(rt *rapid.T) {
		orig := []byte(fmt.Sprintf(`{"id": %d, "note": %q}`,
			rapid.Int64Range(1, 1<<53).Draw(rt, "order"), rapid.StringN(0, 40, -1).Draw(rt, "note")))
		sig := webhook.Sign(secret, orig)
		body := append([]byte(nil), orig...)
		switch rapid.IntRange(0, 4).Draw(rt, "tamper") {
		case 0: // flip one bit of one byte
			i := rapid.IntRange(0, len(body)-1).Draw(rt, "at")
			body[i] ^= byte(1 << rapid.IntRange(0, 7).Draw(rt, "bit"))
		case 1: // cut it short
			body = body[:rapid.IntRange(0, len(body)-1).Draw(rt, "len")]
		case 2: // add to it
			body = append(body, rapid.SliceOfN(rapid.Byte(), 1, 8).Draw(rt, "extra")...)
		case 3: // another body entirely
			body = []byte(fmt.Sprintf(`{"id": %d}`, rapid.Int64Range(1, 1<<53).Draw(rt, "other")))
			if bytes.Equal(body, orig) {
				body = append(body, ' ')
			}
		case 4: // the original body, signed with a wrong secret
			wrong := []byte(rapid.StringMatching(`[a-z0-9-]{1,40}`).Draw(rt, "secret"))
			if bytes.Equal(wrong, secret) {
				wrong = append(wrong, 'x')
			}
			sig = webhook.Sign(wrong, orig)
		}
		status, _, _ := queuetest.PostWebhook(rt, srv.Client(), srv.URL, queuetest.Webhook{
			Shop: queuetest.ShopA, ID: rapid.StringMatching(`[a-f0-9-]{36}`).Draw(rt, "id"), Body: body, HMAC: sig,
		})
		if status != http.StatusUnauthorized {
			rt.Fatalf("tampered webhook %q got %d, want 401", body, status)
		}
	})
	if jobs, keys := rowCounts(t, env); jobs != 0 || keys != 0 {
		t.Errorf("%d jobs and %d keys created by tampered webhooks, want none", jobs, keys)
	}
}

// Oversized bodies get 413 and create nothing, with or without a
// Content-Length; a body of exactly the max size is read in full and accepted.
func TestOversizedWebhookBodies(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := webhookAPI(t, env)
	secret := []byte(queuetest.WebhookSecret)
	pad := func(n int) []byte {
		prefix := `{"id": 7, "pad": "`
		return []byte(prefix + strings.Repeat("x", n-len(prefix)-2) + `"}`)
	}

	exact := pad(api.MaxBodyBytes)
	if status, _, _ := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{Shop: queuetest.ShopA, ID: "exact", Body: exact}); status != http.StatusOK {
		t.Errorf("a body of exactly %d bytes: %d, want 200", api.MaxBodyBytes, status)
	}

	over := pad(api.MaxBodyBytes + 1)
	for name, chunked := range map[string]bool{"with Content-Length": false, "chunked": true} {
		var body io.Reader = bytes.NewReader(over)
		if chunked {
			body = io.MultiReader(bytes.NewReader(over)) // hides the length, so the client streams it chunked
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+webhook.Path, body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(webhook.HeaderTopic, webhook.TopicOrdersCreate)
		req.Header.Set(webhook.HeaderShopDomain, queuetest.ShopA)
		req.Header.Set(webhook.HeaderWebhookID, "over-"+name)
		req.Header.Set(webhook.HeaderHMAC, webhook.Sign(secret, over))
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("%s: %d, want 413", name, resp.StatusCode)
		}
	}
	if jobs, keys := rowCounts(t, env); jobs != 1 || keys != 1 {
		t.Errorf("%d jobs and %d keys, want only the exact-size webhook's", jobs, keys)
	}
}

// Malformed bodies with a valid signature get 422 and create nothing: the HMAC
// proves who sent them, not that they make sense.
func TestMalformedSignedWebhooks(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := webhookAPI(t, env)
	for i, body := range []string{
		``, `null`, `true`, `"text"`, `123`, `[]`, `{`, `{"id":`, `{"id": 1}{"id": 2}`, `{"id": null}`,
		`{"id": {}}`, `{"id": []}`, `{"id": "1"}`, `{"id": 1.0}`, `{"id": 1e3}`, `{"id": -1}`, `{"id": 0}`,
		`{"id": 9223372036854775808}`, "\xff\xfe{}", `{"id": 1, "id": "x"}`,
	} {
		status, _, _ := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{
			Shop: queuetest.ShopA, ID: fmt.Sprintf("bad-%d", i), Body: []byte(body),
		})
		if status != http.StatusUnprocessableEntity {
			t.Errorf("body %q: %d, want 422", body, status)
		}
	}
	if jobs, keys := rowCounts(t, env); jobs != 0 || keys != 0 {
		t.Errorf("%d jobs and %d keys, want none", jobs, keys)
	}
}

// A replay storm through the webhook receiver (design §16, Q4): 200 copies of
// one signed webhook, all at once, make one job; every copy gets 200.
func TestWebhookReplayStorm(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := webhookAPI(t, env)
	body := []byte(`{"id": 4242}`)
	var wg sync.WaitGroup
	statuses := make([]int, 200)
	for i := range statuses {
		wg.Go(func() {
			statuses[i], _, _ = queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{
				Shop: queuetest.ShopB, ID: "storm", Body: body,
			})
		})
	}
	wg.Wait()
	for i, s := range statuses {
		if s != http.StatusOK {
			t.Errorf("copy %d: %d, want 200", i, s)
		}
	}
	if jobs, keys := rowCounts(t, env); jobs != 1 || keys != 1 {
		t.Errorf("%d jobs and %d keys, want 1 and 1", jobs, keys)
	}
}
