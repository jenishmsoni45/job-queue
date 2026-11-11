package invariants

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/mockshop"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

// webhookServer serves the api role's endpoints, webhooks included, on env.
func webhookServer(t *testing.T, env *queuetest.Env) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	api.New(env.Pool, queuetest.Logger(t)).WithWebhookSecret([]byte(queuetest.WebhookSecret)).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// sender is mockshop's webhook sender aimed at srv, with fast retries.
func sender(srv *httptest.Server, m config.Mockshop) *mockshop.Sender {
	return &mockshop.Sender{
		Target: srv.URL + webhook.Path, Secret: []byte(queuetest.WebhookSecret), Settings: m, HTTP: srv.Client(),
	}
}

var fastRetries = config.Mockshop{RetryHorizonMS: 30_000, RetryBaseMS: 50, RetryCapMS: 2000, DeliveryTimeoutMS: 5000}

// jobsFor counts a shop's jobs and its dedupe keys for one webhook ID.
func jobsFor(t *testing.T, env *queuetest.Env, shop, key string) (jobs, keys int, id int64) {
	t.Helper()
	if err := env.Pool.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM jobs WHERE shop_id = $1),
		       (SELECT count(*) FROM dedupe_keys WHERE shop_id = $1 AND key = $2),
		       coalesce((SELECT job_id FROM dedupe_keys WHERE shop_id = $1 AND key = $2), 0)`, shop, key,
	).Scan(&jobs, &keys, &id); err != nil {
		t.Fatal(err)
	}
	return jobs, keys, id
}

// P5-4, Q4 end to end (NFR-4, AC-8.2, AC-8.3): mockshop's sender delivers one
// signed webhook k times to the real receiver, one after another and all at
// once, and also after its job has completed. Every delivery gets a 2xx, and
// the shop ends with exactly one order.tag job and one key for the webhook ID.
func TestQ4WebhookDeliveredKTimesMakesOneJob(t *testing.T) {
	t.Parallel()
	const k = 12
	for name, concurrent := range map[string]bool{"sequential": false, "concurrent": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := queuetest.New(t, nil)
			srv := webhookServer(t, env)
			s := sender(srv, fastRetries)
			wh, err := mockshop.NewWebhook(s.Secret, shopA, 1001, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			deliverK := func() {
				var wg sync.WaitGroup
				for range k {
					if !concurrent {
						if out := s.Deliver(t.Context(), wh); !out.Delivered || out.Status != http.StatusOK {
							t.Errorf("delivery: %+v", out)
						}
						continue
					}
					wg.Go(func() {
						if out := s.Deliver(t.Context(), wh); !out.Delivered || out.Status != http.StatusOK {
							t.Errorf("delivery: %+v", out)
						}
					})
				}
				wg.Wait()
			}
			deliverK()
			jobs, keys, id := jobsFor(t, env, shopA, wh.ID)
			if jobs != 1 || keys != 1 {
				t.Fatalf("after %d deliveries: %d jobs and %d keys, want 1 and 1", k, jobs, keys)
			}
			j := env.Job(t, id)
			if j.Kind != api.OrderTagKind || string(j.Payload) != `{"order_id": 1001}` {
				t.Errorf("job = kind %s, payload %s", j.Kind, j.Payload)
			}

			// The job runs to completion, and the webhook is delivered k more times.
			c := claimFor(t, env, shopA)
			if ok, err := store.Complete(t.Context(), env.Pool, c.Fence()); err != nil || !ok {
				t.Fatalf("complete: %v %v", ok, err)
			}
			deliverK()
			if jobs, keys, again := jobsFor(t, env, shopA, wh.ID); jobs != 1 || keys != 1 || again != id {
				t.Errorf("after completion and %d more deliveries: %d jobs, %d keys, job %d; want 1, 1, %d", k, jobs, keys, again, id)
			}
			env.CheckBacklog(t)
		})
	}
}

// claimFor claims one job of shop.
func claimFor(t *testing.T, env *queuetest.Env, shop string) store.Claimed {
	t.Helper()
	out, err := store.ClaimFor(t.Context(), env.Pool, []string{"q4"}, store.ClaimSpec{Shops: []string{shop}})
	if err != nil || len(out.Jobs) != 1 {
		t.Fatalf("claim: %+v (%v)", out, err)
	}
	return out.Jobs[0]
}

// Q4 with the sender's own duplicates: with both duplicate odds at 1, each
// emitted webhook goes out three times (once, once more at the same time, and
// once later), and still makes one job.
func TestQ4SenderDuplicatesMakeOneJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := webhookServer(t, env)
	m := fastRetries
	m.DuplicateProbability, m.ConcurrentDuplicateProbability, m.DuplicateDelayMaxMS = 1, 1, 50
	s := sender(srv, m)
	for i := range 10 {
		if err := s.Emit(t.Context(), shopB, int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.Webhooks != 10 || st.Duplicates != 20 || st.Delivered != 30 {
		t.Errorf("sender stats = %+v, want 10 webhooks sent 30 times", st)
	}
	var jobs, keys int
	if err := env.Pool.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM jobs WHERE shop_id = $1), (SELECT count(*) FROM dedupe_keys WHERE shop_id = $1)`,
		shopB).Scan(&jobs, &keys); err != nil {
		t.Fatal(err)
	}
	if jobs != 10 || keys != 10 {
		t.Errorf("%d jobs and %d keys, want 10 and 10", jobs, keys)
	}
}

// P5-4, AC-8.4, D-007: at the shop's backlog limit a new webhook gets 429 with
// Retry-After and creates nothing, while a redelivery of an existing one gets
// 200. mockshop retries the refused webhook, honouring Retry-After, and once
// the shop has room it is accepted: still exactly one job.
func TestQ4WebhookAtTheLimitIsRetriedIntoOneJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, queuetest.WithShop(shopC, func(s *config.Shop) {
		s.BacklogLimit, s.BacklogRetryAfterMS = 1, 1000
	})))
	srv := webhookServer(t, env)
	s := sender(srv, fastRetries)

	first, _ := mockshop.NewWebhook(s.Secret, shopC, 1, time.Now())
	if out := s.Deliver(t.Context(), first); !out.Delivered {
		t.Fatalf("first webhook: %+v", out)
	}
	if out := s.Deliver(t.Context(), first); !out.Delivered || out.Status != http.StatusOK {
		t.Errorf("redelivery at the limit: %+v, want 200", out)
	}
	second, _ := mockshop.NewWebhook(s.Secret, shopC, 2, time.Now())
	status, header, _ := queuetest.PostWebhook(t, srv.Client(), srv.URL, queuetest.Webhook{Shop: shopC, ID: second.ID, Body: second.Body, HMAC: second.HMAC})
	if status != http.StatusTooManyRequests || header.Get("Retry-After") != "1" {
		t.Fatalf("new webhook at the limit: %d, Retry-After %q; want 429, 1", status, header.Get("Retry-After"))
	}
	if jobs, keys, _ := jobsFor(t, env, shopC, second.ID); jobs != 1 || keys != 0 {
		t.Fatalf("after the 429: %d jobs and %d keys for it, want the first job only and no key", jobs, keys)
	}

	done := make(chan mockshop.Outcome, 1)
	go func() { done <- s.Deliver(context.Background(), second) }()
	time.Sleep(300 * time.Millisecond) // the sender is waiting out a 429 by now
	c := claimFor(t, env, shopC)
	if ok, err := store.Complete(t.Context(), env.Pool, c.Fence()); err != nil || !ok {
		t.Fatalf("complete: %v %v", ok, err)
	}
	select {
	case out := <-done:
		if !out.Delivered || out.Attempts < 2 {
			t.Errorf("retried delivery: %+v, want delivered after at least one 429", out)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the refused webhook was never delivered")
	}
	if jobs, keys, _ := jobsFor(t, env, shopC, second.ID); jobs != 2 || keys != 1 {
		t.Errorf("after the retry: %d jobs in the shop and %d keys for the webhook, want 2 and 1", jobs, keys)
	}
	env.CheckBacklog(t)
}
