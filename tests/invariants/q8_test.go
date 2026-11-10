package invariants

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
)

// serveAPI serves the api role's endpoints, with the backlog limit of shopA set to limit.
func serveAPI(t *testing.T, limit int) (*queuetest.Env, *httptest.Server) {
	t.Helper()
	env := queuetest.New(t, queuetest.Config(t, queuetest.WithShop(shopA, func(s *config.Shop) {
		s.BacklogLimit, s.BacklogRetryAfterMS = limit, 3000
	})))
	mux := http.NewServeMux()
	api.New(env.Pool, queuetest.Logger(t)).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return env, srv
}

func post(t testing.TB, srv *httptest.Server, path, body string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Retry-After"), string(raw)
}

func single(shop, key string) string {
	dedupe := ""
	if key != "" {
		dedupe = fmt.Sprintf(`, "dedupe_key": %q`, key)
	}
	return fmt.Sprintf(`{"shop": %q, "kind": "test.sleep", "payload": {}%s}`, shop, dedupe)
}

func batchBody(shop string, keys ...string) string {
	jobs := make([]string, len(keys))
	for i, k := range keys {
		dedupe := ""
		if k != "" {
			dedupe = fmt.Sprintf(`, "dedupe_key": %q`, k)
		}
		jobs[i] = `{"kind": "test.sleep", "payload": {}` + dedupe + `}`
	}
	return fmt.Sprintf(`{"shop": %q, "jobs": [%s]}`, shop, strings.Join(jobs, ","))
}

func counts(t *testing.T, env *queuetest.Env) (jobs, keys, shed int) {
	t.Helper()
	if err := env.Pool.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM dedupe_keys), (SELECT coalesce(sum(shed_total), 0) FROM shop_state)`,
	).Scan(&jobs, &keys, &shed); err != nil {
		t.Fatal(err)
	}
	return jobs, keys, shed
}

// P4-3, Q8 (NFR-8, AC-11.1, AC-2.2): fill a shop to its backlog limit. The next
// single and batch enqueues get 429 with Retry-After and create no job and no
// dedupe key. A duplicate of a job that exists still gets 200. Another shop is
// unaffected. (The webhook receiver gets the same check in phase 5, when it exists.)
func TestQ8AtTheBacklogLimitEnqueueReturns429AndCreatesNothing(t *testing.T) {
	t.Parallel()
	const limit = 6
	env, srv := serveAPI(t, limit)
	// Fill it with a single job, a keyed single job and a batch.
	if status, _, body := post(t, srv, "/v1/jobs", single(shopA, "")); status != http.StatusCreated {
		t.Fatalf("status %d, %s", status, body)
	}
	if status, _, body := post(t, srv, "/v1/jobs", single(shopA, "keyed")); status != http.StatusCreated {
		t.Fatalf("status %d, %s", status, body)
	}
	if status, _, body := post(t, srv, "/v1/jobs/batch", batchBody(shopA, "b1", "b2", "", "")); status != http.StatusCreated {
		t.Fatalf("status %d, %s", status, body)
	}
	jobs0, keys0, shed0 := counts(t, env)
	if jobs0 != limit || keys0 != 3 || shed0 != 0 {
		t.Fatalf("after filling: %d jobs, %d keys, %d shed; want %d, 3, 0", jobs0, keys0, shed0, limit)
	}

	rejected := []struct {
		name, path, body string
		shed             int
	}{
		{"single", "/v1/jobs", single(shopA, ""), 1},
		{"single with a new key", "/v1/jobs", single(shopA, "new"), 1},
		{"batch of one", "/v1/jobs/batch", batchBody(shopA, ""), 1},
		{"batch with new keys", "/v1/jobs/batch", batchBody(shopA, "n1", "n2", "n3"), 3},
		{"batch where only some are new", "/v1/jobs/batch", batchBody(shopA, "b1", "n4", "", "keyed"), 2},
	}
	wantShed := 0
	for _, tc := range rejected {
		status, retryAfter, body := post(t, srv, tc.path, tc.body)
		if status != http.StatusTooManyRequests || retryAfter != "3" {
			t.Fatalf("%s: status %d, Retry-After %q, %s; want 429 and 3", tc.name, status, retryAfter, body)
		}
		wantShed += tc.shed
		jobs, keys, shed := counts(t, env)
		if jobs != jobs0 || keys != keys0 || shed != wantShed {
			t.Fatalf("%s: %d jobs, %d keys, %d shed; want %d, %d, %d: a rejected call creates nothing", tc.name, jobs, keys, shed, jobs0, keys0, wantShed)
		}
	}
	env.CheckBacklog(t)

	// Duplicates are answered, not shed.
	if status, _, body := post(t, srv, "/v1/jobs", single(shopA, "keyed")); status != http.StatusOK {
		t.Fatalf("duplicate single at the limit: status %d, %s; want 200", status, body)
	}
	if status, _, body := post(t, srv, "/v1/jobs/batch", batchBody(shopA, "b1", "b2")); status != http.StatusOK {
		t.Fatalf("all-duplicate batch at the limit: status %d, %s; want 200", status, body)
	}
	if jobs, keys, shed := counts(t, env); jobs != jobs0 || keys != keys0 || shed != wantShed {
		t.Fatalf("after duplicates: %d jobs, %d keys, %d shed; want %d, %d, %d", jobs, keys, shed, jobs0, keys0, wantShed)
	}

	// Another shop is unaffected (AC-11.2).
	if status, _, body := post(t, srv, "/v1/jobs", single(shopB, "")); status != http.StatusCreated {
		t.Fatalf("shop B: status %d, %s", status, body)
	}
}

// Q8 under concurrency: single and batch enqueues racing against a small limit.
// The jobs created never pass it, every 429 left no job and no key, and shed_total
// counts exactly the jobs the 429s refused.
func TestQ8ConcurrentEnqueuesNeverPassTheLimitAndRejectionsLeaveNothing(t *testing.T) {
	t.Parallel()
	const limit = 25
	env, srv := serveAPI(t, limit)

	var mu sync.Mutex
	created, keysCreated, shed := 0, 0, 0
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 10 {
				tag := fmt.Sprintf("g%d-i%d", g, i)
				var path, body string
				newJobs, newKeys := 1, 0
				switch (g + i) % 3 {
				case 0:
					path, body = "/v1/jobs", single(shopA, "")
				case 1:
					path, body, newKeys = "/v1/jobs", single(shopA, tag), 1
				default:
					size := 1 + (g+i)%4
					keys := make([]string, size)
					for k := range keys {
						if k%2 == 0 {
							keys[k] = fmt.Sprintf("%s-%d", tag, k)
							newKeys++
						}
					}
					path, body, newJobs = "/v1/jobs/batch", batchBody(shopA, keys...), size
				}
				status, _, resp := post(t, srv, path, body)
				mu.Lock()
				switch status {
				case http.StatusCreated:
					created += newJobs
					keysCreated += newKeys
				case http.StatusTooManyRequests:
					shed += newJobs
				default:
					t.Errorf("%s: status %d, %s", tag, status, resp)
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	jobs, keys, shedTotal := counts(t, env)
	if jobs != created || jobs > limit {
		t.Fatalf("%d jobs in the database, %d created by 201s, limit %d", jobs, created, limit)
	}
	if keys != keysCreated {
		t.Fatalf("%d dedupe keys, %d created by 201s: a 429 left a key behind", keys, keysCreated)
	}
	if shedTotal != shed {
		t.Fatalf("shed_total %d, but the 429s refused %d jobs", shedTotal, shed)
	}
	if created == 0 || shed == 0 {
		t.Fatalf("created %d, shed %d: the test must hit the limit to prove anything (limit %d)", created, shed, limit)
	}
	env.CheckBacklog(t)
}
