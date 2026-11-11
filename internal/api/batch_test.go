package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
)

const shopB = queuetest.ShopB

func withLimit(shop string, limit, retryAfterMS int) func(*config.File) {
	return func(c *config.File) {
		for i := range c.Shops {
			if c.Shops[i].ID == shop {
				c.Shops[i].BacklogLimit, c.Shops[i].BacklogRetryAfterMS = limit, retryAfterMS
			}
		}
	}
}

func shedTotal(t *testing.T, env *queuetest.Env, shop string) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT shed_total FROM shop_state WHERE shop_id = $1`, shop).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// batch builds a batch body for shop with one job per key (an empty key means no
// dedupe key).
func batch(shop string, keys ...string) string {
	jobs := make([]string, len(keys))
	for i, k := range keys {
		dedupe := ""
		if k != "" {
			dedupe = fmt.Sprintf(`, "dedupe_key": %q`, k)
		}
		jobs[i] = `{"kind": "test.sleep", "payload": {"i": ` + fmt.Sprint(i) + `}` + dedupe + `}`
	}
	return fmt.Sprintf(`{"shop": %q, "jobs": [%s]}`, shop, strings.Join(jobs, ", "))
}

func ids(t *testing.T, body map[string]any, field string) []int64 {
	t.Helper()
	list, ok := body[field].([]any)
	if !ok {
		t.Fatalf("%s is %T (%v), want a JSON array", field, body[field], body[field])
	}
	out := make([]int64, len(list))
	for i, v := range list {
		out[i] = int64(v.(float64))
	}
	return out
}

// P4-4, AC-2.1 and AC-2.3 (D-048): a batch creates all its jobs and returns
// their IDs in order; a repeat of it creates nothing, answers 200, and reports
// the existing IDs as duplicates.
func TestBatchCreatesJobsAndReportsDuplicates(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serve(t, env)

	status, _, body := do(t, srv, http.MethodPost, "/v1/jobs/batch", batch(shopA, "a", "", "c"))
	if status != http.StatusCreated {
		t.Fatalf("status %d, body %v; want 201", status, body)
	}
	first := ids(t, body, "ids")
	if len(first) != 3 || len(ids(t, body, "duplicates")) != 0 {
		t.Fatalf("body = %v, want 3 ids and no duplicates", body)
	}
	if n := countJobs(t, env); n != 3 {
		t.Fatalf("%d jobs, want 3", n)
	}
	env.CheckBacklog(t)

	// One new job among two that exist: 201, in request order.
	status, _, body = do(t, srv, http.MethodPost, "/v1/jobs/batch", batch(shopA, "a", "new", "c"))
	if status != http.StatusCreated {
		t.Fatalf("status %d, body %v; want 201", status, body)
	}
	got := ids(t, body, "ids")
	if got[0] != first[0] || got[2] != first[2] || got[1] == first[1] {
		t.Fatalf("ids %v, first batch %v: want a's and c's existing IDs around a new one", got, first)
	}
	if dups := ids(t, body, "duplicates"); len(dups) != 2 || dups[0] != first[0] || dups[1] != first[2] {
		t.Fatalf("duplicates %v, want %d and %d", dups, first[0], first[2])
	}

	// Every job a duplicate: 200, nothing created.
	status, _, body = do(t, srv, http.MethodPost, "/v1/jobs/batch", batch(shopA, "a", "c"))
	if status != http.StatusOK {
		t.Fatalf("all-duplicate batch: status %d, body %v; want 200", status, body)
	}
	if n := countJobs(t, env); n != 4 {
		t.Fatalf("%d jobs, want 4", n)
	}
	env.CheckBacklog(t)
}

// P4-4, AC-1.2, AC-1.3 and AC-2.4: every way a batch can be invalid is refused
// with the right status, and creates no job and no key.
func TestBatchRejectsInvalidBatchesAndCreatesNothing(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serve(t, env)

	tooMany := make([]string, api.MaxBatchSize+1)
	for i := range tooMany {
		tooMany[i] = `{"kind": "test.sleep", "payload": {}}`
	}
	exactlyMax := tooMany[:api.MaxBatchSize]
	huge := `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {"x": "` + strings.Repeat("x", api.MaxBodyBytes) + `"}}]}`

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"more than the maximum batch size", `{"shop": "shop-a.test", "jobs": [` + strings.Join(tooMany, ",") + `]}`, 422},
		{"an empty batch", `{"shop": "shop-a.test", "jobs": []}`, 422},
		{"jobs missing", `{"shop": "shop-a.test"}`, 422},
		{"jobs not an array", `{"shop": "shop-a.test", "jobs": {}}`, 422},
		{"a key used twice", batch(shopA, "same", "x", "same"), 422},
		{"an unknown kind", `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {}, "dedupe_key": "k"}, {"kind": "no.such", "payload": {}}]}`, 422},
		{"a payload that isn't an object", `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {}}, {"kind": "test.sleep", "payload": [1]}]}`, 422},
		{"an unstorable payload", `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {}}, {"kind": "test.sleep", "payload": {"x": "\u0000"}}]}`, 422},
		{"an empty dedupe key", `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {}, "dedupe_key": ""}]}`, 422},
		{"a bad run_at", `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {}, "run_at": "tomorrow"}]}`, 422},
		{"an unknown job field", `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {}, "priority": 1}]}`, 422},
		{"an unknown top-level field", `{"shop": "shop-a.test", "jobs": [{"kind": "test.sleep", "payload": {}}], "extra": 1}`, 422},
		{"a shop in each job", `{"shop": "shop-a.test", "jobs": [{"shop": "shop-b.test", "kind": "test.sleep", "payload": {}}]}`, 422},
		{"no shop", `{"jobs": [{"kind": "test.sleep", "payload": {}}]}`, 422},
		{"not JSON", `[`, 422},
		{"data after the object", batch(shopA, "") + ` {}`, 422},
		{"an unknown shop", batch("no-such-shop.test", "a", "b"), 404},
		{"a body over the limit", huge, 413},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, _, body := do(t, srv, http.MethodPost, "/v1/jobs/batch", tc.body)
			if status != tc.status {
				t.Fatalf("status %d, body %v; want %d", status, body, tc.status)
			}
			if n := countJobs(t, env); n != 0 {
				t.Fatalf("%d jobs created", n)
			}
			if n := countKeys(t, env); n != 0 {
				t.Fatalf("%d dedupe keys created", n)
			}
		})
	}

	// The most a batch may hold is accepted.
	status, _, body := do(t, srv, http.MethodPost, "/v1/jobs/batch", `{"shop": "shop-a.test", "jobs": [`+strings.Join(exactlyMax, ",")+`]}`)
	if status != http.StatusCreated || len(ids(t, body, "ids")) != api.MaxBatchSize {
		t.Fatalf("a full-size batch: status %d", status)
	}
	env.CheckBacklog(t)
}

// P4-3, Q8 (AC-2.2, AC-11.1): at the backlog limit a single enqueue and a batch
// each get 429 with the shop's Retry-After, and create no job and no key. A
// duplicate still gets 200.
func TestEnqueueAtTheBacklogLimitGets429WithRetryAfter(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withLimit(shopA, 3, 5000)))
	srv := serve(t, env)
	if status, _, body := do(t, srv, http.MethodPost, "/v1/jobs/batch", batch(shopA, "k1", "k2", "k3")); status != http.StatusCreated {
		t.Fatalf("filling the shop: status %d, body %v", status, body)
	}

	rejected := map[string]string{
		"single":                   `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}}`,
		"single with a key":        `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}, "dedupe_key": "new"}`,
		"batch":                    batch(shopA, "n1", "n2"),
		"batch with one duplicate": batch(shopA, "k1", "n3"),
	}
	for name, body := range rejected {
		path := "/v1/jobs"
		if strings.HasPrefix(name, "batch") {
			path = "/v1/jobs/batch"
		}
		status, header, out := do(t, srv, http.MethodPost, path, body)
		if status != http.StatusTooManyRequests {
			t.Fatalf("%s: status %d, body %v; want 429", name, status, out)
		}
		if got := header.Get("Retry-After"); got != "5" {
			t.Errorf("%s: Retry-After = %q, want 5 (the shop's 5000 ms)", name, got)
		}
	}
	if n := countJobs(t, env); n != 3 {
		t.Fatalf("%d jobs, want 3", n)
	}
	if n := countKeys(t, env); n != 3 {
		t.Fatalf("%d dedupe keys, want 3", n)
	}
	// 1 + 1 + 2 + 1 jobs were refused (the batch's duplicate isn't counted, D-049).
	if got := shedTotal(t, env, shopA); got != 5 {
		t.Fatalf("shed_total %d, want 5", got)
	}

	// A duplicate of an existing job is still answered with 200 at the limit.
	status, _, body := do(t, srv, http.MethodPost, "/v1/jobs",
		`{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}, "dedupe_key": "k2"}`)
	if status != http.StatusOK || body["duplicate"] != true {
		t.Fatalf("duplicate at the limit: status %d, body %v; want 200 duplicate", status, body)
	}
	if status, _, _ = do(t, srv, http.MethodPost, "/v1/jobs/batch", batch(shopA, "k1", "k3")); status != http.StatusOK {
		t.Fatalf("all-duplicate batch at the limit: status %d, want 200", status)
	}

	// Another shop is unaffected (AC-11.2).
	if status, _, body = do(t, srv, http.MethodPost, "/v1/jobs", `{"shop": "shop-b.test", "kind": "test.sleep", "payload": {}}`); status != http.StatusCreated {
		t.Fatalf("shop B: status %d, body %v", status, body)
	}
	if status, _, body = do(t, srv, http.MethodPost, "/v1/jobs/batch", batch(shopB, "", "")); status != http.StatusCreated {
		t.Fatalf("shop B batch: status %d, body %v", status, body)
	}
	env.CheckBacklog(t)
}

// Retry-After is whole seconds, rounded up (an HTTP delay is an integer).
func TestRetryAfterIsWholeSecondsRoundedUp(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, withLimit(shopA, 1, 1500)))
	srv := serve(t, env)
	do(t, srv, http.MethodPost, "/v1/jobs", `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}}`)
	_, header, _ := do(t, srv, http.MethodPost, "/v1/jobs", `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}}`)
	if got := header.Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want 2 for 1500 ms", got)
	}
}
