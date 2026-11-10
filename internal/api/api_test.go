package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
)

const shopA = queuetest.ShopA

func serve(t *testing.T, env *queuetest.Env) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	api.New(env.Pool, queuetest.Logger(t)).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, srv *httptest.Server, method, path, body string) (int, http.Header, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, strings.NewReader(body))
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
	var out map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: response is not JSON: %q", method, path, raw)
		}
	}
	return resp.StatusCode, resp.Header, out
}

func countJobs(t *testing.T, env *queuetest.Env) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// P2-4, AC-1.1: a valid enqueue returns 201 with the ID of a committed job.
func TestEnqueueCreatesACommittedJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serve(t, env)

	status, header, body := do(t, srv, http.MethodPost, "/v1/jobs",
		`{"shop": "shop-a.test", "kind": "test.sleep", "payload": {"ms": 5, "nested": {"a": [1, 2]}}}`)
	if status != http.StatusCreated {
		t.Fatalf("status %d, body %v; want 201", status, body)
	}
	id := int64(body["id"].(float64))
	if header.Get("Location") != "/v1/jobs/"+strconv.FormatInt(id, 10) {
		t.Errorf("Location = %q", header.Get("Location"))
	}
	// Read it on a separate connection: the job was committed before the 201.
	conn := env.DB.Connect(t)
	var state, payload string
	if err := conn.QueryRow(t.Context(), `SELECT state, payload::text FROM jobs WHERE id = $1`, id).Scan(&state, &payload); err != nil {
		t.Fatalf("job %d is not visible on another connection: %v", id, err)
	}
	if state != store.StateQueued || payload != `{"ms": 5, "nested": {"a": [1, 2]}}` {
		t.Errorf("job = %s %s", state, payload)
	}
	env.CheckBacklog(t)

	runAt := env.DBNow(t).Add(time.Hour).UTC()
	status, _, body = do(t, srv, http.MethodPost, "/v1/jobs",
		`{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}, "run_at": "`+runAt.Format(time.RFC3339Nano)+`"}`)
	if status != http.StatusCreated {
		t.Fatalf("with run_at: status %d, body %v", status, body)
	}
	if j := env.Job(t, int64(body["id"].(float64))); !j.RunAt.Equal(runAt) {
		t.Errorf("run_at = %v, want %v", j.RunAt, runAt)
	}
}

// P2-4, AC-1.2 and AC-1.3: 404, 422 and 413, each creating nothing: no job,
// no dedupe key (D-039, D-045).
func TestEnqueueRejectsAndCreatesNothing(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serve(t, env)
	valid := func(edit string) string {
		return `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}` + edit + `}`
	}
	tests := []struct {
		name string
		body string
		want int
	}{
		{"unknown shop", `{"shop": "nope.test", "kind": "test.sleep", "payload": {}}`, http.StatusNotFound},
		{"unknown kind", `{"shop": "shop-a.test", "kind": "test.nope", "payload": {}}`, http.StatusUnprocessableEntity},
		{"malformed JSON", `{"shop": "shop-a.test",`, http.StatusUnprocessableEntity},
		{"empty body", ``, http.StatusUnprocessableEntity},
		{"array body", `[]`, http.StatusUnprocessableEntity},
		{"trailing data", valid(``) + `{}`, http.StatusUnprocessableEntity},
		{"unknown field", valid(`, "priority": 1`), http.StatusUnprocessableEntity},
		{"missing shop", `{"kind": "test.sleep", "payload": {}}`, http.StatusUnprocessableEntity},
		{"empty kind", `{"shop": "shop-a.test", "kind": "", "payload": {}}`, http.StatusUnprocessableEntity},
		{"control character in shop", `{"shop": "shop-a.test\u0000", "kind": "test.sleep", "payload": {}}`, http.StatusUnprocessableEntity},
		{"overlong shop", `{"shop": "` + strings.Repeat("s", 256) + `", "kind": "test.sleep", "payload": {}}`, http.StatusUnprocessableEntity},
		{"missing payload", `{"shop": "shop-a.test", "kind": "test.sleep"}`, http.StatusUnprocessableEntity},
		{"null payload", `{"shop": "shop-a.test", "kind": "test.sleep", "payload": null}`, http.StatusUnprocessableEntity},
		{"array payload", `{"shop": "shop-a.test", "kind": "test.sleep", "payload": [1]}`, http.StatusUnprocessableEntity},
		{"string payload", `{"shop": "shop-a.test", "kind": "test.sleep", "payload": "x"}`, http.StatusUnprocessableEntity},
		{"unstorable payload", `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {"a": "\u0000"}}`, http.StatusUnprocessableEntity},
		{"run_at not RFC 3339", valid(`, "run_at": "tomorrow"`), http.StatusUnprocessableEntity},
		{"run_at not a string", valid(`, "run_at": 12`), http.StatusUnprocessableEntity},
		{"empty dedupe_key", valid(`, "dedupe_key": ""`), http.StatusUnprocessableEntity},
		{"dedupe_key over 255 bytes", valid(`, "dedupe_key": "` + strings.Repeat("k", api.MaxDedupeKeyLen+1) + `"`), http.StatusUnprocessableEntity},
		{"control character in dedupe_key", valid(`, "dedupe_key": "w-1\n"`), http.StatusUnprocessableEntity},
		{"dedupe_key not a string", valid(`, "dedupe_key": 7`), http.StatusUnprocessableEntity},
		{"unknown kind with a dedupe_key", `{"shop": "shop-a.test", "kind": "test.nope", "payload": {}, "dedupe_key": "w-1"}`, http.StatusUnprocessableEntity},
		{"unknown shop with a dedupe_key", `{"shop": "nope.test", "kind": "test.sleep", "payload": {}, "dedupe_key": "w-1"}`, http.StatusNotFound},
		{"body over 1 MiB", `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {"x": "` + strings.Repeat("a", api.MaxBodyBytes) + `"}}`, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		status, _, body := do(t, srv, http.MethodPost, "/v1/jobs", tt.body)
		if status != tt.want {
			t.Errorf("%s: status %d, want %d (body %v)", tt.name, status, tt.want, body)
		}
		if msg, _ := body["error"].(string); msg == "" {
			t.Errorf("%s: no error message in %v", tt.name, body)
		}
	}
	if n := countJobs(t, env); n != 0 {
		t.Errorf("%d jobs created by rejected requests, want 0", n)
	}
	if n := countKeys(t, env); n != 0 {
		t.Errorf("%d dedupe keys stored by rejected requests, want 0", n)
	}
	env.CheckBacklog(t)
}

func countKeys(t *testing.T, env *queuetest.Env) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM dedupe_keys`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// P3-4, AC-1.4: the first enqueue with a dedupe key gets 201; a repeat gets
// 200 with the same ID and duplicate: true, and creates nothing. A 255-byte key
// is accepted (D-045). The same key for another shop is another job.
func TestEnqueueWithDedupeKey(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serve(t, env)
	key := strings.Repeat("k", api.MaxDedupeKeyLen)
	body := `{"shop": "shop-a.test", "kind": "test.sleep", "payload": {}, "dedupe_key": "` + key + `"}`

	status, header, first := do(t, srv, http.MethodPost, "/v1/jobs", body)
	if status != http.StatusCreated || first["duplicate"] != nil {
		t.Fatalf("first: status %d, body %v; want 201 without duplicate", status, first)
	}
	id := int64(first["id"].(float64))
	for range 3 {
		status, header, again := do(t, srv, http.MethodPost, "/v1/jobs", body)
		if status != http.StatusOK || int64(again["id"].(float64)) != id || again["duplicate"] != true {
			t.Fatalf("repeat: status %d, body %v; want 200 {id: %d, duplicate: true}", status, again, id)
		}
		if header.Get("Location") != "/v1/jobs/"+strconv.FormatInt(id, 10) {
			t.Errorf("repeat: Location = %q", header.Get("Location"))
		}
	}
	if header.Get("Location") != "/v1/jobs/"+strconv.FormatInt(id, 10) {
		t.Errorf("Location = %q", header.Get("Location"))
	}
	if n := countJobs(t, env); n != 1 {
		t.Errorf("%d jobs, want 1", n)
	}

	status, _, other := do(t, srv, http.MethodPost, "/v1/jobs",
		`{"shop": "shop-b.test", "kind": "test.sleep", "payload": {}, "dedupe_key": "`+key+`"}`)
	if status != http.StatusCreated || int64(other["id"].(float64)) == id {
		t.Errorf("same key, other shop: status %d, body %v; want a new job", status, other)
	}
	if n := countKeys(t, env); n != 2 {
		t.Errorf("%d dedupe keys, want 2", n)
	}
	env.CheckBacklog(t)
}

func TestGetJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	srv := serve(t, env)
	id := env.Enqueue(t, shopA, "test.sleep", map[string]string{"secret": "not shown"})
	if _, err := store.Claim(context.Background(), env.Pool, []string{"w1"}); err != nil {
		t.Fatal(err)
	}

	status, _, body := do(t, srv, http.MethodGet, "/v1/jobs/"+strconv.FormatInt(id, 10), "")
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if body["id"].(float64) != float64(id) || body["shop"] != shopA || body["kind"] != "test.sleep" ||
		body["state"] != store.StateRunning || body["claim_seq"].(float64) != 1 || body["attempts"].(float64) != 0 ||
		body["first_claimed_at"] == nil || body["lease_expires_at"] == nil || body["completed_at"] != nil {
		t.Errorf("body = %v", body)
	}
	for _, field := range []string{"run_at", "enqueued_at", "first_runnable_at", "throttles", "dead_at", "dead_reason", "last_error", "discarded_at"} {
		if _, ok := body[field]; !ok {
			t.Errorf("field %s missing from %v", field, body)
		}
	}
	if _, ok := body["payload"]; ok {
		t.Error("GET /v1/jobs/{id} shows the payload; it shows state, counters and timestamps only")
	}

	for _, path := range []string{"/v1/jobs/999999", "/v1/jobs/0", "/v1/jobs/-1", "/v1/jobs/abc", "/v1/jobs/99999999999999999999"} {
		if status, _, _ := do(t, srv, http.MethodGet, path, ""); status != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 404", path, status)
		}
	}
}
