package invariants

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/api"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// apiServer serves the api role's job endpoints on env.
func apiServer(t *testing.T, env *queuetest.Env) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	api.New(env.Pool, queuetest.Logger(t)).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

type enqueueAnswer struct {
	status    int
	id        int64
	duplicate bool
}

// postJob sends one enqueue with a dedupe key. It is safe to call from many
// goroutines; errors are returned, not reported, so callers off the test
// goroutine can collect them.
func postJob(srv *httptest.Server, shop, key string) (enqueueAnswer, error) {
	body := fmt.Sprintf(`{"shop": %q, "kind": %q, "payload": {"ms": 1}, "dedupe_key": %q}`, shop, testkinds.Sleep, key)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/v1/jobs", strings.NewReader(body))
	if err != nil {
		return enqueueAnswer{}, err
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		return enqueueAnswer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return enqueueAnswer{}, err
	}
	var out struct {
		ID        int64 `json:"id"`
		Duplicate bool  `json:"duplicate"`
	}
	if resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return enqueueAnswer{}, fmt.Errorf("decode %q: %w", raw, err)
		}
	}
	return enqueueAnswer{status: resp.StatusCode, id: out.ID, duplicate: out.Duplicate}, nil
}

// deliver sends k enqueues of one key, all at once or one after another.
func deliver(t *testing.T, srv *httptest.Server, shop, key string, k int, concurrent bool) []enqueueAnswer {
	t.Helper()
	answers := make([]enqueueAnswer, k)
	errs := make([]error, k)
	if !concurrent {
		for i := range k {
			answers[i], errs[i] = postJob(srv, shop, key)
		}
	} else {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range k {
			wg.Go(func() {
				<-start
				answers[i], errs[i] = postJob(srv, shop, key)
			})
		}
		close(start)
		wg.Wait()
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	return answers
}

// checkOneJob fails the test unless every answer names the same job, at most
// wantCreated of them created it (201) and the rest were duplicates (200), and
// exactly one job and one key exist for (shop, key).
func checkOneJob(t *testing.T, env *queuetest.Env, shop, key string, answers []enqueueAnswer, wantCreated int) int64 {
	t.Helper()
	created := 0
	ids := map[int64]bool{}
	for _, a := range answers {
		switch {
		case a.status == http.StatusCreated && !a.duplicate:
			created++
		case a.status == http.StatusOK && a.duplicate:
		default:
			t.Errorf("answer %+v, want 201 or 200 duplicate", a)
		}
		ids[a.id] = true
	}
	if created != wantCreated || len(ids) != 1 {
		t.Errorf("%d answers: %d created, IDs %v; want %d created and one ID", len(answers), created, ids, wantCreated)
	}
	var jobs, keys int
	if err := env.Pool.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM jobs WHERE shop_id = $1),
		       (SELECT count(*) FROM dedupe_keys WHERE shop_id = $1 AND key = $2)`, shop, key).Scan(&jobs, &keys); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || keys != 1 {
		t.Errorf("shop %s has %d jobs and %d keys for %q, want 1 and 1", shop, jobs, keys, key)
	}
	for id := range ids {
		return id
	}
	return 0
}

// P3-4, Q4 (enqueue path), AC-1.4, NFR-4: k sequential and k concurrent
// enqueues with one dedupe key create exactly one job, before the job runs and
// after it completes.
func TestQ4OneJobPerDedupeKey(t *testing.T) {
	t.Parallel()
	const k = 25
	for _, concurrent := range []bool{false, true} {
		name := map[bool]string{false: "sequential", true: "concurrent"}[concurrent]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := queuetest.New(t, nil)
			srv := apiServer(t, env)
			key := "webhook-" + name

			id := checkOneJob(t, env, shopA, key, deliver(t, srv, shopA, key, k, concurrent), 1)

			// Run the job to completion, then deliver the key k more times.
			reg := queue.NewRegistry()
			testkinds.Register(reg)
			_, reports := env.StartWorker(t, reg)
			if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted || rep.Job.ID != id {
				t.Fatalf("run: %+v, want job %d completed", rep, id)
			}
			again := checkOneJob(t, env, shopA, key, deliver(t, srv, shopA, key, k, concurrent), 0)
			if again != id {
				t.Errorf("after completion the key maps to job %d, want %d", again, id)
			}
			if j := env.Job(t, id); j.State != store.StateCompleted || j.ClaimSeq != 1 {
				t.Errorf("job %d is %s at claim %d; a duplicate must not touch it", id, j.State, j.ClaimSeq)
			}
			env.CheckBacklog(t)
		})
	}
}
