package adversarial

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
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// A replay storm (design §16, Q4): 32 clients each send the same enqueue 25
// times as fast as they can, for two shops at once, while a worker runs and
// completes the job. Every answer is 201 or 200, exactly one per shop is 201,
// all name the same job, and each shop ends with one job and one key.
func TestReplayStormOnOneDedupeKey(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	mux := http.NewServeMux()
	api.New(env.Pool, queuetest.Logger(t)).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	reg := queue.NewRegistry()
	testkinds.Register(reg)
	_, reports := env.StartWorker(t, reg)

	const clients, perClient = 32, 25
	shops := []string{queuetest.ShopA, queuetest.ShopB}
	type answer struct {
		shop   string
		status int
		id     int64
		dup    bool
		err    error
	}
	answers := make(chan answer, clients*perClient*len(shops))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for c := range clients {
		shop := shops[c%len(shops)]
		wg.Go(func() {
			<-start
			for range perClient {
				a := answer{shop: shop}
				a.status, a.id, a.dup, a.err = post(srv, shop, "storm-key")
				answers <- a
			}
		})
	}
	close(start)
	wg.Wait()
	close(answers)

	created := map[string]int{}
	ids := map[string]map[int64]bool{}
	for a := range answers {
		if a.err != nil {
			t.Fatal(a.err)
		}
		if ids[a.shop] == nil {
			ids[a.shop] = map[int64]bool{}
		}
		ids[a.shop][a.id] = true
		switch {
		case a.status == http.StatusCreated && !a.dup:
			created[a.shop]++
		case a.status == http.StatusOK && a.dup:
		default:
			t.Errorf("%s: status %d duplicate %v, want 201 or 200 duplicate", a.shop, a.status, a.dup)
		}
	}
	for _, shop := range shops {
		if created[shop] != 1 || len(ids[shop]) != 1 {
			t.Errorf("%s: %d created, IDs %v; want one job created once", shop, created[shop], ids[shop])
		}
		var jobs, keys int
		if err := env.Pool.QueryRow(t.Context(), `
			SELECT (SELECT count(*) FROM jobs WHERE shop_id = $1), (SELECT count(*) FROM dedupe_keys WHERE shop_id = $1)`,
			shop).Scan(&jobs, &keys); err != nil {
			t.Fatal(err)
		}
		if jobs != 1 || keys != 1 {
			t.Errorf("%s: %d jobs and %d keys, want 1 and 1", shop, jobs, keys)
		}
	}
	for range shops {
		if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted {
			t.Errorf("run: %+v, want completed", rep)
		}
	}
	env.CheckBacklog(t)
}

func post(srv *httptest.Server, shop, key string) (status int, id int64, duplicate bool, err error) {
	body := fmt.Sprintf(`{"shop": %q, "kind": %q, "payload": {}, "dedupe_key": %q}`, shop, testkinds.Sleep, key)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/v1/jobs", strings.NewReader(body))
	if err != nil {
		return 0, 0, false, err
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		return 0, 0, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, false, err
	}
	var out struct {
		ID        int64 `json:"id"`
		Duplicate bool  `json:"duplicate"`
	}
	if resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return 0, 0, false, fmt.Errorf("decode %q: %w", raw, err)
		}
	}
	return resp.StatusCode, out.ID, out.Duplicate, nil
}
