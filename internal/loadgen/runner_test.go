package loadgen

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
)

// fakeAPI is a stand-in for the api role: it accepts batches (refusing the
// whole batch with 429 when a shop's backlog would pass its limit), and answers
// GET /v1/jobs/{id} with completed once finished says so.
type fakeAPI struct {
	mu       sync.Mutex
	limit    map[string]int
	backlog  map[string]int
	nextID   int64
	orders   map[string]map[int64]bool
	maxBatch int
	finished bool
	srv      *httptest.Server
}

func newFakeAPI(t *testing.T, limit map[string]int) *fakeAPI {
	t.Helper()
	f := &fakeAPI{limit: limit, backlog: map[string]int{}, orders: map[string]map[int64]bool{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs/batch", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Shop string `json:"shop"`
			Jobs []struct {
				Kind    string           `json:"kind"`
				Payload map[string]int64 `json:"payload"`
			} `json:"jobs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Jobs) == 0 {
			http.Error(w, "bad", http.StatusUnprocessableEntity)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.maxBatch = max(f.maxBatch, len(req.Jobs))
		if l, ok := f.limit[req.Shop]; ok && f.backlog[req.Shop]+len(req.Jobs) > l {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if f.orders[req.Shop] == nil {
			f.orders[req.Shop] = map[int64]bool{}
		}
		ids := make([]int64, len(req.Jobs))
		for i, j := range req.Jobs {
			if j.Kind != OrderTag || j.Payload["order_id"] <= 0 || f.orders[req.Shop][j.Payload["order_id"]] {
				t.Errorf("bad or repeated job %+v for %s", j, req.Shop)
			}
			f.orders[req.Shop][j.Payload["order_id"]] = true
			f.nextID++
			ids[i] = f.nextID
		}
		f.backlog[req.Shop] += len(req.Jobs)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"ids": ids, "duplicates": []int64{}})
	})
	mux.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		state := "queued"
		if f.finished {
			state = "completed"
		}
		f.mu.Unlock()
		if _, err := strconv.ParseInt(r.PathValue("id"), 10, 64); err != nil {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"state": state})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) finish() {
	f.mu.Lock()
	f.finished = true
	f.mu.Unlock()
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

var fast = config.Loadgen{
	Seed: 7, TickMS: 10, RatePerShop: 200, WarmupMS: 200,
	SpikeShop: "a.test", SpikeRate: 5000, SpikeMS: 200, DrainMaxMS: 5000,
}

// The runner sends every scheduled arrival exactly once, in batches of at most
// 1,000, counts each batch's jobs as accepted or shed, and keeps going after
// the spike until each shop's last job from before the tail is terminal (D-056).
func TestRunnerSendsTheScheduleAndStopsWhenDrained(t *testing.T) {
	f := newFakeAPI(t, map[string]int{"a.test": 300})
	r := &Runner{API: f.srv.URL, HTTP: f.srv.Client(), Cfg: fast, Shops: []string{"a.test", "b.test"}, Log: quiet, DrainPoll: 10 * time.Millisecond}
	go func() {
		time.Sleep(700 * time.Millisecond) // the tail runs a while before the backlog "drains"
		f.finish()
	}()
	rep := r.Run(t.Context())
	if !rep.Drained || rep.DrainTimedOut || rep.Interrupted {
		t.Fatalf("report = %+v, want drained", rep)
	}
	if rep.Ticks[Warmup] != 20 || rep.Ticks[Spike] != 20 || rep.Ticks[Tail] < 20 {
		t.Errorf("ticks = %v, want 20 warm-up, 20 spike and a tail", rep.Ticks)
	}
	// The schedule the runner sent is the one the seed makes.
	s := NewSchedule(fast, r.Shops)
	want := []map[Phase]int{{}, {}}
	for range rep.Ticks[Warmup] + rep.Ticks[Spike] + rep.Ticks[Tail] {
		tk := s.Next()
		for i, c := range tk.Counts {
			want[i][tk.Phase] += c
		}
	}
	for i, shop := range r.Shops {
		sr := rep.Shops[shop]
		total := want[i][Warmup] + want[i][Spike] + want[i][Tail]
		if sr.Offered != total || sr.Accepted+sr.Shed+sr.Errors != sr.Offered || sr.Errors != 0 {
			t.Errorf("%s: %+v, want %d offered, each accepted or shed", shop, sr, total)
		}
		for _, p := range []Phase{Warmup, Spike, Tail} {
			if sr.OfferedByPhase[p] != want[i][p] {
				t.Errorf("%s: offered in %s = %d, want %d", shop, p, sr.OfferedByPhase[p], want[i][p])
			}
		}
		if sr.SpikeJobID == 0 || sr.LastJobID < sr.SpikeJobID {
			t.Errorf("%s: spike job %d, last job %d", shop, sr.SpikeJobID, sr.LastJobID)
		}
	}
	if a := rep.Shops["a.test"]; a.Shed == 0 || a.Rejected == 0 {
		t.Errorf("a.test passed its limit of 300 without a 429: %+v", a)
	}
	if b := rep.Shops["b.test"]; b.Shed != 0 {
		t.Errorf("b.test has no limit but shed %d", b.Shed)
	}
	if rep.EnqueueLatencyMS.Count == 0 || rep.StartedAt.IsZero() || !rep.EndedAt.After(rep.StartedAt) {
		t.Errorf("report = %+v", rep)
	}
}

// A shop's tick with more than 1,000 arrivals goes as several batches.
func TestRunnerSplitsBigTicks(t *testing.T) {
	f := newFakeAPI(t, nil)
	cfg := fast
	cfg.SpikeRate, cfg.SpikeMS = 300_000, 50 // about 3,000 a tick
	f.finish()
	r := &Runner{API: f.srv.URL, HTTP: f.srv.Client(), Cfg: cfg, Shops: []string{"a.test"}, Log: quiet, DrainPoll: 10 * time.Millisecond}
	rep := r.Run(t.Context())
	if f.maxBatch > MaxBatch || f.maxBatch < MaxBatch/2 {
		t.Errorf("largest batch = %d, want it split at %d", f.maxBatch, MaxBatch)
	}
	if sr := rep.Shops["a.test"]; sr.Accepted != sr.Offered || sr.Batches <= rep.Ticks[Warmup]+rep.Ticks[Spike] {
		t.Errorf("report = %+v", sr)
	}
}

// If the backlog never drains, the tail stops at drain_max_ms.
func TestRunnerStopsAtTheDrainCap(t *testing.T) {
	f := newFakeAPI(t, nil)
	cfg := fast
	cfg.DrainMaxMS = 200
	r := &Runner{API: f.srv.URL, HTTP: f.srv.Client(), Cfg: cfg, Shops: []string{"a.test"}, Log: quiet, DrainPoll: 10 * time.Millisecond}
	rep := r.Run(t.Context())
	if rep.Drained || !rep.DrainTimedOut || rep.Ticks[Tail] != 20 {
		t.Errorf("report = drained %v, timed out %v, tail ticks %d; want stopped at the cap after 20 tail ticks",
			rep.Drained, rep.DrainTimedOut, rep.Ticks[Tail])
	}
}

// Cancelling the run stops it, marks the report interrupted, and still waits
// for the requests in flight.
func TestRunnerInterrupted(t *testing.T) {
	f := newFakeAPI(t, nil)
	r := &Runner{API: f.srv.URL, HTTP: f.srv.Client(), Cfg: fast, Shops: []string{"a.test"}, Log: quiet}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	rep := r.Run(ctx)
	if !rep.Interrupted || rep.Drained {
		t.Errorf("report = %+v, want interrupted", rep)
	}
	if sr := rep.Shops["a.test"]; sr.Accepted+sr.Shed+sr.Errors != sr.Offered {
		t.Errorf("counts don't add up: %+v", sr)
	}
}

// An api that refuses everything with 500 counts the jobs as errors.
func TestRunnerCountsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/jobs/batch") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"state": "completed"})
	}))
	defer srv.Close()
	r := &Runner{API: srv.URL, HTTP: srv.Client(), Cfg: fast, Shops: []string{"a.test"}, Log: quiet, DrainPoll: 10 * time.Millisecond}
	rep := r.Run(t.Context())
	if sr := rep.Shops["a.test"]; sr.Errors != sr.Offered || sr.Accepted != 0 || !rep.Drained {
		t.Errorf("report = %+v / %+v", rep, sr)
	}
}
