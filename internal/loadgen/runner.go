package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
)

// MaxBatch is the most jobs one batch request holds (design §2, D-030). A
// shop's tick with more arrivals is sent as several batches.
const MaxBatch = 1000

// OrderTag is the kind loadgen enqueues (D-020).
const OrderTag = "order.tag"

// ShopReport counts what happened to one shop's arrivals.
type ShopReport struct {
	Offered int `json:"offered"` // jobs in the schedule
	// OfferedByPhase splits Offered by phase. The warm-up's and the spike's are
	// the same in every run with the same seed (design §17).
	OfferedByPhase map[Phase]int `json:"offered_by_phase"`
	Accepted       int           `json:"accepted"`     // jobs in batches answered 201 or 200
	Shed           int           `json:"shed"`         // jobs in batches answered 429, not retried (design §17)
	Errors         int           `json:"errors"`       // jobs in batches that failed any other way
	Batches        int           `json:"batches"`      // batch requests sent
	Rejected       int           `json:"rejected"`     // batch requests answered 429
	LastJobID      int64         `json:"last_job_id"`  // the highest job ID accepted
	SpikeJobID     int64         `json:"spike_job_id"` // the highest job ID accepted for a tick before the spike ended (D-056)
}

// Report is what one run did. loadgen prints it as JSON.
type Report struct {
	Seed      uint64    `json:"seed"`
	TickMS    int       `json:"tick_ms"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	// Ticks counts the ticks sent in each phase.
	Ticks map[Phase]int `json:"ticks"`
	// Drained says the tail ended because every job accepted before the spike
	// ended was terminal; DrainTimedOut that it ended at drain_max_ms instead.
	Drained       bool `json:"drained"`
	DrainTimedOut bool `json:"drain_timed_out"`
	Interrupted   bool `json:"interrupted"`
	// LateTicks counts ticks sent more than one tick after they were due: if it
	// is not 0, loadgen itself could not keep the schedule.
	LateTicks int                    `json:"late_ticks"`
	Shops     map[string]*ShopReport `json:"shops"`
	// EnqueueLatencyMS is the batch requests' latency: a published number with
	// no target (requirements §3).
	EnqueueLatencyMS Percentiles `json:"enqueue_latency_ms"`
}

// Percentiles summarizes a set of durations, in milliseconds.
type Percentiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

// Runner sends a Schedule's arrivals to the api role.
type Runner struct {
	// API is the api role's base URL, such as http://api:8080.
	API   string
	HTTP  *http.Client
	Cfg   config.Loadgen
	Shops []string
	Log   *slog.Logger
	// DrainPoll is how often the tail checks whether the spike has drained
	// (default 1 s).
	DrainPoll time.Duration
}

type batchResult struct {
	shop     int
	tick     Tick
	n        int
	status   int
	ids      []int64
	err      error
	duration time.Duration
}

// tally counts the answers. Every request adds its answer before it counts as
// done, so once the requests sent before the tail are done, their job IDs are
// all here.
type tally struct {
	mu        sync.Mutex
	shops     map[string]*ShopReport
	latencies []time.Duration
}

func (t *tally) add(r *Runner, res batchResult) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sr := t.shops[r.Shops[res.shop]]
	sr.Batches++
	t.latencies = append(t.latencies, res.duration)
	switch {
	case res.err == nil && (res.status == http.StatusCreated || res.status == http.StatusOK):
		sr.Accepted += res.n
		for _, id := range res.ids {
			sr.LastJobID = max(sr.LastJobID, id)
			if res.tick.Phase != Tail {
				sr.SpikeJobID = max(sr.SpikeJobID, id)
			}
		}
	case res.err == nil && res.status == http.StatusTooManyRequests:
		sr.Shed += res.n
		sr.Rejected++
	default:
		sr.Errors += res.n
		r.Log.Warn("batch failed", "shop", r.Shops[res.shop], "jobs", res.n, "status", res.status, "err", errString(res.err))
	}
}

// spikeJobs returns each shop's last job accepted for a tick before the tail.
func (t *tally) spikeJobs() []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var ids []int64
	for _, sr := range t.shops {
		if sr.SpikeJobID > 0 {
			ids = append(ids, sr.SpikeJobID)
		}
	}
	return ids
}

// Run sends the schedule open-loop until the spike's backlog has drained, the
// tail reaches drain_max_ms, or ctx ends. Each tick's batches go out on time,
// each in its own request, whatever earlier requests are doing: a slow queue
// never slows the load down (design §17). Run returns once every request it
// sent has been answered.
func (r *Runner) Run(ctx context.Context) Report {
	rep := Report{Seed: r.Cfg.Seed, TickMS: r.Cfg.TickMS, Ticks: map[Phase]int{}, Shops: map[string]*ShopReport{}}
	for _, s := range r.Shops {
		rep.Shops[s] = &ShopReport{OfferedByPhase: map[Phase]int{}}
	}
	counts := &tally{shops: rep.Shops}
	poll := r.DrainPoll
	if poll <= 0 {
		poll = time.Second
	}
	sched := NewSchedule(r.Cfg, r.Shops)
	tick := time.Duration(r.Cfg.TickMS) * time.Millisecond
	tailMax := time.Duration(r.Cfg.WarmupMS+r.Cfg.SpikeMS+r.Cfg.DrainMaxMS) * time.Millisecond

	var beforeTail, inTail sync.WaitGroup
	nextOrder := make([]int64, len(r.Shops))
	offered := make([]map[Phase]int, len(r.Shops))
	for i := range offered {
		offered[i] = map[Phase]int{}
	}
	start := time.Now()
	rep.StartedAt = start.UTC()
	var drained <-chan bool
	timer := time.NewTimer(0)
	defer timer.Stop()
loop:
	for {
		t := sched.Next()
		if t.At >= tailMax {
			rep.DrainTimedOut = true
			break
		}
		if t.Phase == Tail && drained == nil && !rep.Drained {
			// The spike is over: once its batches are answered, watch for its
			// backlog to drain (D-056).
			drained = r.watchDrain(ctx, &beforeTail, counts, poll)
		}
		due := start.Add(t.At)
		timer.Reset(time.Until(due))
		select {
		case <-ctx.Done():
			rep.Interrupted = true
			break loop
		case ok := <-drained:
			if ok {
				rep.Drained = true
				break loop
			}
			rep.Interrupted = true // the watcher only gives up when ctx ends
			break loop
		case <-timer.C:
		}
		if time.Since(due) > tick {
			rep.LateTicks++
		}
		rep.Ticks[t.Phase]++
		wg := &inTail
		if t.Phase != Tail {
			wg = &beforeTail
		}
		for i, n := range t.Counts {
			offered[i][t.Phase] += n
			for n > 0 {
				size := min(n, MaxBatch)
				n -= size
				first := nextOrder[i] + 1
				nextOrder[i] += int64(size)
				wg.Go(func() { counts.add(r, r.send(ctx, i, t, first, size)) })
			}
		}
	}
	beforeTail.Wait()
	inTail.Wait()
	for i, id := range r.Shops {
		sr := rep.Shops[id]
		for phase, n := range offered[i] {
			sr.OfferedByPhase[phase] = n
			sr.Offered += n
		}
	}
	rep.EndedAt = time.Now().UTC()
	rep.EnqueueLatencyMS = summarize(counts.latencies)
	return rep
}

// send posts one batch of n jobs for shop i, with order IDs from first on.
func (r *Runner) send(ctx context.Context, i int, t Tick, first int64, n int) batchResult {
	res := batchResult{shop: i, tick: t, n: n}
	type job struct {
		Kind    string           `json:"kind"`
		Payload map[string]int64 `json:"payload"`
	}
	body := struct {
		Shop string `json:"shop"`
		Jobs []job  `json:"jobs"`
	}{Shop: r.Shops[i], Jobs: make([]job, n)}
	for k := range body.Jobs {
		body.Jobs[k] = job{Kind: OrderTag, Payload: map[string]int64{"order_id": first + int64(k)}}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		res.err = err
		return res
	}
	// The request is not tied to ctx: a batch already sent is answered, so the
	// report's counts match what the queue accepted.
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, strings.TrimRight(r.API, "/")+"/v1/jobs/batch", bytes.NewReader(raw))
	if err != nil {
		res.err = err
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	began := time.Now()
	resp, err := r.HTTP.Do(req)
	res.duration = time.Since(began)
	if err != nil {
		res.err = err
		return res
	}
	defer func() { _ = resp.Body.Close() }()
	res.status = resp.StatusCode
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		var out struct {
			IDs []int64 `json:"ids"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			res.err = fmt.Errorf("decode the batch answer: %w", err)
			return res
		}
		res.ids = out.IDs
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	return res
}

// watchDrain waits until every batch sent before the tail has been answered
// and counted, then polls each shop's last job accepted before the tail until
// all are terminal. A shop's jobs are claimed oldest first, so by then the
// spike's backlog has drained (D-056). It sends true when that happens, or false
// if ctx ends first.
func (r *Runner) watchDrain(ctx context.Context, beforeTail *sync.WaitGroup, counts *tally, poll time.Duration) <-chan bool {
	out := make(chan bool, 1)
	go func() {
		beforeTail.Wait()
		sentinels := counts.spikeJobs()
		for {
			if r.allTerminal(ctx, sentinels) {
				out <- true
				return
			}
			select {
			case <-ctx.Done():
				out <- false
				return
			case <-time.After(poll):
			}
		}
	}()
	return out
}

// allTerminal reports whether every job in ids is completed or dead.
func (r *Runner) allTerminal(ctx context.Context, ids []int64) bool {
	for _, id := range ids {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/jobs/%d", strings.TrimRight(r.API, "/"), id), nil)
		if err != nil {
			return false
		}
		resp, err := r.HTTP.Do(req)
		if err != nil {
			return false
		}
		var j struct {
			State string `json:"state"`
		}
		err = json.NewDecoder(resp.Body).Decode(&j)
		_ = resp.Body.Close()
		if err != nil || (j.State != "completed" && j.State != "dead") {
			return false
		}
	}
	return true
}

func summarize(ds []time.Duration) Percentiles {
	if len(ds) == 0 {
		return Percentiles{}
	}
	ms := make([]float64, len(ds))
	for i, d := range ds {
		ms[i] = float64(d.Microseconds()) / 1000
	}
	slices.Sort(ms)
	return Percentiles{Count: len(ms), P50: Quantile(ms, 0.5), P99: Quantile(ms, 0.99), Max: ms[len(ms)-1]}
}

// Quantile returns the q-quantile of sorted values by linear interpolation
// between the closest ranks, as Postgres's percentile_cont does.
func Quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := min(lo+1, len(sorted)-1)
	return sorted[lo] + (pos-float64(lo))*(sorted[hi]-sorted[lo])
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
