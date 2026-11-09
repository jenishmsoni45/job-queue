package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// Env is what a chaos run talks to. DB reaches Postgres directly, not through
// Toxiproxy, so the harness can always read the truth.
type Env struct {
	DB     *pgxpool.Pool
	Docker *Docker
	Toxi   *Toxiproxy
	API    string // the api role's base URL
	HTTP   *http.Client
	Shops  []string
	Logf   func(format string, args ...any)
}

// enqueueConcurrency is how many enqueue requests a run keeps in flight.
const enqueueConcurrency = 16

// Setup installs the test-only tables the checks read, and checks that
// Toxiproxy has the expected proxies.
func (e *Env) Setup(ctx context.Context) error {
	if err := e.waitReady(ctx); err != nil {
		return err
	}
	if _, err := e.DB.Exec(ctx, testkinds.EffectsTableSQL); err != nil {
		return fmt.Errorf("chaos: create test_effects: %w", err)
	}
	if _, err := e.DB.Exec(ctx, queuetest.LeaseLogSQL); err != nil {
		return fmt.Errorf("chaos: install the lease log: %w", err)
	}
	names, err := e.Toxi.Proxies(ctx)
	if err != nil {
		return err
	}
	for _, p := range Proxies {
		if !slices.Contains(names, p) {
			return fmt.Errorf("chaos: toxiproxy has proxies %v, missing %s", names, p)
		}
	}
	return nil
}

// Restore undoes every fault: no toxics, and Postgres and both workers
// running and not paused. It then waits until Postgres and the api answer.
func (e *Env) Restore(ctx context.Context) error {
	if err := e.Toxi.Reset(ctx); err != nil {
		return err
	}
	for _, svc := range Services {
		c, err := e.Docker.Inspect(ctx, svc)
		if err != nil {
			return err
		}
		switch c.Status {
		case "running", "restarting":
		case "paused":
			err = e.Docker.Unpause(ctx, svc)
		default:
			err = e.Docker.Start(ctx, svc)
		}
		if err != nil {
			return err
		}
	}
	return e.waitReady(ctx)
}

// waitReady waits until Postgres answers the harness and the api answers /healthz.
func (e *Env) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for {
		_, dbErr := e.DB.Exec(ctx, "SELECT 1")
		apiErr := e.health(ctx)
		if dbErr == nil && apiErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("chaos: stack not ready: postgres: %w; api: %w", dbErr, apiErr)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (e *Env) health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.API+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := e.HTTP.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("api /healthz: %s", resp.Status)
	}
	return nil
}

// ResetDB empties the job tables and the test tables, and zeroes the backlog
// counters, so every run starts from the same state.
func (e *Env) ResetDB(ctx context.Context) error {
	_, err := e.DB.Exec(ctx,
		`TRUNCATE jobs, dedupe_keys, idempotency_keys, test_effects, lease_log; UPDATE shop_state SET backlog = 0`)
	if err != nil {
		return fmt.Errorf("chaos: reset the database: %w", err)
	}
	return nil
}

// FaultResult is a fault as it happened.
type FaultResult struct {
	Fault
	AppliedMS  int64  `json:"applied_ms"`  // when it was applied, from the start of the run
	RevertedMS int64  `json:"reverted_ms"` // when it was undone
	Error      string `json:"error,omitempty"`
}

// RunResult is the outcome of one chaos run. It is written to bench/results/chaos/.
type RunResult struct {
	Seed       uint64        `json:"seed"`
	Started    time.Time     `json:"started"`
	DurationMS int64         `json:"duration_ms"` // from the first enqueue until every job was terminal or the timeout
	Passed     bool          `json:"passed"`
	Error      string        `json:"error,omitempty"` // a harness error that cut the run short
	Jobs       JobCounts     `json:"jobs"`
	Faults     []FaultResult `json:"faults"`
	Samples    Samples       `json:"backlog_samples"`
	Checks     []Check       `json:"checks"`
}

// JobCounts counts the run's jobs.
type JobCounts struct {
	Planned        int `json:"planned"`
	Accepted       int `json:"accepted"`        // keys that got 201 or 200
	EnqueueRetries int `json:"enqueue_retries"` // requests retried after an error or a 5xx
	Duplicates     int `json:"duplicates"`      // retries answered 200: the first try had committed
	Completed      int `json:"completed"`
	Dead           int `json:"dead"`
	NotTerminal    int `json:"not_terminal"`
}

// Samples counts the backlog counter samples taken during the run.
type Samples struct {
	Taken      int `json:"taken"`
	Skipped    int `json:"skipped"` // the database didn't answer, for example during a restart
	Mismatched int `json:"mismatched"`
}

// Run runs one seeded chaos run (design §18) and checks it. It returns an
// error only if the stack could not be brought back to a clean state, in which
// case later runs can't be trusted either.
func (e *Env) Run(ctx context.Context, plan Plan) (RunResult, error) {
	res := RunResult{Seed: plan.Seed, Jobs: JobCounts{Planned: len(plan.Jobs)}}
	if err := e.Restore(ctx); err != nil {
		return res, err
	}
	if err := e.ResetDB(ctx); err != nil {
		return res, err
	}

	start := time.Now()
	res.Started = start.UTC()
	deadline := start.Add(RunTimeout)
	runCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	sampleCtx, stopSampler := context.WithCancel(runCtx)
	samplerDone := make(chan Samples, 1)
	go func() { samplerDone <- e.sampleBacklog(sampleCtx) }()

	var wg sync.WaitGroup
	res.Faults = make([]FaultResult, len(plan.Faults))
	for i, f := range plan.Faults {
		wg.Go(func() { res.Faults[i] = e.inject(runCtx, start, i, f) })
	}
	ids, counts, enqErr := e.enqueueAll(runCtx, plan)
	res.Jobs.Accepted, res.Jobs.EnqueueRetries, res.Jobs.Duplicates = counts.accepted, counts.retries, counts.duplicates
	wg.Wait()

	// Every fault has been undone by now; make sure of it before waiting.
	if err := e.Restore(ctx); err != nil {
		stopSampler()
		<-samplerDone
		return res, err
	}
	notTerminal := e.waitTerminal(runCtx, ids)
	res.DurationMS = time.Since(start).Milliseconds()
	stopSampler()
	res.Samples = <-samplerDone

	switch {
	case enqErr != nil:
		res.Error = enqErr.Error()
	case notTerminal > 0:
		res.Error = fmt.Sprintf("%d accepted jobs were not completed or dead after %v", notTerminal, RunTimeout)
	}
	for _, f := range res.Faults {
		if f.Error != "" && res.Error == "" {
			res.Error = fmt.Sprintf("fault %s on %s at %d ms: %s", f.Kind, f.Target, f.AtMS, f.Error)
		}
	}

	checkCtx, cancelChecks := context.WithTimeout(ctx, time.Minute)
	defer cancelChecks()
	res.Checks, res.Jobs = e.check(checkCtx, plan, ids, res.Jobs, res.Samples)
	res.Passed = res.Error == ""
	for _, c := range res.Checks {
		res.Passed = res.Passed && c.Passed
	}
	return res, nil
}

type enqueueCounts struct{ accepted, retries, duplicates int }

// enqueueAll enqueues every planned job through the api, each with its dedupe
// key, retrying errors and 5xx answers until the key is accepted (design §18:
// dedupe keys make the retries safe). It returns the job ID for each planned
// job, or 0 for a key that was never accepted.
func (e *Env) enqueueAll(ctx context.Context, plan Plan) ([]int64, enqueueCounts, error) {
	ids := make([]int64, len(plan.Jobs))
	var retries, duplicates atomic.Int64
	var firstErr error
	var errOnce sync.Once
	next := make(chan int)
	var wg sync.WaitGroup
	for range enqueueConcurrency {
		wg.Go(func() {
			for i := range next {
				id, dup, tries, err := e.enqueue(ctx, plan.Jobs[i], EffectKind)
				retries.Add(int64(tries))
				if dup {
					duplicates.Add(1)
				}
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				ids[i] = id
			}
		})
	}
	for i := range plan.Jobs {
		next <- i
	}
	close(next)
	wg.Wait()

	return ids, enqueueCounts{len(accepted(ids)), int(retries.Load()), int(duplicates.Load())}, firstErr
}

// enqueue enqueues one job of kind through the api with its dedupe key,
// retrying errors and 5xx answers until it is accepted or ctx ends.
func (e *Env) enqueue(ctx context.Context, j PlannedJob, kind string) (id int64, duplicate bool, retries int, err error) {
	body, err := json.Marshal(map[string]any{
		"shop": j.Shop, "kind": kind, "payload": map[string]int{"ms": j.SleepMS}, "dedupe_key": j.Key,
	})
	if err != nil {
		return 0, false, 0, err
	}
	backoff := 100 * time.Millisecond
	for {
		status, got, err := e.post(ctx, body)
		switch {
		case err == nil && (status == http.StatusCreated || status == http.StatusOK):
			return got.ID, got.Duplicate, retries, nil
		case err == nil && status < 500:
			return 0, false, retries, fmt.Errorf("chaos: enqueue %s: unexpected status %d", j.Key, status)
		}
		select {
		case <-ctx.Done():
			return 0, false, retries, fmt.Errorf("chaos: enqueue %s: gave up: %w (last: status %d, %w)", j.Key, ctx.Err(), status, err)
		case <-time.After(backoff):
		}
		retries++
		backoff = min(2*backoff, time.Second)
	}
}

type enqueued struct {
	ID        int64 `json:"id"`
	Duplicate bool  `json:"duplicate"`
}

func (e *Env) post(ctx context.Context, body []byte) (int, enqueued, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.API+"/v1/jobs", bytes.NewReader(body))
	if err != nil {
		return 0, enqueued{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.HTTP.Do(req)
	if err != nil {
		return 0, enqueued{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out enqueued
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return 0, enqueued{}, fmt.Errorf("decode the enqueue answer: %w", err)
		}
	} else {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	}
	return resp.StatusCode, out, nil
}

// inject applies one fault at its time and undoes it after its duration.
// Undoing uses a context of its own, so a fault is always undone.
func (e *Env) inject(ctx context.Context, start time.Time, index int, f Fault) FaultResult {
	r := FaultResult{Fault: f}
	select {
	case <-ctx.Done():
		r.Error = "the run ended before the fault was due"
		return r
	case <-time.After(time.Until(start.Add(time.Duration(f.AtMS) * time.Millisecond))):
	}
	since := func() int64 { return time.Since(start).Milliseconds() }
	undo, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
	defer cancel()
	hold := func() { time.Sleep(time.Duration(f.DurationMS) * time.Millisecond) }

	r.AppliedMS = since()
	var err error
	switch f.Kind {
	case KillWorker:
		if err = e.Docker.Kill(ctx, f.Target); err == nil {
			hold()
			err = e.Docker.Start(undo, f.Target)
		}
	case PauseWorker:
		if err = e.Docker.Pause(ctx, f.Target); err == nil {
			hold()
			err = e.Docker.Unpause(undo, f.Target)
		}
	case RestartPostgres:
		if !f.Hard {
			err = e.Docker.Restart(undo, f.Target, PostgresStopTimeout)
			break
		}
		if err = e.Docker.Kill(ctx, f.Target); err == nil {
			hold()
			err = e.Docker.Start(undo, f.Target)
		}
	case Latency, Timeout, ResetPeer:
		name := fmt.Sprintf("fault-%d-%s", index, f.Kind)
		toxic := Toxic{Name: name, Type: string(f.Kind), Stream: f.Stream, Toxicity: 1, Attributes: f.Attributes}
		if err = e.Toxi.AddToxic(ctx, f.Target, toxic); err == nil {
			hold()
			err = e.Toxi.RemoveToxic(undo, f.Target, name)
		}
	default:
		err = fmt.Errorf("unknown fault kind %q", f.Kind)
	}
	r.RevertedMS = since()
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

// waitTerminal waits until every accepted job is completed or dead, or ctx
// ends. It returns how many were still not terminal.
func (e *Env) waitTerminal(ctx context.Context, ids []int64) int {
	ids = accepted(ids)
	left := len(ids)
	for {
		var n int
		err := e.DB.QueryRow(ctx, `
			SELECT count(*) FROM unnest($1::bigint[]) AS a (id)
			 WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = a.id AND j.state IN ('completed', 'dead'))`,
			ids).Scan(&n)
		if err == nil {
			left = n
			if n == 0 {
				return 0
			}
		}
		select {
		case <-ctx.Done():
			return left
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// backlogMismatchSQL compares every shop's backlog counter with its real count
// of queued and running jobs. One statement reads one snapshot, so the
// comparison is exact even while jobs move (design §3).
const backlogMismatchSQL = `
	SELECT s.shop_id, s.backlog,
	       (SELECT count(*) FROM jobs j WHERE j.shop_id = s.shop_id AND j.state IN ('queued', 'running'))
	  FROM shop_state s`

// sampleBacklog checks the backlog counter against a real count every second
// until ctx ends.
func (e *Env) sampleBacklog(ctx context.Context) Samples {
	var s Samples
	for {
		select {
		case <-ctx.Done():
			return s
		case <-time.After(time.Second):
		}
		mismatches, err := e.backlogMismatches(ctx)
		switch {
		case err != nil:
			s.Skipped++
		case len(mismatches) > 0:
			s.Taken++
			s.Mismatched++
			e.Logf("backlog counter mismatch: %v", mismatches)
		default:
			s.Taken++
		}
	}
}

func (e *Env) backlogMismatches(ctx context.Context) ([]string, error) {
	rows, err := e.DB.Query(ctx, backlogMismatchSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var shop string
		var counter, real int64
		if err := rows.Scan(&shop, &counter, &real); err != nil {
			return nil, err
		}
		if counter != real {
			out = append(out, fmt.Sprintf("%s: counter %d, real %d", shop, counter, real))
		}
	}
	return out, rows.Err()
}

// toxiproxyAddrs returns the addresses the toxiproxy service resolves to on
// the Compose network.
func toxiproxyAddrs(ctx context.Context) ([]string, error) {
	addrs, err := net.DefaultResolver.LookupHost(ctx, "toxiproxy")
	if err != nil {
		return nil, fmt.Errorf("chaos: resolve toxiproxy: %w", err)
	}
	if len(addrs) == 0 {
		return nil, errors.New("chaos: toxiproxy resolves to no address")
	}
	return addrs, nil
}
