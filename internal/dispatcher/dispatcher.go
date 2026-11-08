// Package dispatcher is the dispatcher role (design §6, §9, D-002): the only
// process that claims jobs. It holds a leader lock, queues the workers that
// long-poll it for work, claims jobs for them, and runs the reaper.
//
// Phase 2 claims in FIFO order (the globally oldest runnable jobs) with no
// caps, token buckets or DRR; phase 4 adds those. Correctness never depends on
// there being one leader: the claim SQL keeps one live lease per job (Q3) even
// if two dispatchers claim at once during a split brain.
package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/store"
)

// LeaderLockName names the session advisory lock the leader holds (design §9).
const LeaderLockName = "queued:dispatcher"

// ClaimPath is the long-poll endpoint workers call (design §11.2).
const ClaimPath = "/internal/v1/claim"

// maxOwnerLen bounds a lease owner, which is <host>/<pid>/<slot>/<boot nonce>.
const maxOwnerLen = 512

// Config configures a dispatcher.
type Config struct {
	// DB is the pool claims and the reaper use.
	DB store.DB
	// Connect opens the dedicated connection that holds the leader lock and
	// listens for enqueue notifications.
	Connect func(ctx context.Context) (*pgx.Conn, error)
	// Refresh is how often the dispatcher retries a claim for waiting workers
	// when no notification arrived (design §2: 250 ms). A scheduled job becomes
	// claimable within one refresh of its run_at (AC-3.2).
	Refresh time.Duration
	// LongPoll is how long a claim request waits before 204 (design §2: 25 s).
	LongPoll time.Duration
	// ReaperInterval is the reaper scan (D-003: 5 s), and ReaperBatch the most
	// jobs one reaper statement requeues (design §9: 500).
	ReaperInterval time.Duration
	ReaperBatch    int
	// StandbyRetry is how often a standby tries for the lock (design §9: 1 s).
	StandbyRetry time.Duration
	Log          *slog.Logger
}

// Dispatcher is one dispatcher process.
type Dispatcher struct {
	cfg Config

	leader atomic.Bool
	wake   chan struct{}

	mu      sync.Mutex
	waiting []*waiter // workers waiting for a job, oldest first
}

// waiter is one long-poll request. Its fields other than owner and reply are
// guarded by Dispatcher.mu.
type waiter struct {
	owner string
	reply chan reply // buffered; receives exactly one reply once taken
	taken bool       // in a claim right now, so not in waiting
	gone  bool       // the request gave up while taken
}

type reply struct {
	job       *store.Claimed
	notLeader bool
}

// New checks cfg and returns a dispatcher.
func New(cfg Config) (*Dispatcher, error) {
	if cfg.DB == nil || cfg.Connect == nil || cfg.Log == nil {
		return nil, errors.New("dispatcher: needs DB, Connect and Log")
	}
	if cfg.Refresh <= 0 || cfg.LongPoll <= 0 || cfg.ReaperInterval <= 0 || cfg.ReaperBatch <= 0 || cfg.StandbyRetry <= 0 {
		return nil, errors.New("dispatcher: intervals and the reaper batch must be positive")
	}
	return &Dispatcher{cfg: cfg, wake: make(chan struct{}, 1)}, nil
}

// IsLeader reports whether this dispatcher holds the leader lock.
func (d *Dispatcher) IsLeader() bool { return d.leader.Load() }

// Run campaigns for leadership until ctx is done. As leader it claims for
// waiting workers and runs the reaper; as standby it retries the lock every
// StandbyRetry. It steps down at once if the lock connection fails.
func (d *Dispatcher) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		conn, err := d.cfg.Connect(ctx)
		if err != nil {
			if ctx.Err() == nil {
				d.cfg.Log.Warn("leader connection failed", "err", err.Error())
				sleep(ctx, d.cfg.StandbyRetry)
			}
			continue
		}
		led := d.campaign(ctx, conn)
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = conn.Close(closeCtx) // closing the session releases the lock if held
		cancel()
		if led {
			// A dispatcher that just lost the lock waits longer than a standby's
			// retry interval before it tries again, so a waiting standby takes
			// over (P2-8) instead of the old leader winning the lock straight back.
			sleep(ctx, 2*d.cfg.StandbyRetry)
		}
	}
	return nil
}

// campaign waits on conn for the leader lock, then leads until conn fails or
// ctx ends. It reports whether it led.
func (d *Dispatcher) campaign(ctx context.Context, conn *pgx.Conn) bool {
	for {
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, LeaderLockName).Scan(&got); err != nil {
			if ctx.Err() == nil {
				d.cfg.Log.Warn("leader lock attempt failed", "err", err.Error())
			}
			return false
		}
		if got {
			break
		}
		if !sleep(ctx, d.cfg.StandbyRetry) {
			return false
		}
	}
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{store.EnqueueNotifyChannel}.Sanitize()); err != nil {
		d.cfg.Log.Warn("listen failed", "err", err.Error())
		return false
	}

	leaderCtx, cancel := context.WithCancel(ctx)
	d.leader.Store(true)
	d.cfg.Log.Info("became leader")
	var wg sync.WaitGroup
	wg.Go(func() { d.claimLoop(leaderCtx) })
	wg.Go(func() { d.reaperLoop(leaderCtx) })

	d.watch(leaderCtx, conn)

	// Stop claiming at once, cancel in-flight claims, and send waiting workers
	// away; they retry, and find the new leader (design §6.4, §9).
	d.stepDown()
	cancel()
	wg.Wait()
	d.cfg.Log.Info("stepped down")
	return true
}

// watch waits for enqueue notifications on conn and checks that conn is alive,
// until it fails or ctx ends.
func (d *Dispatcher) watch(ctx context.Context, conn *pgx.Conn) {
	const check = time.Second
	for {
		waitCtx, cancel := context.WithTimeout(ctx, check)
		_, err := conn.WaitForNotification(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			d.signal()
			continue
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			d.cfg.Log.Warn("leader connection lost", "err", err.Error())
			return
		}
		// No notification for a while: make sure the session, and so the lock,
		// is still there.
		pingCtx, cancel := context.WithTimeout(ctx, check)
		err = conn.Ping(pingCtx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				d.cfg.Log.Warn("leader connection lost", "err", err.Error())
			}
			return
		}
	}
}

func (d *Dispatcher) stepDown() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.leader.Store(false)
	for _, w := range d.waiting {
		w.reply <- reply{notLeader: true}
	}
	d.waiting = nil
}

// signal asks the claim loop to try a claim now.
func (d *Dispatcher) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// claimLoop claims for waiting workers whenever one arrives, a job is
// enqueued, the reaper requeues jobs, or the refresh interval passes.
func (d *Dispatcher) claimLoop(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.wake:
		case <-ticker.C:
		}
		for ctx.Err() == nil {
			batch := d.take()
			if len(batch) == 0 {
				break
			}
			owners := make([]string, len(batch))
			for i, w := range batch {
				owners[i] = w.owner
			}
			jobs, err := store.Claim(ctx, d.cfg.DB, owners)
			for i := range jobs {
				batch[i].reply <- reply{job: &jobs[i]}
			}
			d.settle(batch[len(jobs):])
			if err != nil {
				if ctx.Err() == nil {
					d.cfg.Log.Warn("claim failed", "err", err.Error())
				}
				break
			}
			if len(jobs) < len(batch) {
				break // no more runnable jobs; wait for a signal or the next refresh
			}
		}
	}
}

// take removes every waiting worker for one claim.
func (d *Dispatcher) take() []*waiter {
	d.mu.Lock()
	defer d.mu.Unlock()
	batch := d.waiting
	d.waiting = nil
	for _, w := range batch {
		w.taken = true
	}
	return batch
}

// settle returns workers that got no job: back to the queue, or a reply if
// they gave up or this dispatcher is no longer the leader.
func (d *Dispatcher) settle(unserved []*waiter) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, w := range unserved {
		switch {
		case !d.leader.Load():
			w.reply <- reply{notLeader: true}
		case w.gone:
			w.reply <- reply{}
		default:
			w.taken = false
			d.waiting = append(d.waiting, w)
		}
	}
}

// join queues a waiting worker. It returns false if this is not the leader.
func (d *Dispatcher) join(w *waiter) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.leader.Load() {
		return false
	}
	d.waiting = append(d.waiting, w)
	return true
}

// withdraw removes a worker that stopped waiting. It returns false if a claim
// has the worker right now, in which case the worker gets a reply soon.
func (d *Dispatcher) withdraw(w *waiter) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if w.taken {
		w.gone = true
		return false
	}
	for i, x := range d.waiting {
		if x == w {
			d.waiting = append(d.waiting[:i], d.waiting[i+1:]...)
			break
		}
	}
	return true
}

// reaperLoop requeues expired leases every ReaperInterval, repeating while a
// batch comes back full (design §9).
func (d *Dispatcher) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for ctx.Err() == nil {
			r, err := store.Reap(ctx, d.cfg.DB, d.cfg.ReaperBatch)
			if err != nil {
				if ctx.Err() == nil {
					d.cfg.Log.Warn("reaper failed", "err", err.Error())
				}
				break
			}
			if r.Requeued > 0 || r.Dead > 0 {
				d.cfg.Log.Info("reaped expired leases", "requeued", r.Requeued, "dead", r.Dead)
			}
			if r.Requeued > 0 {
				d.signal() // reaped jobs are runnable at once (D-031)
			}
			if r.Requeued+r.Dead < d.cfg.ReaperBatch {
				break
			}
		}
	}
}

// ClaimRequest is the body of POST /internal/v1/claim.
type ClaimRequest struct {
	Owner string `json:"owner"`
}

// ServeHTTP answers POST /internal/v1/claim: 200 with a job, 204 when none
// arrived within the long-poll timeout, 503 when this is not the leader
// (design §11.2).
func (d *Dispatcher) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	var req ClaimRequest
	dec := json.NewDecoder(http.MaxBytesReader(rw, r.Body, 4<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || req.Owner == "" || len(req.Owner) > maxOwnerLen {
		http.Error(rw, "body must be {\"owner\": \"...\"}", http.StatusBadRequest)
		return
	}
	w := &waiter{owner: req.Owner, reply: make(chan reply, 1)}
	if !d.join(w) {
		http.Error(rw, "not the leader", http.StatusServiceUnavailable)
		return
	}
	d.signal()

	timer := time.NewTimer(d.cfg.LongPoll)
	defer timer.Stop()
	done := r.Context().Done()
	for {
		select {
		case rep := <-w.reply:
			d.answer(rw, rep)
			return
		case <-timer.C:
		case <-done:
		}
		if d.withdraw(w) {
			rw.WriteHeader(http.StatusNoContent)
			return
		}
		// A claim has this worker right now; wait for its reply.
		timer.Stop()
		done = nil
	}
}

func (d *Dispatcher) answer(rw http.ResponseWriter, rep reply) {
	switch {
	case rep.notLeader:
		http.Error(rw, "not the leader", http.StatusServiceUnavailable)
	case rep.job == nil:
		rw.WriteHeader(http.StatusNoContent)
	default:
		rw.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(rw).Encode(rep.job); err != nil {
			// The worker never learns of the job; its lease expires and the
			// reaper requeues it.
			d.cfg.Log.Warn("claim reply failed", "job", rep.job.ID, "err", err.Error())
		}
	}
}

// sleep waits for d or until ctx is done. It reports whether the full wait
// passed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Client is the worker's side of the claim long-poll. It implements
// queue.ClaimSource.
type Client struct {
	// URL is the dispatcher's base URL, such as http://dispatcher:8080.
	URL  string
	HTTP *http.Client
}

// ErrNotLeader is returned when the dispatcher answers 503.
var ErrNotLeader = errors.New("dispatcher: not the leader")

// Claim long-polls for one job for owner. It returns nil and no error on 204.
func (c *Client) Claim(ctx context.Context, owner string) (*store.Claimed, error) {
	body, err := json.Marshal(ClaimRequest{Owner: owner})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+ClaimPath, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("dispatcher: claim request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: claim: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		var job store.Claimed
		if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
			return nil, fmt.Errorf("dispatcher: decode claimed job: %w", err)
		}
		return &job, nil
	case http.StatusNoContent:
		return nil, nil
	case http.StatusServiceUnavailable:
		return nil, ErrNotLeader
	default:
		return nil, fmt.Errorf("dispatcher: claim: unexpected status %d", resp.StatusCode)
	}
}
