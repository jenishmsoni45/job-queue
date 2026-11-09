package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jenishmsoni45/job-queue/internal/clock"
	"github.com/jenishmsoni45/job-queue/internal/lease"
	"github.com/jenishmsoni45/job-queue/internal/store"
)

// ClaimSource hands the worker its jobs. dispatcher.Client is the real one: it
// long-polls the dispatcher (design §6.4).
type ClaimSource interface {
	// Claim waits for one job claimed for owner. It returns nil and no error
	// when no job arrived before the long-poll timeout.
	Claim(ctx context.Context, owner string) (*store.Claimed, error)
}

// Outcome is what became of one run.
type Outcome string

const (
	// OutcomeCompleted: the fenced completion committed.
	OutcomeCompleted Outcome = "completed"
	// OutcomeRetry: a temporary failure was written; the job is queued again.
	OutcomeRetry Outcome = "retry"
	// OutcomeDead: a permanent failure, or the last allowed attempt failed.
	OutcomeDead Outcome = "dead"
	// OutcomeLeaseLost: the lease was gone, so no result was written (Q2).
	OutcomeLeaseLost Outcome = "lease_lost"
	// OutcomeAbandoned: the worker could not learn or write the result (the
	// database was unreachable, a commit's outcome stayed unknown, or the worker
	// was killed). The lease expires and the reaper requeues the job.
	OutcomeAbandoned Outcome = "abandoned"
)

// Report describes one finished run, for logs, tests and (later) metrics.
type Report struct {
	Job     store.Claimed
	Outcome Outcome
	// Err is the handler's error, or why no result could be written.
	Err error
	// HardTimeout is set when the run hit its kind's hard timeout (AC-4.5).
	HardTimeout bool
	// AbandonedHandler is set when the handler was still running when the
	// worker gave up on it; its goroutine is left to finish on its own.
	AbandonedHandler bool
	// AmbiguousCommit is set when the completion's commit failed with an
	// unknown outcome and the worker read the job back (design §7).
	AmbiguousCommit bool
	// Backoff is the delay drawn for a retry.
	Backoff time.Duration
	// Started and Finished come from the worker's clock. They are for reports
	// only and never reach a lease decision (NFR-14).
	Started, Finished time.Time
}

// Config configures a worker process.
type Config struct {
	// DB is the worker's connection pool: heartbeats, results and run.Tx().
	DB store.DB
	// Claims hands out jobs.
	Claims ClaimSource
	// Registry maps kinds to handlers.
	Registry *Registry
	// Slots is the number of handler goroutines (8 per process in Compose).
	Slots int
	// HeartbeatInterval is how often the one batched heartbeat runs. It must be
	// no longer than any kind's heartbeat_ms (config.File.MinHeartbeat).
	HeartbeatInterval time.Duration
	// Backoff is the retry backoff (D-004).
	Backoff Backoff
	// Identity is "<host>/<pid>". Each slot's lease owner adds the slot and a
	// random boot nonce, so a restarted worker never matches old leases (design §4).
	Identity string
	Log      *slog.Logger
	// Clock stamps reports. Nil means the system clock.
	Clock clock.Clock
	// OnReport, if set, is called after every run.
	OnReport func(Report)
	// Heartbeat renews leases. Nil means store.Heartbeat on DB. Tests replace
	// it to stall heartbeats, as a paused process would.
	Heartbeat lease.HeartbeatFunc
	// Commit commits a completion transaction. Nil means tx.Commit.
	// Fault-injection tests replace it to make a commit's outcome unknown.
	Commit func(ctx context.Context, tx pgx.Tx) error
}

const (
	// resultTimeout bounds each result write and read-back.
	resultTimeout = 10 * time.Second
	// rollbackTimeout bounds rolling back a run transaction.
	rollbackTimeout = 5 * time.Second
)

// claimRetry spaces out claim attempts after an error, such as 503 from a
// standby dispatcher during a leader change (design §6.4).
var claimRetry = Backoff{Base: 100 * time.Millisecond, Cap: 2 * time.Second}

var (
	errHardTimeout = errors.New("hard timeout")
	errFencedOut   = errors.New("the fenced write matched no row: the lease was lost")
	errKilled      = errors.New("the worker was stopped")
)

// PanicError is a recovered handler panic. Its message, with the stack, is
// stored in last_error, truncated to 4 KiB.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v\n%s", e.Value, e.Stack) }

// Worker runs handlers for claimed jobs.
type Worker struct {
	cfg    Config
	nonce  string
	clock  clock.Clock
	keeper *lease.Keeper
	commit func(ctx context.Context, tx pgx.Tx) error

	// hard is cancelled by Kill. Handlers and result writes run under it, so a
	// killed worker stops at once and writes nothing more, like a crashed process.
	hard context.Context
	kill context.CancelFunc
}

// New checks cfg and returns a worker.
func New(cfg Config) (*Worker, error) {
	switch {
	case cfg.DB == nil || cfg.Claims == nil || cfg.Registry == nil || cfg.Log == nil:
		return nil, errors.New("queue: worker needs DB, Claims, Registry and Log")
	case cfg.Slots < 1:
		return nil, errors.New("queue: worker needs at least one slot")
	case cfg.HeartbeatInterval <= 0:
		return nil, errors.New("queue: worker needs a positive heartbeat interval")
	case cfg.Identity == "":
		return nil, errors.New("queue: worker needs an identity")
	}
	if err := cfg.Backoff.Validate(); err != nil {
		return nil, err
	}
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("queue: boot nonce: %w", err)
	}
	w := &Worker{cfg: cfg, nonce: hex.EncodeToString(nonce), clock: cfg.Clock, commit: cfg.Commit}
	if w.clock == nil {
		w.clock = clock.System{}
	}
	if w.commit == nil {
		w.commit = func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }
	}
	beat := cfg.Heartbeat
	if beat == nil {
		beat = func(ctx context.Context, leases []store.Fence) ([]int64, error) {
			return store.Heartbeat(ctx, cfg.DB, leases)
		}
	}
	w.keeper = lease.New(cfg.HeartbeatInterval, beat, cfg.Log)
	w.hard, w.kill = context.WithCancel(context.Background())
	return w, nil
}

// Owner returns the lease owner of a slot: <host>/<pid>/<slot>/<boot nonce>.
func (w *Worker) Owner(slot int) string {
	return fmt.Sprintf("%s/%d/%s", w.cfg.Identity, slot, w.nonce)
}

// Run claims and runs jobs until ctx is cancelled. Then it stops claiming,
// lets in-flight runs finish and write their results, and returns. Kill makes
// it return at once.
func (w *Worker) Run(ctx context.Context) error {
	keeperCtx, stopKeeper := context.WithCancel(w.hard)
	keeperDone := make(chan struct{})
	go func() {
		defer close(keeperDone)
		w.keeper.Run(keeperCtx)
	}()

	claimCtx, stopClaims := context.WithCancel(ctx)
	defer stopClaims()
	stopOnKill := context.AfterFunc(w.hard, stopClaims)
	defer stopOnKill()

	var wg sync.WaitGroup
	for slot := range w.cfg.Slots {
		wg.Go(func() { w.slot(claimCtx, slot) })
	}
	wg.Wait()
	stopKeeper()
	<-keeperDone
	return nil
}

// Kill stops the worker the way a crash would: handlers are cancelled and
// abandoned, and no further result is written. Leases expire and the reaper
// requeues the jobs. Run returns soon after.
func (w *Worker) Kill() { w.kill() }

// slot claims and runs one job at a time until ctx is done.
func (w *Worker) slot(ctx context.Context, slot int) {
	owner := w.Owner(slot)
	var failures int32
	for ctx.Err() == nil {
		job, err := w.cfg.Claims.Claim(ctx, owner)
		if job != nil {
			// Run a job even if ctx ended during the claim: its lease is ours.
			failures = 0
			w.run(*job)
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			w.cfg.Log.Debug("claim failed", "owner", owner, "err", err.Error())
			sleep(ctx, claimRetry.Delay(failures))
			continue
		}
		failures = 0 // long-poll timed out with no job; poll again
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func millis(ms int32) time.Duration { return time.Duration(ms) * time.Millisecond }

// run executes one claimed job and writes its fenced result (design §7, §8).
func (w *Worker) run(c store.Claimed) {
	rep := Report{Job: c, Started: w.clock.Now()}
	defer func() {
		rep.Finished = w.clock.Now()
		w.report(rep)
	}()

	f := c.Fence()
	hctx, cancel := context.WithCancelCause(w.hard)
	defer cancel(nil)
	release := w.keeper.Hold(f, millis(c.LeaseMS), cancel)
	defer release()

	h, ok := w.cfg.Registry.Lookup(c.Kind)
	if !ok {
		// D-037: a temporary failure, so a later deploy with the handler runs it.
		rep.Err = fmt.Errorf("no handler registered for kind %q", c.Kind)
		w.writeFailure(&rep, f, rep.Err)
		return
	}

	r := &Run{
		Job: Job{ID: c.ID, Shop: c.Shop, Kind: c.Kind, Payload: c.Payload, ClaimSeq: c.ClaimSeq, Attempts: c.Attempts},
		db:  w.cfg.DB,
	}
	done := make(chan error, 1)
	go func() {
		err := call(hctx, h, r)
		ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
		defer cancel()
		if r.finish(ctx) {
			done <- err
		}
	}()

	grace := millis(c.HeartbeatMS) // D-038
	timeout := time.NewTimer(millis(c.TimeoutMS))
	defer timeout.Stop()

	var herr error
	select {
	case herr = <-done:
	case <-timeout.C:
		// AC-4.5: cancel the handler and count a failed attempt. A handler that
		// ignores cancellation is abandoned after the grace period; heartbeats
		// continue meanwhile, so the failure write still passes fencing.
		rep.HardTimeout = true
		cancel(errHardTimeout)
		if _, finished := w.awaitHandler(r, done, grace); !finished {
			rep.AbandonedHandler = true
		}
		herr = fmt.Errorf("hard timeout: the handler ran longer than %s", millis(c.TimeoutMS))
	case <-hctx.Done():
		// The lease is lost (AC-4.3) or the worker was killed. Never write a
		// result, whatever the handler returns.
		if _, finished := w.awaitHandler(r, done, grace); !finished {
			rep.AbandonedHandler = true
		}
		w.rollback(r)
		rep.Outcome, rep.Err = OutcomeLeaseLost, context.Cause(hctx)
		if w.hard.Err() != nil {
			rep.Outcome, rep.Err = OutcomeAbandoned, errKilled
		}
		return
	}
	rep.Err = herr

	switch cause := context.Cause(hctx); {
	case w.hard.Err() != nil:
		w.rollback(r)
		rep.Outcome, rep.Err = OutcomeAbandoned, errors.Join(herr, errKilled)
	case errors.Is(cause, lease.ErrLeaseLost) || errors.Is(cause, lease.ErrHeartbeatsFailing):
		// The lease went while the handler was returning; a write would be fenced out.
		w.rollback(r)
		rep.Outcome, rep.Err = OutcomeLeaseLost, errors.Join(herr, cause)
	case herr == nil && !rep.HardTimeout:
		w.complete(&rep, f, r)
	case IsPermanent(herr):
		w.rollback(r)
		w.writePermanent(&rep, f, herr)
	default:
		w.rollback(r)
		w.writeFailure(&rep, f, herr)
	}
}

// call runs the handler and turns a panic into a PanicError.
func call(ctx context.Context, h Handler, r *Run) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = &PanicError{Value: p, Stack: debug.Stack()}
		}
	}()
	return h(ctx, r)
}

// awaitHandler waits up to grace for a cancelled handler to return, and
// returns its result. finished is false if the handler was abandoned still
// running.
func (w *Worker) awaitHandler(r *Run, done <-chan error, grace time.Duration) (err error, finished bool) {
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case err = <-done:
		return err, true
	case <-t.C:
	case <-w.hard.Done():
	}
	if r.abandon() {
		return nil, false
	}
	return <-done, true // it finished at the last moment, so it is sending its result
}

// rollback discards the run transaction of a handler that has returned.
func (w *Worker) rollback(r *Run) {
	if tx := r.takeTx(); tx != nil {
		ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
		defer cancel()
		_ = tx.Rollback(ctx)
	}
}

// complete commits the fenced completion together with the run transaction.
func (w *Worker) complete(rep *Report, f store.Fence, r *Run) {
	ctx, cancel := context.WithTimeout(w.hard, resultTimeout)
	defer cancel()

	tx := r.takeTx()
	if tx == nil {
		var err error
		if tx, err = w.cfg.DB.Begin(ctx); err != nil {
			w.writeFailure(rep, f, fmt.Errorf("completion failed: %w", err))
			return
		}
	}
	ok, err := store.Complete(ctx, tx, f)
	if err != nil || !ok {
		rctx, rcancel := context.WithTimeout(context.Background(), rollbackTimeout)
		_ = tx.Rollback(rctx)
		rcancel()
		if err != nil {
			// Not committed: the handler's effects are gone, so the run failed.
			w.writeFailure(rep, f, fmt.Errorf("completion failed: %w", err))
			return
		}
		rep.Outcome, rep.Err = OutcomeLeaseLost, errFencedOut
		return
	}

	err = w.commit(ctx, tx)
	switch {
	case err == nil:
		rep.Outcome = OutcomeCompleted
	case notCommitted(err):
		w.writeFailure(rep, f, fmt.Errorf("completion commit failed: %w", err))
	default:
		// Unknown outcome (design §7): only the holder of this claim_seq can
		// complete at it, so "completed at my claim_seq" means the commit landed.
		rep.AmbiguousCommit = true
		// A fresh deadline: the commit may have used up ctx's. Kill still stops it.
		rctx, rcancel := context.WithTimeout(w.hard, resultTimeout)
		defer rcancel()
		j, rerr := store.GetJob(rctx, w.cfg.DB, f.JobID)
		if rerr == nil && j.State == store.StateCompleted && j.ClaimSeq == f.ClaimSeq {
			rep.Outcome = OutcomeCompleted
			return
		}
		rep.Outcome, rep.Err = OutcomeAbandoned, errors.Join(fmt.Errorf("commit outcome unknown: %w", err), rerr)
	}
}

// notCommitted reports whether a commit error means the transaction certainly
// did not commit: the server answered with an error, it rolled back, or the
// request was never sent. Anything else, such as a connection reset while
// COMMIT was in flight, leaves the outcome unknown.
func notCommitted(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) || errors.Is(err, pgx.ErrTxCommitRollback) || pgconn.SafeToRetry(err)
}

// writeFailure records a temporary failure: attempts + 1, then a retry after
// backoff, or dead at the kind's max (design §8). On a write error the outcome
// is unknown, so the job is abandoned: either the write landed, or the lease
// expires and the reaper requeues it (design §7).
func (w *Worker) writeFailure(rep *Report, f store.Fence, cause error) {
	ctx, cancel := context.WithTimeout(w.hard, resultTimeout)
	defer cancel()
	delay := w.cfg.Backoff.Delay(rep.Job.Attempts + 1)
	res, err := store.Fail(ctx, w.cfg.DB, f, failMessage(cause), delay)
	switch {
	case err != nil:
		rep.Outcome, rep.Err = OutcomeAbandoned, errors.Join(cause, err)
	case !res.Applied:
		rep.Outcome, rep.Err = OutcomeLeaseLost, errors.Join(cause, errFencedOut)
	case res.State == store.StateDead:
		rep.Outcome, rep.Err = OutcomeDead, cause
	default:
		rep.Outcome, rep.Err, rep.Backoff = OutcomeRetry, cause, delay
	}
}

// writePermanent sends the job straight to dead (AC-5.3).
func (w *Worker) writePermanent(rep *Report, f store.Fence, cause error) {
	ctx, cancel := context.WithTimeout(w.hard, resultTimeout)
	defer cancel()
	ok, err := store.Permanent(ctx, w.cfg.DB, f, failMessage(cause))
	switch {
	case err != nil:
		rep.Outcome, rep.Err = OutcomeAbandoned, errors.Join(cause, err)
	case !ok:
		rep.Outcome, rep.Err = OutcomeLeaseLost, errors.Join(cause, errFencedOut)
	default:
		rep.Outcome, rep.Err = OutcomeDead, cause
	}
}

func failMessage(err error) string {
	if err == nil {
		return "unknown error"
	}
	return err.Error()
}

func (w *Worker) report(rep Report) {
	attrs := []any{"job", rep.Job.ID, "kind", rep.Job.Kind, "shop", rep.Job.Shop,
		"claim_seq", rep.Job.ClaimSeq, "outcome", string(rep.Outcome)}
	if rep.Err != nil {
		attrs = append(attrs, "err", rep.Err.Error())
	}
	if rep.Outcome == OutcomeCompleted {
		w.cfg.Log.Debug("run finished", attrs...)
	} else {
		w.cfg.Log.Info("run finished", attrs...)
	}
	if w.cfg.OnReport != nil {
		w.cfg.OnReport(rep)
	}
}
