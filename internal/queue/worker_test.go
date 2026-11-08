package queue_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/lease"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

const shopA = queuetest.ShopA

// A test kind every test registers its own handler for.
const kind = testkinds.Sleep

// newEnv sets the lease, heartbeat, timeout and max attempts of every test kind.
func newEnv(t *testing.T, leaseMS, heartbeatMS, timeoutMS, maxAttempts int) *queuetest.Env {
	t.Helper()
	return queuetest.New(t, queuetest.Config(t, func(c *config.File) {
		for i := range c.Kinds {
			k := &c.Kinds[i]
			k.LeaseMS, k.HeartbeatMS, k.TimeoutMS, k.MaxAttempts = leaseMS, heartbeatMS, timeoutMS, maxAttempts
		}
	}))
}

func registry(h queue.Handler) *queue.Registry {
	r := queue.NewRegistry()
	r.Register(kind, h)
	return r
}

func createEffects(t *testing.T, env *queuetest.Env) {
	t.Helper()
	if _, err := env.Pool.Exec(t.Context(), `CREATE TABLE effects (job_id bigint NOT NULL, claim_seq int NOT NULL)`); err != nil {
		t.Fatal(err)
	}
}

func effects(t *testing.T, env *queuetest.Env, id int64) int {
	t.Helper()
	var n int
	if err := env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM effects WHERE job_id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func writeEffect(ctx context.Context, run *queue.Run) error {
	tx, err := run.Tx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO effects VALUES ($1, $2)`, run.Job.ID, run.Job.ClaimSeq)
	return err
}

// AC-7.2's happy path: run.Tx() effects commit together with the completion.
func TestCompletesAndCommitsRunTxEffects(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	createEffects(t, env)
	_, reports := env.StartWorker(t, registry(func(ctx context.Context, run *queue.Run) error {
		if err := writeEffect(ctx, run); err != nil {
			return err
		}
		tx, _ := run.Tx(ctx)
		if !errors.Is(tx.Commit(ctx), queue.ErrManagedTx) || !errors.Is(tx.Rollback(ctx), queue.ErrManagedTx) {
			return errors.New("handler could commit or roll back run.Tx()")
		}
		return nil
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	rep := reports.Next(t, 10*time.Second)
	if rep.Outcome != queue.OutcomeCompleted || rep.Job.ID != id {
		t.Fatalf("report = %+v, want job %d completed", rep, id)
	}
	j := env.Job(t, id)
	if j.State != store.StateCompleted || j.Attempts != 0 || j.ClaimSeq != 1 {
		t.Errorf("job = %+v", j)
	}
	if n := effects(t, env, id); n != 1 {
		t.Errorf("%d effects committed, want 1", n)
	}
	env.CheckBacklog(t)
}

// AC-5.1: a temporary failure retries after a full-jitter backoff, then completes.
func TestTemporaryFailureRetriesWithBackoff(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	backoff := queue.Backoff{Base: 40 * time.Millisecond, Cap: 100 * time.Millisecond}
	_, reports := env.StartWorker(t, registry(func(_ context.Context, run *queue.Run) error {
		if run.Job.Attempts < 2 {
			return errors.New("flaky")
		}
		return nil
	}), func(c *queue.Config) { c.Backoff = backoff })
	id := env.Enqueue(t, shopA, kind, nil)

	for attempt := int32(1); attempt <= 2; attempt++ {
		rep := reports.Next(t, 10*time.Second)
		if rep.Outcome != queue.OutcomeRetry || rep.Err == nil || rep.Err.Error() != "flaky" {
			t.Fatalf("run %d: %+v, want a retry", attempt, rep)
		}
		if rep.Backoff < 0 || rep.Backoff >= backoff.Bound(attempt) {
			t.Errorf("run %d: backoff %v outside [0, %v)", attempt, rep.Backoff, backoff.Bound(attempt))
		}
	}
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted {
		t.Fatalf("third run: %+v, want completed", rep)
	}
	if j := env.Job(t, id); j.Attempts != 2 || j.ClaimSeq != 3 || *j.LastError != "flaky" {
		t.Errorf("job = %+v, want attempts 2, claim_seq 3, last_error flaky", j)
	}
}

// AC-5.2: at the max attempts the job goes dead with a reason and the last error.
func TestAttemptsExhaustedGoesDead(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 3)
	var runs atomic.Int32
	_, reports := env.StartWorker(t, registry(func(context.Context, *queue.Run) error {
		return fmt.Errorf("failure %d", runs.Add(1))
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	j := env.WaitState(t, id, store.StateDead, 10*time.Second)
	if j.Attempts != 3 || *j.DeadReason != store.ReasonAttemptsExhausted || *j.LastError != "failure 3" {
		t.Errorf("dead job = %+v", j)
	}
	var outcomes []queue.Outcome
	for range 3 {
		outcomes = append(outcomes, reports.Next(t, time.Second).Outcome)
	}
	if outcomes[0] != queue.OutcomeRetry || outcomes[1] != queue.OutcomeRetry || outcomes[2] != queue.OutcomeDead {
		t.Errorf("outcomes = %v, want retry, retry, dead", outcomes)
	}
	env.CheckBacklog(t)
}

// AC-5.3: Permanent sends the job straight to dead without retries.
func TestPermanentGoesDead(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	_, reports := env.StartWorker(t, registry(func(context.Context, *queue.Run) error {
		return queue.Permanent(errors.New("order does not exist"))
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeDead {
		t.Fatalf("report = %+v, want dead", rep)
	}
	j := env.Job(t, id)
	if j.State != store.StateDead || *j.DeadReason != store.ReasonPermanent || j.Attempts != 0 || *j.LastError != "order does not exist" {
		t.Errorf("job = %+v", j)
	}
}

// Design §8: a panic is recovered, stored in last_error with its stack, and
// counts as a temporary failure. The worker keeps going.
func TestPanicIsRecovered(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	_, reports := env.StartWorker(t, registry(func(_ context.Context, run *queue.Run) error {
		if run.Job.Attempts == 0 {
			panic("nil map")
		}
		return nil
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	rep := reports.Next(t, 10*time.Second)
	var pe *queue.PanicError
	if rep.Outcome != queue.OutcomeRetry || !errors.As(rep.Err, &pe) {
		t.Fatalf("report = %+v, want a retry with a PanicError", rep)
	}
	j := env.WaitState(t, id, store.StateCompleted, 10*time.Second)
	if j.Attempts != 1 || !strings.HasPrefix(*j.LastError, "panic: nil map") || !strings.Contains(*j.LastError, "goroutine") {
		t.Errorf("job = %+v, want one attempt and the panic with its stack in last_error", j)
	}
}

// D-037: a kind with no handler is a temporary failure.
func TestNoHandlerIsATemporaryFailure(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 2)
	_, reports := env.StartWorker(t, queue.NewRegistry())
	id := env.Enqueue(t, shopA, kind, nil)
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeRetry {
		t.Fatalf("report = %+v, want a retry", rep)
	}
	j := env.WaitState(t, id, store.StateDead, 10*time.Second)
	if j.Attempts != 2 || !strings.Contains(*j.LastError, `no handler registered for kind "test.sleep"`) {
		t.Errorf("job = %+v", j)
	}
}

// AC-4.5: a run past its hard timeout is cancelled and counts a failed attempt.
func TestHardTimeoutCountsAnAttempt(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 2000, 100, 200, 5)
	cause := make(chan error, 1)
	_, reports := env.StartWorker(t, registry(func(ctx context.Context, run *queue.Run) error {
		if run.Job.Attempts > 0 {
			return nil
		}
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	rep := reports.Next(t, 10*time.Second)
	if !rep.HardTimeout || rep.Outcome != queue.OutcomeRetry || rep.AbandonedHandler {
		t.Fatalf("report = %+v, want a hard-timeout retry with the handler returned", rep)
	}
	if got := <-cause; got == nil || !strings.Contains(got.Error(), "hard timeout") {
		t.Errorf("handler's cancel cause = %v, want the hard timeout", got)
	}
	j := env.WaitState(t, id, store.StateCompleted, 10*time.Second)
	if j.Attempts != 1 || !strings.Contains(*j.LastError, "hard timeout") {
		t.Errorf("job = %+v", j)
	}
}

// AC-4.5 and D-038: a handler that ignores cancellation is abandoned after a
// grace period of one heartbeat interval, and the failure is written anyway.
func TestHardTimeoutAbandonsAHandlerThatIgnoresCancellation(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 2000, 150, 200, 5)
	release := make(chan struct{})
	defer close(release)
	_, reports := env.StartWorker(t, registry(func(_ context.Context, run *queue.Run) error {
		if run.Job.Attempts == 0 {
			<-release // ignores ctx
		}
		return nil
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	rep := reports.Next(t, 10*time.Second)
	if !rep.HardTimeout || !rep.AbandonedHandler || rep.Outcome != queue.OutcomeRetry {
		t.Fatalf("report = %+v, want an abandoned hard-timeout retry", rep)
	}
	if took := rep.Finished.Sub(rep.Started); took < 350*time.Millisecond {
		t.Errorf("failure written after %v, want after the timeout (200ms) plus the grace (150ms)", took)
	}
	j := env.WaitState(t, id, store.StateCompleted, 10*time.Second)
	if j.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", j.Attempts)
	}
}

// AC-4.1: heartbeats extend the lease, so a run three times longer than its
// lease completes on its first claim.
func TestHeartbeatsExtendTheLease(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 300, 100, 10_000, 5)
	started := make(chan struct{})
	_, reports := env.StartWorker(t, registry(func(ctx context.Context, _ *queue.Run) error {
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	}))
	// Let a reaper run the whole time, so an unrenewed lease would be taken.
	reapCtx, stopReaper := context.WithCancel(t.Context())
	defer stopReaper()
	go func() {
		for reapCtx.Err() == nil {
			_, _ = store.Reap(reapCtx, env.Pool, 500)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	id := env.Enqueue(t, shopA, kind, nil)
	<-started
	first := *env.WaitState(t, id, store.StateRunning, time.Second).LeaseExpiresAt
	queuetest.Eventually(t, time.Second, "the lease to be extended", func() bool {
		exp := env.Job(t, id).LeaseExpiresAt
		return exp != nil && exp.After(first)
	})
	rep := reports.Next(t, 10*time.Second)
	if rep.Outcome != queue.OutcomeCompleted {
		t.Fatalf("report = %+v, want completed", rep)
	}
	if j := env.Job(t, id); j.ClaimSeq != 1 || j.Attempts != 0 {
		t.Errorf("job = %+v, want one claim and no failed attempt", j)
	}
}

// AC-4.3: when a heartbeat affects 0 rows, the handler's context is cancelled
// within one heartbeat interval, and no result is written.
func TestLostLeaseCancelsTheHandlerWithinOneHeartbeat(t *testing.T) {
	t.Parallel()
	const heartbeat = 100 * time.Millisecond
	env := newEnv(t, 2000, int(heartbeat.Milliseconds()), 10_000, 5)
	running := make(chan struct{})
	cancelled := make(chan error, 1)
	var cancelledAt atomic.Int64
	_, reports := env.StartWorker(t, registry(func(ctx context.Context, _ *queue.Run) error {
		close(running)
		<-ctx.Done()
		cancelledAt.Store(time.Now().UnixNano())
		cancelled <- context.Cause(ctx)
		return nil // even a nil result must not be written
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	<-running
	// The lease is lost: it expires now, by the database clock.
	if _, err := env.Pool.Exec(t.Context(), `UPDATE jobs SET lease_expires_at = statement_timestamp() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	lostAt := time.Now()
	cause := <-cancelled
	if elapsed := time.Duration(cancelledAt.Load() - lostAt.UnixNano()); elapsed > heartbeat+100*time.Millisecond {
		t.Errorf("handler cancelled %v after the lease was lost, want within one heartbeat interval (%v) plus statement and scheduling time", elapsed, heartbeat)
	}
	if !errors.Is(cause, lease.ErrLeaseLost) {
		t.Errorf("cancel cause = %v, want ErrLeaseLost", cause)
	}
	rep := reports.Next(t, 5*time.Second)
	if rep.Outcome != queue.OutcomeLeaseLost {
		t.Fatalf("report = %+v, want lease_lost", rep)
	}
	if j := env.Job(t, id); j.State != store.StateRunning || j.Attempts != 0 {
		t.Errorf("job = %+v, want it untouched by the worker (the reaper takes it)", j)
	}
}

// Design §7: when heartbeats fail for about a whole lease, the worker cancels
// the handler early and writes nothing.
func TestFailingHeartbeatsCancelTheHandler(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 300, 50, 10_000, 5)
	_, reports := env.StartWorker(t, registry(func(ctx context.Context, _ *queue.Run) error {
		<-ctx.Done()
		return ctx.Err()
	}), func(c *queue.Config) {
		c.Heartbeat = func(context.Context, []store.Fence) ([]int64, error) { return nil, errors.New("postgres unreachable") }
	})
	env.Enqueue(t, shopA, kind, nil)
	rep := reports.Next(t, 10*time.Second)
	if rep.Outcome != queue.OutcomeLeaseLost || !errors.Is(rep.Err, lease.ErrHeartbeatsFailing) {
		t.Fatalf("report = %+v, want lease_lost from failing heartbeats", rep)
	}
	if took := rep.Finished.Sub(rep.Started); took < 250*time.Millisecond {
		t.Errorf("gave up after %v, before lease minus one heartbeat (250ms)", took)
	}
}

// netReset stands in for a connection reset while COMMIT was in flight: not a
// server error, and not safe to retry, so the outcome is unknown.
var netReset = &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}

// Design §7: after a commit with an unknown outcome, the worker reads the job
// back. "completed at my claim_seq" means the commit landed.
func TestAmbiguousCommitThatLanded(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	createEffects(t, env)
	_, reports := env.StartWorker(t, registry(writeEffect), func(c *queue.Config) {
		c.Commit = func(ctx context.Context, tx pgx.Tx) error {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return netReset
		}
	})
	id := env.Enqueue(t, shopA, kind, nil)
	rep := reports.Next(t, 10*time.Second)
	if !rep.AmbiguousCommit || rep.Outcome != queue.OutcomeCompleted {
		t.Fatalf("report = %+v, want an ambiguous commit resolved as completed", rep)
	}
	if j := env.Job(t, id); j.State != store.StateCompleted || effects(t, env, id) != 1 {
		t.Errorf("job = %+v with %d effects, want completed with 1", j, effects(t, env, id))
	}
}

// Design §7: if the read-back shows the commit did not land, the worker
// abandons the job; its lease expires, and the next run completes it.
func TestAmbiguousCommitThatDidNotLand(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 300, 100, 10_000, 5)
	createEffects(t, env)
	var commits atomic.Int32
	_, reports := env.StartWorker(t, registry(writeEffect), func(c *queue.Config) {
		c.Commit = func(ctx context.Context, tx pgx.Tx) error {
			if commits.Add(1) == 1 {
				_ = tx.Rollback(ctx)
				return netReset
			}
			return tx.Commit(ctx)
		}
	})
	reapCtx, stopReaper := context.WithCancel(t.Context())
	defer stopReaper()
	go func() {
		for reapCtx.Err() == nil {
			_, _ = store.Reap(reapCtx, env.Pool, 500)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	id := env.Enqueue(t, shopA, kind, nil)
	rep := reports.Next(t, 10*time.Second)
	if !rep.AmbiguousCommit || rep.Outcome != queue.OutcomeAbandoned {
		t.Fatalf("report = %+v, want an ambiguous commit resolved as abandoned", rep)
	}
	j := env.WaitState(t, id, store.StateCompleted, 10*time.Second)
	if j.ClaimSeq != 2 || j.Attempts != 1 || effects(t, env, id) != 1 {
		t.Errorf("job = %+v with %d effects, want the second run to complete it once", j, effects(t, env, id))
	}
}

// A commit that certainly failed is a temporary failure, not an ambiguous one.
func TestCommitThatCertainlyFailedRetries(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	var commits atomic.Int32
	_, reports := env.StartWorker(t, registry(func(context.Context, *queue.Run) error { return nil }), func(c *queue.Config) {
		c.Commit = func(ctx context.Context, tx pgx.Tx) error {
			if commits.Add(1) == 1 {
				_ = tx.Rollback(ctx)
				return pgx.ErrTxCommitRollback
			}
			return tx.Commit(ctx)
		}
	})
	id := env.Enqueue(t, shopA, kind, nil)
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeRetry || rep.AmbiguousCommit {
		t.Fatalf("report = %+v, want a plain retry", rep)
	}
	if j := env.WaitState(t, id, store.StateCompleted, 10*time.Second); j.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", j.Attempts)
	}
}

// Kill stops the worker like a crash: Run returns promptly and nothing more is written.
func TestKillWritesNothing(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	running := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	w, reports := env.StartWorker(t, registry(func(context.Context, *queue.Run) error {
		close(running)
		<-release // ignores cancellation
		return nil
	}))
	id := env.Enqueue(t, shopA, kind, nil)
	<-running
	w.Kill()
	if rep := reports.Next(t, 5*time.Second); rep.Outcome != queue.OutcomeAbandoned {
		t.Fatalf("report = %+v, want abandoned", rep)
	}
	if j := env.Job(t, id); j.State != store.StateRunning || j.Attempts != 0 {
		t.Errorf("job = %+v, want it left running for the reaper", j)
	}
}

// A graceful stop lets the in-flight run finish and write its result.
func TestGracefulStopFinishesInFlightRuns(t *testing.T) {
	t.Parallel()
	env := newEnv(t, 1000, 200, 10_000, 5)
	reports := queuetest.NewReports()
	running := make(chan struct{})
	w, err := queue.New(queue.Config{
		DB: env.Pool, Claims: queuetest.DirectClaims{DB: env.Pool}, Slots: 1, Identity: "test/1",
		Registry: registry(func(ctx context.Context, _ *queue.Run) error {
			close(running)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
				return nil
			}
		}),
		HeartbeatInterval: 200 * time.Millisecond, Backoff: queue.DefaultBackoff, Log: queuetest.Logger(t), OnReport: reports.Add,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	id := env.Enqueue(t, shopA, kind, nil)
	<-running
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the in-flight run finished")
	}
	if j := env.Job(t, id); j.State != store.StateCompleted {
		t.Errorf("job = %+v, want completed", j)
	}
	if all := reports.All(); len(all) != 1 || all[0].Outcome != queue.OutcomeCompleted {
		t.Errorf("reports = %+v", all)
	}
}

func TestOwnersAreUniquePerSlotAndBoot(t *testing.T) {
	cfg := queue.Config{DB: nil, Claims: queuetest.DirectClaims{}, Registry: queue.NewRegistry(), Slots: 2,
		HeartbeatInterval: time.Second, Backoff: queue.DefaultBackoff, Identity: "host/42", Log: queuetest.Logger(t)}
	if _, err := queue.New(cfg); err == nil {
		t.Fatal("New without a DB succeeded")
	}
	env := newEnv(t, 1000, 200, 10_000, 5)
	cfg.DB = env.Pool
	a, err := queue.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := queue.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(a.Owner(1), "host/42/1/") || a.Owner(0) == a.Owner(1) || a.Owner(1) == b.Owner(1) {
		t.Errorf("owners %q %q %q: want <host>/<pid>/<slot>/<boot nonce>, unique per slot and boot", a.Owner(0), a.Owner(1), b.Owner(1))
	}
}
