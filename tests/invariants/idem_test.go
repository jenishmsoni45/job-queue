package invariants

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/idem"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// stalledHeartbeats pretends every lease was renewed while nothing reaches the
// database, as a paused process would see it.
func stalledHeartbeats(c *queue.Config) {
	c.Heartbeat = func(_ context.Context, leases []store.Fence) ([]int64, error) {
		ids := make([]int64, len(leases))
		for i, l := range leases {
			ids[i] = l.JobID
		}
		return ids, nil
	}
}

// P3-5, AC-7.1: two runs of one job overlap inside run.Step. Run 1 holds its
// step open past its lease; the job is reaped and run 2 reaches the same step,
// which waits for run 1's step to commit and then skips it. The step's effect
// is applied once, by run 1, and run 2 completes the job.
func TestStepAppliesOnceWhenRunsOverlap(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, shortLeases(300, 100, 5)))
	ctx := t.Context()
	if _, err := env.Pool.Exec(ctx, `CREATE TABLE step_effects (job_id bigint, claim_seq int)`); err != nil {
		t.Fatal(err)
	}
	inStep, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	// If the test fails early, let run 1 finish, so its connection goes back to
	// the pool and the cleanup doesn't hang.
	t.Cleanup(release)
	var stepCalls sync.Map // claim_seq → true once fn ran
	handler := func(ctx context.Context, run *queue.Run) error {
		return run.Step(ctx, "apply", func(ctx context.Context, tx pgx.Tx) error {
			stepCalls.Store(run.Job.ClaimSeq, true)
			if _, err := tx.Exec(ctx, `INSERT INTO step_effects VALUES ($1, $2)`, run.Job.ID, run.Job.ClaimSeq); err != nil {
				return err
			}
			if run.Job.ClaimSeq == 1 {
				close(inStep)
				<-resume // run 1 is paused inside its step, past its lease
			}
			return nil
		})
	}
	regA := queue.NewRegistry()
	regA.Register(testkinds.Sleep, handler)
	_, reportsA := env.StartWorker(t, regA, func(c *queue.Config) {
		c.Slots, c.Identity = 1, "worker-a/1"
		stalledHeartbeats(c)
	})
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	<-inStep
	time.Sleep(400 * time.Millisecond) // run 1's lease expires by the database clock
	// Run 1's open step holds its idempotency key; the reaper still requeues the job (D-047).
	if r, err := store.Reap(ctx, env.Pool, 500); err != nil || r.Requeued != 1 {
		t.Fatalf("reap = %+v, %v", r, err)
	}

	regB := queue.NewRegistry()
	regB.Register(testkinds.Sleep, handler)
	_, reportsB := env.StartWorker(t, regB, func(c *queue.Config) { c.Slots, c.Identity = 1, "worker-b/1" })
	// Run 2 is claimed and blocks in idem.Once behind run 1's open step.
	queuetest.Eventually(t, 5*time.Second, "run 2 to claim the job", func() bool { return env.Job(t, id).ClaimSeq == 2 })
	time.Sleep(200 * time.Millisecond)
	if all := reportsB.All(); len(all) != 0 {
		t.Fatalf("run 2 finished before run 1's step committed: %+v", all)
	}
	release()

	if rep := reportsB.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted {
		t.Fatalf("run 2: %+v, want completed", rep)
	}
	if rep := reportsA.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeLeaseLost {
		t.Fatalf("run 1: %+v, want lease_lost", rep)
	}
	if _, ran := stepCalls.Load(int32(2)); ran {
		t.Error("run 2 ran the step's function although run 1 had committed the step")
	}
	var n int
	var seq int32
	if err := env.Pool.QueryRow(ctx, `SELECT count(*), min(claim_seq) FROM step_effects WHERE job_id = $1`, id).Scan(&n, &seq); err != nil {
		t.Fatal(err)
	}
	if n != 1 || seq != 1 {
		t.Errorf("%d step effects (first from claim %d), want exactly one, from run 1", n, seq)
	}
	env.CheckBacklog(t)
}

// run.Step: a step whose function fails is rolled back and not recorded, so
// the next run applies it; a committed step is skipped on every later run. The
// step's function can't commit the transaction itself, and a bad step name is
// refused.
func TestStepRetriesAFailedStepAndSkipsACommittedOne(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, shortLeases(2000, 200, 5)))
	ctx := t.Context()
	if _, err := env.Pool.Exec(ctx, `CREATE TABLE step_effects (job_id bigint, step text, claim_seq int)`); err != nil {
		t.Fatal(err)
	}
	reg := queue.NewRegistry()
	reg.Register(testkinds.Sleep, func(ctx context.Context, run *queue.Run) error {
		if err := run.Step(ctx, "bad step", func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, idem.ErrBadStep) {
			return queue.Permanent(errors.New("a bad step name was not refused"))
		}
		for _, step := range []string{"one", "two"} {
			err := run.Step(ctx, step, func(ctx context.Context, tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `INSERT INTO step_effects VALUES ($1, $2, $3)`, run.Job.ID, step, run.Job.ClaimSeq); err != nil {
					return err
				}
				if err := tx.Commit(ctx); !errors.Is(err, queue.ErrManagedTx) {
					return errors.New("the step's function could commit its own transaction")
				}
				if step == "two" && run.Job.Attempts == 0 {
					return errors.New("step two fails on the first run")
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	_, reports := env.StartWorker(t, reg)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeRetry {
		t.Fatalf("run 1: %+v, want a retry", rep)
	}
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted {
		t.Fatalf("run 2: %+v, want completed", rep)
	}
	rows, err := env.Pool.Query(ctx, `SELECT step || '@' || claim_seq FROM step_effects WHERE job_id = $1 ORDER BY step`, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "one@1,two@2" {
		t.Errorf("effects = %v, want step one from run 1 only and step two from run 2 only", got)
	}
}

// P3-5, AC-7.2: test.effect writes its effect through run.Tx(). When the run's
// lease is lost, the fenced completion affects 0 rows and the effect rolls back
// with it; the run that does complete commits exactly one effect.
func TestRunTxEffectRollsBackWhenFencingFails(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, shortLeases(300, 100, 5)))
	ctx := t.Context()
	if _, err := env.Pool.Exec(ctx, testkinds.EffectsTableSQL); err != nil {
		t.Fatal(err)
	}
	regA := queue.NewRegistry()
	testkinds.Register(regA)
	workerA, reportsA := env.StartWorker(t, regA, func(c *queue.Config) {
		c.Slots, c.Identity = 1, "worker-a/1"
		stalledHeartbeats(c) // the lease expires while the handler sleeps
	})
	id := env.Enqueue(t, shopA, testkinds.Effect, map[string]int{"ms": 600})
	if rep := reportsA.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeLeaseLost {
		t.Fatalf("run 1: %+v, want lease_lost (its fenced completion affected 0 rows)", rep)
	}
	workerA.Kill() // so that only worker b can claim the job again
	var n int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM test_effects WHERE job_id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d effects after a run whose fence failed, want 0", n)
	}

	if r, err := store.Reap(ctx, env.Pool, 500); err != nil || r.Requeued != 1 {
		t.Fatalf("reap = %+v, %v", r, err)
	}
	regB := queue.NewRegistry()
	testkinds.Register(regB)
	_, reportsB := env.StartWorker(t, regB, func(c *queue.Config) { c.Identity = "worker-b/1" })
	if rep := reportsB.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted {
		t.Fatalf("run 2: %+v, want completed", rep)
	}
	var seq int32
	if err := env.Pool.QueryRow(ctx, `SELECT count(*), max(claim_seq) FROM test_effects WHERE job_id = $1`, id).Scan(&n, &seq); err != nil {
		t.Fatal(err)
	}
	if n != 1 || seq != env.Job(t, id).ClaimSeq {
		t.Errorf("%d effects (claim %d), want one, from the completing run", n, seq)
	}
	env.CheckBacklog(t)
}

// fakeAPI is an HTTP test server that honours Idempotency-Key the way design
// §12 says mockshop will: the first request with a key is applied and its
// response stored; repeats replay that response and apply nothing.
type fakeAPI struct {
	mu      sync.Mutex
	applied map[string]int
	stored  map[string]string
	seen    []string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := r.Header.Get("Idempotency-Key")
	f.seen = append(f.seen, key)
	if key == "" {
		http.Error(w, "missing Idempotency-Key", http.StatusBadRequest)
		return
	}
	if body, ok := f.stored[key]; ok {
		_, _ = w.Write([]byte(body))
		return
	}
	f.applied[key]++
	f.stored[key] = `{"applied": true}`
	_, _ = w.Write([]byte(f.stored[key]))
}

// P3-5, AC-7.3: a job's handler calls an external API with idem.Key, then
// fails, so the job runs again and makes the call again. Both runs send the
// same key, and the server applies the call once.
func TestIdempotencyKeyIsStableAcrossRuns(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, shortLeases(2000, 200, 5)))
	fake := &fakeAPI{applied: map[string]int{}, stored: map[string]string{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	reg := queue.NewRegistry()
	reg.Register(testkinds.Sleep, func(ctx context.Context, run *queue.Run) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/tags", strings.NewReader(`{"tag": "vip"}`))
		if err != nil {
			return err
		}
		req.Header.Set("Idempotency-Key", idem.Key(run.Job.ID, "tag"))
		resp, err := srv.Client().Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if run.Job.Attempts == 0 {
			return errors.New("the run fails after its call, so the job runs again")
		}
		return nil
	})
	_, reports := env.StartWorker(t, reg)
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeRetry {
		t.Fatalf("run 1: %+v, want a retry", rep)
	}
	if rep := reports.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted {
		t.Fatalf("run 2: %+v, want completed", rep)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	want := idem.Key(id, "tag")
	if len(fake.seen) != 2 || fake.seen[0] != want || fake.seen[1] != want {
		t.Errorf("keys sent = %q, want %q twice", fake.seen, want)
	}
	if fake.applied[want] != 1 || len(fake.applied) != 1 {
		t.Errorf("applied = %v, want the call applied once", fake.applied)
	}
}
