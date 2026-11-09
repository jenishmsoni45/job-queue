package invariants

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

const (
	shopA = queuetest.ShopA
	shopB = queuetest.ShopB
	shopC = queuetest.ShopC
)

// shortLeases sets every test kind's lease and heartbeat.
func shortLeases(leaseMS, heartbeatMS, maxAttempts int) func(*config.File) {
	return func(c *config.File) {
		for i := range c.Kinds {
			c.Kinds[i].LeaseMS, c.Kinds[i].HeartbeatMS, c.Kinds[i].MaxAttempts = leaseMS, heartbeatMS, maxAttempts
		}
	}
}

// staleWrites tries every fenced write a stale worker could make and fails the
// test if any of them changes a row.
func staleWrites(t *testing.T, env *queuetest.Env, f store.Fence) {
	t.Helper()
	ctx := context.Background()
	if ok, err := store.Complete(ctx, env.Pool, f); err != nil || ok {
		t.Errorf("stale complete = %v, %v; want 0 rows", ok, err)
	}
	if res, err := store.Fail(ctx, env.Pool, f, "stale failure", 0); err != nil || res.Applied {
		t.Errorf("stale fail (reschedule) = %+v, %v; want 0 rows", res, err)
	}
	if ok, err := store.Permanent(ctx, env.Pool, f, "stale permanent"); err != nil || ok {
		t.Errorf("stale permanent = %v, %v; want 0 rows", ok, err)
	}
	if alive, err := store.Heartbeat(ctx, env.Pool, []store.Fence{f}); err != nil || len(alive) != 0 {
		t.Errorf("stale heartbeat = %v, %v; want no lease revived", alive, err)
	}
}

// P2-1, Q2 at the SQL: a worker held past its lease cannot complete, fail or
// reschedule, both after the reaper requeued the job (and another worker
// completed it) and before the reaper has run.
func TestQ2StaleWritesAffectNoRows(t *testing.T) {
	t.Parallel()
	t.Run("after the reaper requeued and another worker completed", func(t *testing.T) {
		t.Parallel()
		env := queuetest.New(t, queuetest.Config(t, shortLeases(200, 50, 5)))
		id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
		a := claim(t, env, "worker-a")
		time.Sleep(250 * time.Millisecond)
		if r, err := store.Reap(t.Context(), env.Pool, 500); err != nil || r.Requeued != 1 {
			t.Fatalf("reap = %+v, %v", r, err)
		}
		b := claim(t, env, "worker-b")
		if ok, err := store.Complete(t.Context(), env.Pool, b.Fence()); err != nil || !ok {
			t.Fatalf("worker b's complete = %v, %v", ok, err)
		}
		before := env.Job(t, id)
		staleWrites(t, env, a.Fence())
		if after := env.Job(t, id); !sameJob(before, after) || after.State != store.StateCompleted || after.ClaimSeq != 2 {
			t.Errorf("stale writes changed the job:\nbefore %+v\nafter  %+v", before, after)
		}
		env.CheckBacklog(t)
	})
	t.Run("before the reaper has run", func(t *testing.T) {
		t.Parallel()
		env := queuetest.New(t, queuetest.Config(t, shortLeases(200, 50, 5)))
		id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
		a := claim(t, env, "worker-a")
		time.Sleep(250 * time.Millisecond)
		before := env.Job(t, id)
		staleWrites(t, env, a.Fence())
		if after := env.Job(t, id); !sameJob(before, after) || after.State != store.StateRunning || after.Attempts != 0 {
			t.Errorf("stale writes changed the job:\nbefore %+v\nafter  %+v", before, after)
		}
		env.CheckBacklog(t)
	})
}

// P2-1, Q2 through the worker. Worker A believes it still holds the lease: its
// heartbeats report success but renew nothing, as for a process that paused
// between heartbeats. Its handler writes an effect in run.Tx() and returns
// after the lease expired. Whatever it returns, its write affects 0 rows and
// its effect rolls back (AC-4.2, AC-7.2).
func TestQ2PausedWorkerCannotWrite(t *testing.T) {
	t.Parallel()
	results := map[string]error{
		"complete":   nil,
		"reschedule": errors.New("temporary failure"),
		"permanent":  queue.Permanent(errors.New("permanent failure")),
	}
	for name, result := range results {
		for _, reaped := range []bool{true, false} {
			label := name + "/before the reaper ran"
			if reaped {
				label = name + "/after the reaper requeued and worker b completed"
			}
			t.Run(label, func(t *testing.T) {
				t.Parallel()
				pausedWorkerScenario(t, result, reaped)
			})
		}
	}
}

func pausedWorkerScenario(t *testing.T, result error, reaped bool) {
	env := queuetest.New(t, queuetest.Config(t, shortLeases(300, 100, 5)))
	if _, err := env.Pool.Exec(t.Context(), `CREATE TABLE effects (job_id bigint, claim_seq int)`); err != nil {
		t.Fatal(err)
	}
	effect := func(ctx context.Context, run *queue.Run) error {
		tx, err := run.Tx(ctx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO effects VALUES ($1, $2)`, run.Job.ID, run.Job.ClaimSeq)
		return err
	}

	claimed, resume := make(chan struct{}), make(chan struct{})
	regA := queue.NewRegistry()
	regA.Register(testkinds.Sleep, func(ctx context.Context, run *queue.Run) error {
		if err := effect(ctx, run); err != nil {
			return err
		}
		close(claimed)
		<-resume // paused past the lease
		return result
	})
	_, reportsA := env.StartWorker(t, regA, func(c *queue.Config) {
		c.Slots = 1
		c.Identity = "worker-a/1"
		c.Heartbeat = func(_ context.Context, leases []store.Fence) ([]int64, error) {
			ids := make([]int64, len(leases))
			for i, l := range leases {
				ids[i] = l.JobID
			}
			return ids, nil // "renewed", but nothing reached the database
		}
	})
	id := env.Enqueue(t, shopA, testkinds.Sleep, nil)
	<-claimed
	time.Sleep(400 * time.Millisecond) // the lease expires by the database clock

	if reaped {
		if r, err := store.Reap(t.Context(), env.Pool, 500); err != nil || r.Requeued != 1 {
			t.Fatalf("reap = %+v, %v", r, err)
		}
		regB := queue.NewRegistry()
		regB.Register(testkinds.Sleep, effect)
		_, reportsB := env.StartWorker(t, regB, func(c *queue.Config) { c.Identity = "worker-b/1" })
		if rep := reportsB.Next(t, 10*time.Second); rep.Outcome != queue.OutcomeCompleted {
			t.Fatalf("worker b: %+v, want completed", rep)
		}
	}
	before := env.Job(t, id)
	close(resume)

	rep := reportsA.Next(t, 10*time.Second)
	if rep.Outcome != queue.OutcomeLeaseLost {
		t.Fatalf("worker a: %+v, want lease_lost (its write affected 0 rows)", rep)
	}
	after := env.Job(t, id)
	if !sameJob(before, after) {
		t.Errorf("worker a's write changed the job:\nbefore %+v\nafter  %+v", before, after)
	}
	var fromA, total int
	if err := env.Pool.QueryRow(t.Context(),
		`SELECT count(*) FILTER (WHERE claim_seq = 1), count(*) FROM effects WHERE job_id = $1`, id).Scan(&fromA, &total); err != nil {
		t.Fatal(err)
	}
	if fromA != 0 {
		t.Errorf("worker a's run.Tx() effect committed although its fence failed")
	}
	if wantTotal := map[bool]int{true: 1, false: 0}[reaped]; total != wantTotal {
		t.Errorf("%d effects, want %d", total, wantTotal)
	}
	env.CheckBacklog(t)
}

func claim(t *testing.T, env *queuetest.Env, owner string) store.Claimed {
	t.Helper()
	jobs, err := store.Claim(t.Context(), env.Pool, []string{owner})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim for %s = %v, %v; want 1 job", owner, jobs, err)
	}
	return jobs[0]
}

// sameJob compares the fields a result write would change.
func sameJob(a, b store.Job) bool {
	eqTime := func(x, y *time.Time) bool { return (x == nil) == (y == nil) && (x == nil || x.Equal(*y)) }
	eqStr := func(x, y *string) bool { return (x == nil) == (y == nil) && (x == nil || *x == *y) }
	return a.State == b.State && a.ClaimSeq == b.ClaimSeq && a.Attempts == b.Attempts && a.Throttles == b.Throttles &&
		a.RunAt.Equal(b.RunAt) && eqStr(a.LeaseOwner, b.LeaseOwner) && eqTime(a.LeaseExpiresAt, b.LeaseExpiresAt) &&
		eqTime(a.CompletedAt, b.CompletedAt) && eqTime(a.DeadAt, b.DeadAt) && eqStr(a.DeadReason, b.DeadReason) &&
		eqStr(a.LastError, b.LastError)
}
