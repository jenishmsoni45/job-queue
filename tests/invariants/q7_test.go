package invariants

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/handlers"
	"github.com/jenishmsoni45/job-queue/internal/idem"
	"github.com/jenishmsoni45/job-queue/internal/mockshop"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/scheduler"
	"github.com/jenishmsoni45/job-queue/internal/store"
)

// throttleLogSQL records every change to a shop's throttled_until with the
// statement time of the throttle that made it, for the AC-6.2 check.
const throttleLogSQL = `
CREATE TABLE IF NOT EXISTS throttle_log (
  shop_id text        NOT NULL,
  at      timestamptz NOT NULL,
  until   timestamptz NOT NULL
);
CREATE OR REPLACE FUNCTION log_throttle() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.throttled_until IS DISTINCT FROM OLD.throttled_until THEN
    INSERT INTO throttle_log VALUES (NEW.shop_id, statement_timestamp(), NEW.throttled_until);
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS log_throttle ON shop_state;
CREATE TRIGGER log_throttle AFTER UPDATE ON shop_state FOR EACH ROW EXECUTE FUNCTION log_throttle();`

// startMockshop serves mockshop's Admin API for cfg's shops in process.
func startMockshop(t *testing.T, cfg *config.File) (*mockshop.API, *adminapi.Client) {
	t.Helper()
	api := mockshop.NewAPI(cfg, nil)
	mux := http.NewServeMux()
	api.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return api, &adminapi.Client{BaseURL: srv.URL, HTTP: srv.Client()}
}

// P5-1, Q7 (NFR-7, AC-6.1), with P5-2's AC-6.2 through the dispatcher. One
// shop's mockshop bucket holds a single call and leaks 20 a second, while four
// workers run 40 order.tag jobs for it through a real dispatcher, so most calls
// are throttled. Every job completes; no run ever saw an attempt, so attempts
// stayed 0 through every throttle; some jobs were throttled; and each job's tag
// was applied exactly once. Once a throttle committed, no claim for the shop
// started before its throttled_until, give or take the throttle's own commit.
func TestQ7ThrottlesNeverUseAnAttempt(t *testing.T) {
	t.Parallel()
	cfg := queuetest.Config(t, queuetest.Edits(
		queuetest.WithShop(shopA, func(s *config.Shop) {
			s.MaxConcurrency, s.MockAPICapacity, s.MockAPILeakPerSec = 4, 1, 20
		}),
		queuetest.WithKind(handlers.OrderTag, func(k *config.Kind) {
			// A long lease and a big throttle budget: only throttles may happen.
			k.LeaseMS, k.HeartbeatMS, k.MaxThrottles, k.MaxAttempts = 10_000, 1000, 10_000, 1
		}),
	))
	env := queuetest.New(t, cfg)
	if _, err := env.Pool.Exec(t.Context(), queuetest.LeaseLogSQL+throttleLogSQL); err != nil {
		t.Fatal(err)
	}
	api, admin := startMockshop(t, cfg)

	const jobs = 40
	ids := make([]int64, jobs)
	for i := range ids {
		ids[i] = env.Enqueue(t, shopA, handlers.OrderTag, handlers.OrderTagPayload{OrderID: int64(1000 + i)})
	}

	reg := queue.NewRegistry()
	handlers.Register(reg, cfg, admin)
	var mu sync.Mutex
	var bad []string
	claims := env.StartDispatcher(t, "q7", scheduler.ModeDRR)
	_, reports := env.StartWorker(t, reg, func(c *queue.Config) {
		c.Claims, c.Slots = claims, 4
		c.OnReport = func(rep queue.Report) {
			if rep.Job.Attempts != 0 || (rep.Outcome != queue.OutcomeThrottled && rep.Outcome != queue.OutcomeCompleted) {
				mu.Lock()
				bad = append(bad, fmt.Sprintf("job %d: attempts %d at claim, outcome %s (%v)", rep.Job.ID, rep.Job.Attempts, rep.Outcome, rep.Err))
				mu.Unlock()
			}
		}
	})
	_ = reports

	queuetest.Eventually(t, 60*time.Second, "every job to complete", func() bool {
		var left int
		if err := env.Pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE state <> 'completed'`).Scan(&left); err != nil {
			t.Fatal(err)
		}
		return left == 0
	})
	mu.Lock()
	for _, b := range bad {
		t.Error(b)
	}
	mu.Unlock()

	var attempts, throttled, throttles int
	if err := env.Pool.QueryRow(t.Context(), `
		SELECT coalesce(sum(attempts), 0), count(*) FILTER (WHERE throttles > 0), coalesce(sum(throttles), 0)
		  FROM jobs`).Scan(&attempts, &throttled, &throttles); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Errorf("jobs used %d attempts in all, want 0 (Q7)", attempts)
	}
	if throttled == 0 {
		t.Error("no job was throttled; the bucket of 1 should have throttled many calls")
	}
	t.Logf("%d of %d jobs were throttled, %d throttles in all", throttled, jobs, throttles)

	m, _ := api.Mutations(shopA)
	for _, id := range ids {
		if n := m.Applied[idem.Key(id, handlers.TagStep)]; n != 1 {
			t.Errorf("job %d applied %d mutations, want 1", id, n)
		}
	}

	// AC-6.2: a claim's lease starts at its UPDATE, a little after its throttle
	// check, and a throttle's commit comes a little after its statement, so the
	// check allows 50 ms around the throttle's statement time.
	var violations []string
	rows, err := env.Pool.Query(t.Context(), `
		SELECT l.job_id, l.started_at, th.at, th.until
		  FROM lease_log l JOIN jobs j ON j.id = l.job_id
		  JOIN throttle_log th ON th.shop_id = j.shop_id
		 WHERE l.started_at > th.at + interval '50 milliseconds' AND l.started_at < th.until`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		var started, at, until time.Time
		if err := rows.Scan(&id, &started, &at, &until); err != nil {
			t.Fatal(err)
		}
		violations = append(violations, fmt.Sprintf("job %d claimed at %v, after a throttle at %v paused the shop until %v", id, started, at, until))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, v := range violations {
		t.Error(v)
	}
	var logged int
	if err := env.Pool.QueryRow(t.Context(), `SELECT count(*) FROM throttle_log`).Scan(&logged); err != nil || logged == 0 {
		t.Errorf("throttle log has %d rows (%v); the AC-6.2 check saw no throttle", logged, err)
	}
	env.CheckBacklog(t)
}

// P5-2, AC-6.3 end to end: a shop whose bucket never frees up (it leaks one
// call an hour) throttles a job until its budget runs out, and the job goes dead
// with "throttle budget exhausted", having used no attempt.
func TestThrottleBudgetEndsInDead(t *testing.T) {
	t.Parallel()
	cfg := queuetest.Config(t, queuetest.Edits(
		queuetest.WithShop(shopB, func(s *config.Shop) { s.MockAPICapacity, s.MockAPILeakPerSec = 1, 1.0/3600 }),
		queuetest.WithKind(handlers.OrderTag, func(k *config.Kind) { k.LeaseMS, k.HeartbeatMS, k.MaxThrottles = 10_000, 1000, 3 }),
	))
	env := queuetest.New(t, cfg)
	_, admin := startMockshop(t, cfg)
	// Fill the bucket, so every call from the job is throttled.
	if err := admin.TagOrder(t.Context(), shopB, 1, "fill", ""); err != nil {
		t.Fatal(err)
	}
	id := env.Enqueue(t, shopB, handlers.OrderTag, handlers.OrderTagPayload{OrderID: 2})
	// mockshop asks for an hour's wait; make each throttled job runnable again
	// and lift the pause at once, so the test needn't wait.
	reg := queue.NewRegistry()
	reg.Register(handlers.OrderTag, func(ctx context.Context, run *queue.Run) error {
		err := handlers.OrderTagHandler(admin, 0)(ctx, run)
		if d, ok := queue.IsThrottled(err); ok && d > 0 {
			return queue.Throttled(0)
		}
		return err
	})
	env.StartWorker(t, reg)
	j := env.WaitState(t, id, store.StateDead, 30*time.Second)
	if j.DeadReason == nil || *j.DeadReason != store.ReasonThrottleBudget || j.Attempts != 0 || j.Throttles != 4 {
		raw, _ := json.Marshal(j)
		t.Errorf("job = %s, want dead by the throttle budget after 4 throttles and no attempt", raw)
	}
	env.CheckBacklog(t)
}
