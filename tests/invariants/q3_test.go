package invariants

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// leaseLogSQL installs a test-only trigger that records every lease with
// database times: when a claim starts it, and when it ends. A lease ends at the
// fenced write that moved the job out of running, or at its expiry if that
// came first (a reaped lease ended when it expired, not when it was reaped).
const leaseLogSQL = `
CREATE TABLE lease_log (
  job_id     bigint      NOT NULL,
  claim_seq  int         NOT NULL,
  owner      text        NOT NULL,
  started_at timestamptz NOT NULL,
  ended_at   timestamptz,
  PRIMARY KEY (job_id, claim_seq)       -- one owner per (job, claim_seq)
);
CREATE FUNCTION log_lease() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state = 'running' AND (NEW.state <> 'running' OR NEW.claim_seq <> OLD.claim_seq) THEN
    UPDATE lease_log SET ended_at = least(statement_timestamp(), OLD.lease_expires_at)
     WHERE job_id = OLD.id AND claim_seq = OLD.claim_seq;
  END IF;
  IF NEW.state = 'running' AND (OLD.state <> 'running' OR NEW.claim_seq <> OLD.claim_seq) THEN
    INSERT INTO lease_log VALUES (NEW.id, NEW.claim_seq, NEW.lease_owner, statement_timestamp(), NULL);
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER log_lease AFTER UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION log_lease();`

type leaseRecord struct {
	jobID   int64
	seq     int32
	owner   string
	started time.Time
	ended   *time.Time
}

// P2-2, Q3: many concurrent claimers run the real claim SQL against the same
// jobs, with no dispatcher, while jobs complete, fail and are requeued at once,
// and leases are abandoned and reaped. Every (job, claim_seq) has exactly one
// owner, and the leases of one job never overlap by the database clock.
func TestQ3AtMostOneLiveLeasePerJob(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, queuetest.Config(t, shortLeases(40, 10, 1000)))
	if _, err := env.Pool.Exec(t.Context(), leaseLogSQL); err != nil {
		t.Fatal(err)
	}

	rapid.Check(t, func(rt *rapid.T) {
		ctx := context.Background()
		if _, err := env.Pool.Exec(ctx, `TRUNCATE jobs, lease_log; UPDATE shop_state SET backlog = 0`); err != nil {
			rt.Fatal(err)
		}
		jobs := rapid.IntRange(1, 10).Draw(rt, "jobs")
		claimers := rapid.IntRange(2, 8).Draw(rt, "claimers")
		type step struct {
			batch   int
			actions []string // per claimed job: complete, fail or abandon
			pauseMS int
		}
		scripts := make([][]step, claimers)
		for c := range scripts {
			rounds := rapid.IntRange(1, 8).Draw(rt, fmt.Sprintf("rounds[%d]", c))
			for r := range rounds {
				batch := rapid.IntRange(1, 3).Draw(rt, fmt.Sprintf("batch[%d][%d]", c, r))
				actions := rapid.SliceOfN(rapid.SampledFrom([]string{"complete", "fail", "abandon"}), batch, batch).
					Draw(rt, fmt.Sprintf("actions[%d][%d]", c, r))
				pause := rapid.IntRange(0, 20).Draw(rt, fmt.Sprintf("pause[%d][%d]", c, r))
				scripts[c] = append(scripts[c], step{batch, actions, pause})
			}
		}
		for range jobs {
			if _, err := store.Enqueue(ctx, env.Pool, store.NewJob{Shop: shopA, Kind: testkinds.Sleep, Payload: []byte(`{}`)}); err != nil {
				rt.Fatal(err)
			}
		}

		var mu sync.Mutex
		claimedBy := map[[2]int64]string{}
		var errs []error
		fail := func(err error) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
		reapCtx, stopReaper := context.WithCancel(ctx)
		reaperDone := make(chan struct{})
		go func() {
			defer close(reaperDone)
			for reapCtx.Err() == nil {
				if _, err := store.Reap(reapCtx, env.Pool, 500); err != nil && reapCtx.Err() == nil {
					fail(fmt.Errorf("reap: %w", err))
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		start := make(chan struct{})
		var wg sync.WaitGroup
		for c, script := range scripts {
			wg.Go(func() {
				<-start
				for r, s := range script {
					owners := make([]string, s.batch)
					for k := range owners {
						owners[k] = fmt.Sprintf("claimer-%d/round-%d/%d", c, r, k)
					}
					got, err := store.Claim(ctx, env.Pool, owners)
					if err != nil {
						fail(fmt.Errorf("claim: %w", err))
						return
					}
					for k, job := range got {
						mu.Lock()
						key := [2]int64{job.ID, int64(job.ClaimSeq)}
						if prev, dup := claimedBy[key]; dup {
							errs = append(errs, fmt.Errorf("job %d claim_seq %d handed to %s and %s", job.ID, job.ClaimSeq, prev, job.Owner))
						}
						claimedBy[key] = job.Owner
						mu.Unlock()
						switch s.actions[k] {
						case "complete":
							_, err = store.Complete(ctx, env.Pool, job.Fence())
						case "fail":
							_, err = store.Fail(ctx, env.Pool, job.Fence(), "retry now", 0)
						}
						if err != nil {
							fail(err)
						}
					}
					time.Sleep(time.Duration(s.pauseMS) * time.Millisecond)
				}
			})
		}
		close(start)
		wg.Wait()
		stopReaper()
		<-reaperDone
		for _, err := range errs {
			rt.Error(err)
		}

		records := readLeaseLog(rt, env)
		if len(records) != len(claimedBy) {
			rt.Errorf("lease log has %d leases, claimers received %d", len(records), len(claimedBy))
		}
		for i, rec := range records {
			if owner, ok := claimedBy[[2]int64{rec.jobID, int64(rec.seq)}]; !ok || owner != rec.owner {
				rt.Errorf("job %d claim_seq %d: logged owner %s, claimer saw %q", rec.jobID, rec.seq, rec.owner, owner)
			}
			if i == 0 || records[i-1].jobID != rec.jobID {
				if rec.seq != 1 {
					rt.Errorf("job %d: first logged claim_seq is %d, want 1", rec.jobID, rec.seq)
				}
				continue
			}
			prev := records[i-1]
			if rec.seq != prev.seq+1 {
				rt.Errorf("job %d: claim_seq jumped from %d to %d", rec.jobID, prev.seq, rec.seq)
			}
			if prev.ended == nil {
				rt.Errorf("job %d: lease %d (%s) never ended, but lease %d (%s) started at %v",
					rec.jobID, prev.seq, prev.owner, rec.seq, rec.owner, rec.started)
			} else if prev.ended.After(rec.started) {
				rt.Errorf("job %d: lease %d (%s) ended at %v, after lease %d (%s) started at %v",
					rec.jobID, prev.seq, prev.owner, *prev.ended, rec.seq, rec.owner, rec.started)
			}
		}
		var running int
		if err := env.Pool.QueryRow(ctx, `
			SELECT count(*) FROM jobs j JOIN lease_log l ON l.job_id = j.id AND l.claim_seq = j.claim_seq
			 WHERE j.state = 'running' AND (l.ended_at IS NOT NULL OR l.owner <> j.lease_owner)`).Scan(&running); err != nil {
			rt.Fatal(err)
		}
		if running != 0 {
			rt.Errorf("%d running jobs disagree with their current lease", running)
		}
	})
}

func readLeaseLog(rt *rapid.T, env *queuetest.Env) []leaseRecord {
	rows, err := env.Pool.Query(context.Background(),
		`SELECT job_id, claim_seq, owner, started_at, ended_at FROM lease_log ORDER BY job_id, claim_seq`)
	if err != nil {
		rt.Fatal(err)
	}
	defer rows.Close()
	var out []leaseRecord
	for rows.Next() {
		var r leaseRecord
		if err := rows.Scan(&r.jobID, &r.seq, &r.owner, &r.started, &r.ended); err != nil {
			rt.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		rt.Fatal(err)
	}
	return out
}
