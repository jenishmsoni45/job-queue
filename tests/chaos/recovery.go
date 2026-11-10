package chaos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// The crash-recovery scenario's workload (NFR-10, P3-6, design §18).
const (
	// RecoveryKind uses the default 30 s lease and 10 s heartbeat in
	// tests/chaos/shops.json (D-003).
	RecoveryKind = "test.sleep"
	// RecoveryJobs fit in one worker's 8 slots, so whichever worker survives
	// has a free slot for every orphan, and the measurement has no queueing in it.
	RecoveryJobs = 8
	// RecoverySleepMS keeps the jobs running well past the kill.
	RecoverySleepMS = 15_000
)

// RecoveryResult is the outcome of the crash-recovery scenario. It is written
// to bench/results/chaos/.
type RecoveryResult struct {
	Started      time.Time `json:"started"`
	LeaseMS      int64     `json:"lease_ms"`
	ReaperScanMS int64     `json:"reaper_scan_ms"`
	// TargetMS is NFR-10's target: one lease plus one reaper scan.
	TargetMS int64 `json:"target_ms"`
	// Killed is the worker that was killed with SIGKILL, and Orphans the jobs
	// it held.
	Killed  string `json:"killed"`
	Orphans int    `json:"orphans"`
	// RecoveryMS is the time from the kill to the last orphan's new claim, by
	// the database clock. The kill time is read just before the kill is sent,
	// so the measurement can only err on the long side.
	RecoveryMS int64  `json:"recovery_ms"`
	Passed     bool   `json:"passed"`
	Error      string `json:"error,omitempty"`
}

// Recovery runs the crash-recovery scenario (NFR-10): with the default lease,
// it kills the worker holding the most jobs and measures the time from the
// kill to the last orphaned job's new claim.
func (e *Env) Recovery(ctx context.Context, leaseMS, reaperScanMS int64) RecoveryResult {
	res := RecoveryResult{Started: time.Now().UTC(), LeaseMS: leaseMS, ReaperScanMS: reaperScanMS, TargetMS: leaseMS + reaperScanMS}
	if err := e.recovery(ctx, &res); err != nil {
		res.Error = err.Error()
	}
	res.Passed = res.Error == "" && res.RecoveryMS <= res.TargetMS
	return res
}

func (e *Env) recovery(ctx context.Context, res *RecoveryResult) error {
	if err := e.Restore(ctx); err != nil {
		return err
	}
	if err := e.ResetDB(ctx); err != nil {
		return err
	}
	for i := range RecoveryJobs {
		job := PlannedJob{Key: fmt.Sprintf("recovery-%d", i), Shop: e.Shops[i%len(e.Shops)], SleepMS: RecoverySleepMS}
		if _, _, _, err := e.enqueue(ctx, job, RecoveryKind); err != nil {
			return err
		}
	}

	// Wait until every job is running, then pick the worker holding the most.
	hosts := map[string]string{}
	for _, w := range Workers {
		c, err := e.Docker.Inspect(ctx, w)
		if err != nil {
			return err
		}
		hosts[c.Hostname] = w
	}
	held := map[string]int{}
	if err := poll(ctx, 60*time.Second, "every recovery job to be running", func() (bool, error) {
		clear(held)
		rows, err := e.DB.Query(ctx, `
			SELECT split_part(lease_owner, '/', 1), count(*) FROM jobs WHERE state = 'running' GROUP BY 1`)
		if err != nil {
			return false, err
		}
		total := 0
		for rows.Next() {
			var host string
			var n int
			if err := rows.Scan(&host, &n); err != nil {
				return false, err
			}
			held[host] = n
			total += n
		}
		return total == RecoveryJobs, rows.Err()
	}); err != nil {
		return err
	}
	var victimHost string
	for host, n := range held {
		if victimHost == "" || n > held[victimHost] || (n == held[victimHost] && host < victimHost) {
			victimHost = host
		}
	}
	victim, ok := hosts[victimHost]
	if !ok {
		return fmt.Errorf("chaos: running jobs are held by %q, which is not a worker: %v", victimHost, held)
	}
	res.Killed = victim

	var killedAt time.Time
	if err := e.DB.QueryRow(ctx, `SELECT statement_timestamp()`).Scan(&killedAt); err != nil {
		return err
	}
	if err := e.Docker.Kill(ctx, victim); err != nil {
		return err
	}
	defer func() {
		startCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = e.Docker.Start(startCtx, victim)
	}()

	// The killed process can't write any more, so the jobs it held are fixed now.
	rows, err := e.DB.Query(ctx, `
		SELECT id, claim_seq FROM jobs WHERE state = 'running' AND split_part(lease_owner, '/', 1) = $1`, victimHost)
	if err != nil {
		return err
	}
	type orphan struct {
		id  int64
		seq int32
	}
	orphans, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (orphan, error) {
		var o orphan
		err := row.Scan(&o.id, &o.seq)
		return o, err
	})
	if err != nil {
		return err
	}
	res.Orphans = len(orphans)
	if len(orphans) == 0 {
		return errors.New("chaos: the killed worker held no jobs")
	}
	ids := make([]int64, len(orphans))
	seqs := make([]int32, len(orphans))
	for i, o := range orphans {
		ids[i], seqs[i] = o.id, o.seq
	}

	// Wait for every orphan's next lease, with room past the target to measure a miss.
	var reclaimedAt time.Time
	wait := time.Duration(res.TargetMS)*time.Millisecond + 30*time.Second
	if err := poll(ctx, wait, "every orphan to be claimed again", func() (bool, error) {
		var n int
		var last *time.Time
		err := e.DB.QueryRow(ctx, `
			SELECT count(*), max(l.started_at)
			  FROM unnest($1::bigint[], $2::int[]) AS o (id, seq)
			  JOIN lease_log l ON l.job_id = o.id AND l.claim_seq = o.seq + 1`, ids, seqs).Scan(&n, &last)
		if err != nil || n < len(orphans) {
			return false, err
		}
		reclaimedAt = *last
		return true, nil
	}); err != nil {
		return err
	}
	res.RecoveryMS = reclaimedAt.Sub(killedAt).Milliseconds()
	return nil
}

// poll calls cond every 100 ms until it reports true, an error, or timeout passes.
func poll(ctx context.Context, timeout time.Duration, what string, cond func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		done, err := cond()
		if err != nil {
			return fmt.Errorf("chaos: waiting for %s: %w", what, err)
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("chaos: timed out after %v waiting for %s", timeout, what)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
