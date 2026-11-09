package chaos

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/queuetest"
)

// Check is one check of a run's outcome.
type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

// The checks every run makes (design §18, P3-1, P3-3).
const (
	CheckQ1      = "q1_every_accepted_job_completed_or_dead"
	CheckQ4      = "q4_one_job_per_dedupe_key"
	CheckQ3      = "q3_no_overlapping_leases"
	CheckBacklog = "backlog_counter_matches_real_count"
	CheckEffects = "run_tx_effect_applied_once"
	CheckRouting = "workers_and_dispatcher_reach_postgres_through_toxiproxy"
	maxExamples  = 10
	queuedApps   = "'queued-worker', 'queued-dispatcher', 'queued-dispatcher-leader'"
)

// check runs every check after a run.
func (e *Env) check(ctx context.Context, plan Plan, ids []int64, counts JobCounts, samples Samples) ([]Check, JobCounts) {
	var checks []Check
	add := func(name string, detail string, err error) {
		if err != nil {
			checks = append(checks, Check{Name: name, Passed: false, Detail: err.Error()})
			return
		}
		checks = append(checks, Check{Name: name, Passed: true, Detail: detail})
	}

	q1, err := e.checkQ1(ctx, plan, ids, &counts)
	add(CheckQ1, q1, err)
	q4, err := e.checkQ4(ctx, plan, ids)
	add(CheckQ4, q4, err)
	q3, err := e.checkQ3(ctx, counts.NotTerminal == 0)
	add(CheckQ3, q3, err)
	bl, err := e.checkBacklog(ctx, samples)
	add(CheckBacklog, bl, err)
	ef, err := e.checkEffects(ctx)
	add(CheckEffects, ef, err)
	rt, err := e.checkRouting(ctx, samples)
	add(CheckRouting, rt, err)
	return checks, counts
}

// checkQ1: every planned key was accepted, and every accepted job is completed
// or dead (NFR-1). A job row that is missing altogether is a lost job too.
func (e *Env) checkQ1(ctx context.Context, plan Plan, ids []int64, counts *JobCounts) (string, error) {
	rows, err := e.DB.Query(ctx, `
		SELECT a.id, coalesce(j.state, 'missing')
		  FROM unnest($1::bigint[]) AS a (id) LEFT JOIN jobs j ON j.id = a.id`, accepted(ids))
	if err != nil {
		return "", err
	}
	type idState struct {
		id    int64
		state string
	}
	states, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (idState, error) {
		var s idState
		err := row.Scan(&s.id, &s.state)
		return s, err
	})
	if err != nil {
		return "", err
	}
	counts.Completed, counts.Dead, counts.NotTerminal = 0, 0, 0
	var bad []string
	for _, s := range states {
		switch s.state {
		case "completed":
			counts.Completed++
		case "dead":
			counts.Dead++
		default:
			counts.NotTerminal++
			if len(bad) < maxExamples {
				bad = append(bad, fmt.Sprintf("job %d is %s", s.id, s.state))
			}
		}
	}
	var errs []error
	if counts.Accepted != len(plan.Jobs) {
		errs = append(errs, fmt.Errorf("%d of %d keys were accepted", counts.Accepted, len(plan.Jobs)))
	}
	if counts.NotTerminal > 0 {
		errs = append(errs, fmt.Errorf("%d accepted jobs are not completed or dead: %s", counts.NotTerminal, strings.Join(bad, "; ")))
	}
	detail := fmt.Sprintf("%d accepted: %d completed, %d dead", counts.Accepted, counts.Completed, counts.Dead)
	return detail, errors.Join(errs...)
}

// checkQ4: each dedupe key maps to exactly one job, and no other job exists,
// although enqueues were retried through api errors and Postgres restarts.
func (e *Env) checkQ4(ctx context.Context, plan Plan, ids []int64) (string, error) {
	want := map[string]int64{}
	byID := map[int64]string{}
	var errs []error
	for i, id := range ids {
		if id == 0 {
			continue
		}
		key := plan.Jobs[i].Key
		if other, dup := byID[id]; dup {
			errs = append(errs, fmt.Errorf("keys %s and %s both got job %d", other, key, id))
		}
		byID[id] = key
		want[key] = id
	}
	var jobs int
	if err := e.DB.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&jobs); err != nil {
		return "", err
	}
	if jobs != len(want) {
		errs = append(errs, fmt.Errorf("%d jobs exist for %d accepted keys", jobs, len(want)))
	}
	rows, err := e.DB.Query(ctx, `SELECT key, job_id FROM dedupe_keys`)
	if err != nil {
		return "", err
	}
	type keyJob struct {
		key string
		id  int64
	}
	keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (keyJob, error) {
		var k keyJob
		err := row.Scan(&k.key, &k.id)
		return k, err
	})
	if err != nil {
		return "", err
	}
	if len(keys) != len(want) {
		errs = append(errs, fmt.Errorf("%d dedupe keys stored for %d accepted keys", len(keys), len(want)))
	}
	wrong := 0
	for _, k := range keys {
		if want[k.key] != k.id {
			wrong++
			if wrong <= maxExamples {
				errs = append(errs, fmt.Errorf("key %s points to job %d, but the api returned %d", k.key, k.id, want[k.key]))
			}
		}
	}
	return fmt.Sprintf("%d keys, %d jobs", len(keys), jobs), errors.Join(errs...)
}

// checkQ3: no job ever had two overlapping leases, by the database clock, and
// every job's claim_seq matches its last recorded lease.
func (e *Env) checkQ3(ctx context.Context, allTerminal bool) (string, error) {
	records, err := queuetest.ReadLeaseLog(ctx, e.DB)
	if err != nil {
		return "", err
	}
	errs := queuetest.LeaseLogErrors(records, allTerminal)
	var mismatched int
	if err := e.DB.QueryRow(ctx, `
		SELECT count(*) FROM jobs j
		 WHERE j.claim_seq <> (SELECT coalesce(max(l.claim_seq), 0) FROM lease_log l WHERE l.job_id = j.id)`,
	).Scan(&mismatched); err != nil {
		return "", err
	}
	if mismatched > 0 {
		errs = append(errs, fmt.Errorf("%d jobs have a claim_seq that differs from their last recorded lease", mismatched))
	}
	if len(errs) > maxExamples {
		errs = append(errs[:maxExamples], fmt.Errorf("and %d more", len(errs)-maxExamples))
	}
	return fmt.Sprintf("%d leases checked", len(records)), errors.Join(errs...)
}

// checkBacklog: the backlog counter matched a real count in every sample taken
// during the run, and matches it now.
func (e *Env) checkBacklog(ctx context.Context, samples Samples) (string, error) {
	mismatches, err := e.backlogMismatches(ctx)
	if err != nil {
		return "", err
	}
	var errs []error
	if len(mismatches) > 0 {
		errs = append(errs, fmt.Errorf("at the end: %s", strings.Join(mismatches, "; ")))
	}
	if samples.Mismatched > 0 {
		errs = append(errs, fmt.Errorf("%d of %d samples during the run mismatched", samples.Mismatched, samples.Taken))
	}
	if samples.Taken == 0 {
		errs = append(errs, errors.New("no sample was taken during the run"))
	}
	return fmt.Sprintf("%d samples during the run and one at the end matched", samples.Taken), errors.Join(errs...)
}

// checkEffects: each completed job has exactly one test_effects row, written
// by the run that completed it (its final claim_seq), and no other job has
// one (P3-3, AC-7.2).
func (e *Env) checkEffects(ctx context.Context) (string, error) {
	var wrongCount, wrongRun, orphans, total int
	if err := e.DB.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM jobs j
		    WHERE (SELECT count(*) FROM test_effects t WHERE t.job_id = j.id)
		          <> CASE WHEN j.state = 'completed' THEN 1 ELSE 0 END),
		  (SELECT count(*) FROM jobs j
		    WHERE j.state = 'completed'
		      AND NOT EXISTS (SELECT 1 FROM test_effects t WHERE t.job_id = j.id AND t.claim_seq = j.claim_seq)),
		  (SELECT count(*) FROM test_effects t WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = t.job_id)),
		  (SELECT count(*) FROM test_effects)`,
	).Scan(&wrongCount, &wrongRun, &orphans, &total); err != nil {
		return "", err
	}
	var errs []error
	if wrongCount > 0 {
		errs = append(errs, fmt.Errorf("%d jobs have a number of effects other than 1 if completed, 0 otherwise", wrongCount))
	}
	if wrongRun > 0 {
		errs = append(errs, fmt.Errorf("%d completed jobs have no effect from the run that completed them", wrongRun))
	}
	if orphans > 0 {
		errs = append(errs, fmt.Errorf("%d effects belong to no job", orphans))
	}
	return fmt.Sprintf("%d effects", total), errors.Join(errs...)
}

// checkRouting: every worker and dispatcher connection seen during the run,
// sampled every second, and at its end, came through Toxiproxy, and at least
// one was seen, so faults on the proxies really reached them (design §18).
func (e *Env) checkRouting(ctx context.Context, samples Samples) (string, error) {
	seen, bypasses := samples.Connections, slices.Clone(samples.Bypasses)
	if n, b, err := e.routing(ctx); err == nil {
		seen += n
		bypasses = append(bypasses, b...)
	}
	var errs []error
	if seen == 0 {
		errs = append(errs, errors.New("no worker or dispatcher connection was seen during the run"))
	}
	for _, b := range bypasses {
		errs = append(errs, fmt.Errorf("%s, not through toxiproxy (%v)", b, e.toxiAddrs))
	}
	return fmt.Sprintf("%d connections seen across the run's samples, all through toxiproxy", seen), errors.Join(errs...)
}

// accepted returns the non-zero IDs.
func accepted(ids []int64) []int64 {
	return slices.DeleteFunc(slices.Clone(ids), func(id int64) bool { return id == 0 })
}
