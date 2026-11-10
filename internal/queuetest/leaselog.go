package queuetest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/store"
)

// LeaseLogSQL installs a test-only trigger that records every lease with
// database times: when a claim starts it, and when it ends. A lease ends at the
// write that moved the job out of running, or at its expiry if that came first
// (a reaped lease ended when it expired, not when it was reaped). Q3 is checked
// from these records (design §13, §18). It is safe to run more than once.
const LeaseLogSQL = `
CREATE TABLE IF NOT EXISTS lease_log (
  job_id     bigint      NOT NULL,
  claim_seq  int         NOT NULL,
  owner      text        NOT NULL,
  started_at timestamptz NOT NULL,
  ended_at   timestamptz,
  PRIMARY KEY (job_id, claim_seq)       -- one owner per (job, claim_seq)
);
CREATE OR REPLACE FUNCTION log_lease() RETURNS trigger LANGUAGE plpgsql AS $$
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
DROP TRIGGER IF EXISTS log_lease ON jobs;
CREATE TRIGGER log_lease AFTER UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION log_lease();`

// LeaseRecord is one row of lease_log.
type LeaseRecord struct {
	JobID   int64
	Seq     int32
	Owner   string
	Started time.Time
	Ended   *time.Time
}

// ReadLeaseLog returns every lease_log row, ordered by job and claim_seq.
func ReadLeaseLog(ctx context.Context, db store.DB) ([]LeaseRecord, error) {
	rows, err := db.Query(ctx, `SELECT job_id, claim_seq, owner, started_at, ended_at FROM lease_log ORDER BY job_id, claim_seq`)
	if err != nil {
		return nil, fmt.Errorf("queuetest: read lease_log: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (LeaseRecord, error) {
		var r LeaseRecord
		err := row.Scan(&r.JobID, &r.Seq, &r.Owner, &r.Started, &r.Ended)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("queuetest: read lease_log: %w", err)
	}
	return out, nil
}

// LeaseLogErrors checks Q3 over records ordered by job and claim_seq: each
// job's claim_seq runs 1, 2, 3, ... with no gap, and every lease ended no later
// than the next one of the same job started, by the database clock. With
// allEnded set (every job is terminal), every lease must also have ended.
func LeaseLogErrors(records []LeaseRecord, allEnded bool) []error {
	var errs []error
	for i, rec := range records {
		last := i == len(records)-1 || records[i+1].JobID != rec.JobID
		if allEnded && last && rec.Ended == nil {
			errs = append(errs, fmt.Errorf("job %d: lease %d (%s) never ended, but every job is terminal", rec.JobID, rec.Seq, rec.Owner))
		}
		if i == 0 || records[i-1].JobID != rec.JobID {
			if rec.Seq != 1 {
				errs = append(errs, fmt.Errorf("job %d: first logged claim_seq is %d, want 1", rec.JobID, rec.Seq))
			}
			continue
		}
		prev := records[i-1]
		if rec.Seq != prev.Seq+1 {
			errs = append(errs, fmt.Errorf("job %d: claim_seq jumped from %d to %d", rec.JobID, prev.Seq, rec.Seq))
		}
		if prev.Ended == nil {
			errs = append(errs, fmt.Errorf("job %d: lease %d (%s) never ended, but lease %d (%s) started at %v",
				rec.JobID, prev.Seq, prev.Owner, rec.Seq, rec.Owner, rec.Started))
		} else if prev.Ended.After(rec.Started) {
			errs = append(errs, fmt.Errorf("job %d: lease %d (%s) ended at %v, after lease %d (%s) started at %v",
				rec.JobID, prev.Seq, prev.Owner, *prev.Ended, rec.Seq, rec.Owner, rec.Started))
		}
	}
	return errs
}
