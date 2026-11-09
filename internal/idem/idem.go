// Package idem is the idempotency helper (design §10, D-017). The queue is
// at-least-once: a job can run more than once. This helper makes those repeated
// runs harmless; it does not stop them.
//
//   - Effects written through run.Tx() commit only with the fenced completion,
//     so a stale run's effects roll back and need nothing more (AC-7.2).
//   - A step that commits on its own records Once(job, step) in the same
//     transaction, so a later run skips a step that already committed (AC-7.1).
//     queue.Run.Step does this.
//   - An external call sends Key(job, step) as its Idempotency-Key header, which
//     is the same on every run of the job, so a server that honours the header
//     applies the call once (AC-7.3).
package idem

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// stepPattern is what a step name may look like: a short ASCII name that is
// also safe inside an HTTP header value.
var stepPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$`)

// ErrBadStep means a step name is empty, too long, or has characters other
// than letters, digits and . _ : -.
var ErrBadStep = errors.New("idem: a step name is 1 to 100 characters: letters, digits and . _ : - (not first)")

// ValidStep reports whether step is a usable step name.
func ValidStep(step string) error {
	if !stepPattern.MatchString(step) {
		return fmt.Errorf("%w: %q", ErrBadStep, step)
	}
	return nil
}

// Once records (jobID, step) in tx. It returns true if this is the first time,
// and false if a committed transaction already recorded it. If another open
// transaction is recording the same step, Once waits for it to finish: false if
// it committed, true if it rolled back. So of two overlapping runs of a job,
// only one can commit a step's effect (AC-7.1).
func Once(ctx context.Context, tx pgx.Tx, jobID int64, step string) (first bool, err error) {
	if err := ValidStep(step); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (job_id, step) VALUES ($1, $2)
		ON CONFLICT (job_id, step) DO NOTHING`, jobID, step)
	if err != nil {
		return false, fmt.Errorf("idem: record step %q of job %d: %w", step, jobID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Key returns the Idempotency-Key header value for an external call made by
// this job's step: "job-<id>-<step>". It is identical on every run of the
// same job (AC-7.3). The step must be a valid step name (ValidStep).
func Key(jobID int64, step string) string {
	return "job-" + strconv.FormatInt(jobID, 10) + "-" + step
}
