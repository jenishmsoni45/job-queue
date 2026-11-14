package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ThrottleNotifyChannel is the channel a throttle notifies at commit, with the
// shop ID as payload, so the dispatcher refreshes its view of the shop at once
// (design §6.1, §8).
const ThrottleNotifyChannel = "shop_throttled"

// ReasonThrottleBudget is the dead reason of a job throttled more often than
// its kind's throttle budget (AC-6.3).
const ReasonThrottleBudget = "throttle budget exhausted"

// ThrottleResult is the outcome of a fenced throttle write.
type ThrottleResult struct {
	Applied bool   // false: the lease was lost and nothing changed
	State   string // queued (will run again) or dead (throttle budget exhausted)
	// ThrottledUntil is the shop's pause after this throttle, by the database clock.
	ThrottledUntil time.Time
}

// Throttle records a Throttled(retryAfter) result, if the fence holds (design
// §8, D-004), in one transaction:
//
//   - throttles + 1, and attempts unchanged, so a throttle never uses up the
//     retry budget (Q7, AC-6.1);
//   - past the kind's max_throttles, the job goes dead with "throttle budget
//     exhausted" and leaves the backlog (AC-6.3); otherwise it is queued again
//     at statement_timestamp() + retryAfter + jitter;
//   - the shop's throttled_until becomes the later of its current value and
//     statement_timestamp() + retryAfter, so no claim that starts after this
//     commits can claim for the shop before then (AC-6.2);
//   - a notify on shop_throttled wakes the dispatcher at commit.
//
// The shop is paused whether the job is requeued or goes dead: the API's 429
// is about the shop, not the job.
func Throttle(ctx context.Context, db DB, f Fence, retryAfter, jitter time.Duration, msg string) (ThrottleResult, error) {
	retryAfter, jitter = max(0, retryAfter), max(0, jitter)
	var res ThrottleResult
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var shop string
		err := tx.QueryRow(ctx, `
			UPDATE jobs j
			   SET throttles = j.throttles + 1, last_error = $4, lease_owner = NULL, lease_expires_at = NULL,
			       state       = CASE WHEN j.throttles + 1 > k.max_throttles THEN 'dead' ELSE 'queued' END,
			       run_at      = CASE WHEN j.throttles + 1 > k.max_throttles THEN j.run_at
			                          ELSE statement_timestamp() + ($5::bigint + $6::bigint) * interval '1 microsecond' END,
			       dead_at     = CASE WHEN j.throttles + 1 > k.max_throttles THEN statement_timestamp() END,
			       dead_reason = CASE WHEN j.throttles + 1 > k.max_throttles THEN $7 END
			  FROM job_kinds k
			 WHERE k.kind = j.kind AND `+fenced+`
			RETURNING j.shop_id, j.state`,
			f.JobID, f.ClaimSeq, f.Owner, CleanError(msg), retryAfter.Microseconds(), jitter.Microseconds(), ReasonThrottleBudget,
		).Scan(&shop, &res.State)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // the lease is lost: change nothing
		}
		if err != nil {
			return fmt.Errorf("store: throttle job %d: %w", f.JobID, err)
		}
		res.Applied = true
		dead := 0
		if res.State == StateDead {
			dead = 1
		}
		if err := tx.QueryRow(ctx, `
			UPDATE shop_state
			   SET backlog = backlog - $2,
			       throttled_until = greatest(coalesce(throttled_until, statement_timestamp()),
			                                  statement_timestamp() + $3::bigint * interval '1 microsecond')
			 WHERE shop_id = $1
			RETURNING throttled_until`, shop, dead, retryAfter.Microseconds(),
		).Scan(&res.ThrottledUntil); err != nil {
			return fmt.Errorf("store: pause shop %s: %w", shop, err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, ThrottleNotifyChannel, shop); err != nil {
			return fmt.Errorf("store: notify: %w", err)
		}
		return nil
	})
	if err != nil {
		return ThrottleResult{}, err
	}
	return res, nil
}
