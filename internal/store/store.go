// Package store holds the Postgres queries for jobs and shop state (design §3,
// §5–§9): seeding from config, enqueue with dedupe and admission (single and
// batch), the claim transaction with its cap step, heartbeats, the fenced result
// writes, the reaper and the pruner.
//
// Every lease, claim and fencing time comes from the database clock, read with
// statement_timestamp() (D-036). now() is frozen at the start of a transaction,
// and the fenced completion can run in a transaction the handler opened long
// before, so it would judge an expired lease as live.
//
// Every write that moves a job into or out of queued/running updates the shop's
// backlog counter in the same statement or transaction (design §3).
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jenishmsoni45/job-queue/internal/admission"
	"github.com/jenishmsoni45/job-queue/internal/config"
)

// DB is what the queries need. *pgxpool.Pool, *pgx.Conn and pgx.Tx all satisfy it.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Job states (design §3).
const (
	StateQueued    = "queued"
	StateRunning   = "running"
	StateCompleted = "completed"
	StateDead      = "dead"
)

// Dead reasons (design §8, §9).
const (
	ReasonPermanent         = "permanent"
	ReasonAttemptsExhausted = "attempts exhausted"
)

// LastErrorMax is the most bytes of an error message stored in last_error.
const LastErrorMax = 4096

// EnqueueNotifyChannel is the channel enqueue notifies at commit, with the shop
// ID as payload. The dispatcher listens on it to claim without waiting for its
// next refresh (design §5.1, §6.1).
const EnqueueNotifyChannel = "jobs_enqueued"

var (
	// ErrUnknownShop means the shop is not in the shops table (AC-1.2).
	ErrUnknownShop = errors.New("unknown shop")
	// ErrUnknownKind means the job kind is not in the job_kinds table (AC-1.3).
	ErrUnknownKind = errors.New("unknown job kind")
	// ErrNotFound means no job has the ID.
	ErrNotFound = errors.New("job not found")
	// ErrInvalidPayload means Postgres refused the payload as jsonb, for
	// example a \u0000 escape, which is valid JSON but not storable (AC-1.3).
	ErrInvalidPayload = errors.New("invalid payload")
)

// Seed upserts shops, shop_state and job_kinds from the config file in one
// transaction (D-019). It never resets a shop's counters.
func Seed(ctx context.Context, db DB, cfg *config.File) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		for _, s := range cfg.Shops {
			if _, err := tx.Exec(ctx, `
				INSERT INTO shops (id, weight, max_concurrency, rate_per_sec, rate_burst, backlog_limit, backlog_retry_after_ms)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (id) DO UPDATE SET
				  weight = EXCLUDED.weight, max_concurrency = EXCLUDED.max_concurrency,
				  rate_per_sec = EXCLUDED.rate_per_sec, rate_burst = EXCLUDED.rate_burst,
				  backlog_limit = EXCLUDED.backlog_limit, backlog_retry_after_ms = EXCLUDED.backlog_retry_after_ms`,
				s.ID, s.Weight, s.MaxConcurrency, s.RatePerSec, s.RateBurst, s.BacklogLimit, s.BacklogRetryAfterMS,
			); err != nil {
				return fmt.Errorf("store: seed shop %s: %w", s.ID, err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO shop_state (shop_id) VALUES ($1) ON CONFLICT (shop_id) DO NOTHING`, s.ID,
			); err != nil {
				return fmt.Errorf("store: seed shop_state %s: %w", s.ID, err)
			}
		}
		for _, k := range cfg.Kinds {
			if _, err := tx.Exec(ctx, `
				INSERT INTO job_kinds (kind, cost, lease_ms, heartbeat_ms, timeout_ms, max_attempts, max_throttles)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (kind) DO UPDATE SET
				  cost = EXCLUDED.cost, lease_ms = EXCLUDED.lease_ms, heartbeat_ms = EXCLUDED.heartbeat_ms,
				  timeout_ms = EXCLUDED.timeout_ms, max_attempts = EXCLUDED.max_attempts,
				  max_throttles = EXCLUDED.max_throttles`,
				k.Kind, k.Cost, k.LeaseMS, k.HeartbeatMS, k.TimeoutMS, k.MaxAttempts, k.MaxThrottles,
			); err != nil {
				return fmt.Errorf("store: seed kind %s: %w", k.Kind, err)
			}
		}
		return nil
	})
}

// NewJob is one job to enqueue.
type NewJob struct {
	Shop    string
	Kind    string
	Payload json.RawMessage // a JSON object; the api role validates it
	RunAt   *time.Time      // nil means runnable at once
	// DedupeKey, if set, makes the enqueue idempotent for this shop within the
	// dedupe window (D-018): X-Shopify-Webhook-Id for a webhook, or a caller's key.
	DedupeKey string
}

// Enqueued is the outcome of an enqueue.
type Enqueued struct {
	ID int64
	// Duplicate means the shop already had the dedupe key: ID is the existing
	// job, and nothing was created (AC-1.4).
	Duplicate bool
}

// errKeyTaken means a concurrent enqueue committed the same dedupe key between
// this enqueue's check and its insert.
var errKeyTaken = errors.New("store: dedupe key taken by a concurrent enqueue")

// maxKeyRetries bounds how often an enqueue starts over because its dedupe key
// was taken and then vanished again (pruned) before it could be read.
const maxKeyRetries = 3

// Enqueue inserts one queued job and adds it to the shop's backlog, in one
// transaction (design §5.1). It notifies the dispatcher at commit. On any error
// nothing is created.
//
// If the job would take the shop past its backlog limit, nothing is created and
// the error is an *admission.Rejected (Q8). The shop's shed count goes up in the
// same transaction that found the shop full, so the rejection is counted exactly
// once and survives the error return.
//
// With a dedupe key, a key the shop already has returns that job as a
// duplicate, and creates nothing (D-018). Two enqueues with the same new key
// can both pass that check; the shop_state row lock then serializes them, and
// the second one's key insert conflicts. It rolls back and returns the first
// one's job (Q4).
func Enqueue(ctx context.Context, db DB, j NewJob) (Enqueued, error) {
	for range maxKeyRetries {
		res, err := enqueueOnce(ctx, db, j)
		if !errors.Is(err, errKeyTaken) {
			return res, err
		}
		id, found, err := lookupKey(ctx, db, j.Shop, j.DedupeKey)
		if err != nil {
			return Enqueued{}, err
		}
		if found {
			return Enqueued{ID: id, Duplicate: true}, nil
		}
	}
	return Enqueued{}, fmt.Errorf("store: enqueue: dedupe key %q kept changing", j.DedupeKey)
}

func enqueueOnce(ctx context.Context, db DB, j NewJob) (res Enqueued, err error) {
	var rej *admission.Rejected
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var cost int32
		if err := tx.QueryRow(ctx, `SELECT cost FROM job_kinds WHERE kind = $1`, j.Kind).Scan(&cost); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrUnknownKind
			}
			return fmt.Errorf("store: read job kind: %w", err)
		}
		// Dedupe runs before the shop lock and admission, so a duplicate never
		// waits on the hot row and gets 200 even at the backlog limit (D-018).
		if j.DedupeKey != "" {
			id, found, err := lookupKey(ctx, tx, j.Shop, j.DedupeKey)
			if err != nil {
				return err
			}
			if found {
				res = Enqueued{ID: id, Duplicate: true}
				return nil
			}
		}
		limits, err := lockShop(ctx, tx, j.Shop)
		if err != nil {
			return err
		}
		if !admission.Fits(limits.backlog, limits.limit, 1) {
			rej, err = shed(ctx, tx, j.Shop, 1, limits)
			return err // nil: commit the shed count, then report the rejection
		}
		// enqueued_at defaults to now(), the transaction's start, and run_at and
		// first_runnable_at use the same instant, so a job enqueued without run_at
		// has run_at = first_runnable_at = enqueued_at (D-011).
		var id int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO jobs (shop_id, kind, cost, payload, state, run_at, first_runnable_at)
			VALUES ($1, $2, $3, $4, 'queued', coalesce($5, now()), greatest(now(), coalesce($5, now())))
			RETURNING id`,
			j.Shop, j.Kind, cost, j.Payload, j.RunAt,
		).Scan(&id); err != nil {
			if isDataException(err) {
				return fmt.Errorf("%w: %w", ErrInvalidPayload, err)
			}
			return fmt.Errorf("store: insert job: %w", err)
		}
		if j.DedupeKey != "" {
			// The shop_state lock means any other enqueue of this key has committed
			// or rolled back by now, so DO NOTHING sees a committed key.
			tag, err := tx.Exec(ctx, `
				INSERT INTO dedupe_keys (shop_id, key, job_id) VALUES ($1, $2, $3)
				ON CONFLICT (shop_id, key) DO NOTHING`, j.Shop, j.DedupeKey, id)
			if err != nil {
				if isDataException(err) {
					return fmt.Errorf("%w: dedupe key: %w", ErrInvalidPayload, err)
				}
				return fmt.Errorf("store: insert dedupe key: %w", err)
			}
			if tag.RowsAffected() == 0 {
				return errKeyTaken // rolls back the job
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE shop_state SET backlog = backlog + 1 WHERE shop_id = $1`, j.Shop); err != nil {
			return fmt.Errorf("store: count backlog: %w", err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, EnqueueNotifyChannel, j.Shop); err != nil {
			return fmt.Errorf("store: notify: %w", err)
		}
		res = Enqueued{ID: id}
		return nil
	})
	if err != nil {
		return Enqueued{}, err
	}
	if rej != nil {
		return Enqueued{}, rej
	}
	return res, nil
}

// shopLimits is a shop's backlog and the limit on it, read under the shop's row lock.
type shopLimits struct {
	backlog    int
	limit      int
	retryAfter time.Duration
}

// lockShop takes the shop's shop_state row lock, which serializes the shop's
// enqueues (design §5.1), and reads its backlog and limit. A shop that isn't in
// the table is ErrUnknownShop.
func lockShop(ctx context.Context, tx pgx.Tx, shop string) (shopLimits, error) {
	var backlog, limit, retryMS int32
	err := tx.QueryRow(ctx, `
		SELECT st.backlog, sh.backlog_limit, sh.backlog_retry_after_ms
		  FROM shop_state st JOIN shops sh ON sh.id = st.shop_id
		 WHERE st.shop_id = $1
		   FOR UPDATE OF st`, shop).Scan(&backlog, &limit, &retryMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return shopLimits{}, ErrUnknownShop
	}
	if err != nil {
		return shopLimits{}, fmt.Errorf("store: lock shop_state: %w", err)
	}
	return shopLimits{
		backlog: int(backlog), limit: int(limit),
		retryAfter: time.Duration(retryMS) * time.Millisecond,
	}, nil
}

// shed counts n rejected jobs against the shop, in the transaction that holds
// its lock and found it full (design §5.1, AC-11.3). The caller commits, so the
// count is kept, and returns the rejection.
func shed(ctx context.Context, tx pgx.Tx, shop string, n int, l shopLimits) (*admission.Rejected, error) {
	if _, err := tx.Exec(ctx, `UPDATE shop_state SET shed_total = shed_total + $2 WHERE shop_id = $1`, shop, n); err != nil {
		return nil, fmt.Errorf("store: count shed jobs: %w", err)
	}
	return &admission.Rejected{Shop: shop, Shed: n, RetryAfter: l.retryAfter}, nil
}

// lookupKey returns the job the shop's dedupe key points to, if the key exists.
func lookupKey(ctx context.Context, db DB, shop, key string) (id int64, found bool, err error) {
	err = db.QueryRow(ctx, `SELECT job_id FROM dedupe_keys WHERE shop_id = $1 AND key = $2`, shop, key).Scan(&id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, false, nil
	case err != nil:
		if isDataException(err) {
			return 0, false, fmt.Errorf("%w: dedupe key: %w", ErrInvalidPayload, err)
		}
		return 0, false, fmt.Errorf("store: read dedupe key: %w", err)
	}
	return id, true, nil
}

// Job is one row of the jobs table.
type Job struct {
	ID              int64           `db:"id" json:"id"`
	Shop            string          `db:"shop_id" json:"shop"`
	Kind            string          `db:"kind" json:"kind"`
	Cost            int32           `db:"cost" json:"cost"`
	Payload         json.RawMessage `db:"payload" json:"payload"`
	State           string          `db:"state" json:"state"`
	RunAt           time.Time       `db:"run_at" json:"run_at"`
	EnqueuedAt      time.Time       `db:"enqueued_at" json:"enqueued_at"`
	FirstRunnableAt time.Time       `db:"first_runnable_at" json:"first_runnable_at"`
	FirstClaimedAt  *time.Time      `db:"first_claimed_at" json:"first_claimed_at"`
	ClaimSeq        int32           `db:"claim_seq" json:"claim_seq"`
	Attempts        int32           `db:"attempts" json:"attempts"`
	Throttles       int32           `db:"throttles" json:"throttles"`
	LeaseOwner      *string         `db:"lease_owner" json:"lease_owner"`
	LeaseExpiresAt  *time.Time      `db:"lease_expires_at" json:"lease_expires_at"`
	CompletedAt     *time.Time      `db:"completed_at" json:"completed_at"`
	DeadAt          *time.Time      `db:"dead_at" json:"dead_at"`
	DeadReason      *string         `db:"dead_reason" json:"dead_reason"`
	LastError       *string         `db:"last_error" json:"last_error"`
	DiscardedAt     *time.Time      `db:"discarded_at" json:"discarded_at"`
}

const jobColumns = `id, shop_id, kind, cost, payload, state, run_at, enqueued_at, first_runnable_at,
	first_claimed_at, claim_seq, attempts, throttles, lease_owner, lease_expires_at, completed_at,
	dead_at, dead_reason, last_error, discarded_at`

// GetJob reads one job. It returns ErrNotFound if no job has the ID.
func GetJob(ctx context.Context, db DB, id int64) (Job, error) {
	rows, err := db.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	if err != nil {
		return Job{}, fmt.Errorf("store: get job: %w", err)
	}
	j, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[Job])
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("store: get job: %w", err)
	}
	return j, nil
}

// Claimed is a job just claimed for one owner, with the lease settings of its kind.
type Claimed struct {
	ID             int64           `json:"id"`
	Shop           string          `json:"shop"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	ClaimSeq       int32           `json:"claim_seq"`
	Attempts       int32           `json:"attempts"`
	Owner          string          `json:"owner"`
	LeaseMS        int32           `json:"lease_ms"`
	HeartbeatMS    int32           `json:"heartbeat_ms"`
	TimeoutMS      int32           `json:"timeout_ms"`
	ClaimedAt      time.Time       `json:"claimed_at"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
}

// Fence identifies one run of a job: the claim it holds (D-004).
func (c Claimed) Fence() Fence { return Fence{JobID: c.ID, ClaimSeq: c.ClaimSeq, Owner: c.Owner} }

// Fence identifies the claim a result is written under (D-004). A fenced write
// applies only while the job is running with this claim_seq and owner and its
// lease has not expired by the database clock (Q2).
type Fence struct {
	JobID    int64
	ClaimSeq int32
	Owner    string
}

// fenced is the predicate every completion, failure and reschedule ends with
// (design §7). $1, $2 and $3 are the fence. Its columns are unqualified, which
// is unambiguous because job_kinds, the only table joined to jobs, has none of them.
const fenced = `id = $1 AND claim_seq = $2 AND lease_owner = $3
	AND state = 'running' AND lease_expires_at > statement_timestamp()`

// Complete marks the job completed and takes it off the backlog, if the fence
// holds. It returns false, having changed nothing, if the lease is lost. Run it
// in the handler's run.Tx() transaction so the handler's effects commit with it.
func Complete(ctx context.Context, db DB, f Fence) (bool, error) {
	var n int
	err := db.QueryRow(ctx, `
		WITH done AS (
		  UPDATE jobs SET state = 'completed', completed_at = statement_timestamp(),
		                  lease_owner = NULL, lease_expires_at = NULL
		   WHERE `+fenced+`
		  RETURNING shop_id),
		counted AS (
		  UPDATE shop_state s SET backlog = s.backlog - 1 FROM done WHERE s.shop_id = done.shop_id)
		SELECT count(*) FROM done`,
		f.JobID, f.ClaimSeq, f.Owner,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("store: complete job %d: %w", f.JobID, err)
	}
	return n == 1, nil
}

// FailResult is the outcome of a fenced failure write.
type FailResult struct {
	Applied bool   // false: the lease was lost and nothing changed
	State   string // queued (will retry) or dead (attempts exhausted)
}

// Fail records a temporary failure, if the fence holds (design §8): attempts + 1,
// then dead with "attempts exhausted" at the kind's max, or queued again at
// statement_timestamp() + backoff. A job that goes dead leaves the backlog.
func Fail(ctx context.Context, db DB, f Fence, msg string, backoff time.Duration) (FailResult, error) {
	var state string
	err := db.QueryRow(ctx, `
		WITH failed AS (
		  UPDATE jobs j
		     SET attempts = j.attempts + 1, last_error = $4, lease_owner = NULL, lease_expires_at = NULL,
		         state       = CASE WHEN j.attempts + 1 >= k.max_attempts THEN 'dead' ELSE 'queued' END,
		         run_at      = CASE WHEN j.attempts + 1 >= k.max_attempts THEN j.run_at
		                            ELSE statement_timestamp() + $5 * interval '1 millisecond' END,
		         dead_at     = CASE WHEN j.attempts + 1 >= k.max_attempts THEN statement_timestamp() END,
		         dead_reason = CASE WHEN j.attempts + 1 >= k.max_attempts THEN $6 END
		    FROM job_kinds k
		   WHERE k.kind = j.kind AND `+fenced+`
		  RETURNING j.shop_id, j.state),
		counted AS (
		  UPDATE shop_state s SET backlog = s.backlog - 1
		    FROM failed WHERE s.shop_id = failed.shop_id AND failed.state = 'dead')
		SELECT state FROM failed`,
		f.JobID, f.ClaimSeq, f.Owner, CleanError(msg), backoff.Milliseconds(), ReasonAttemptsExhausted,
	).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return FailResult{}, nil
	}
	if err != nil {
		return FailResult{}, fmt.Errorf("store: fail job %d: %w", f.JobID, err)
	}
	return FailResult{Applied: true, State: state}, nil
}

// Permanent sends the job straight to dead with reason "permanent", if the fence
// holds (AC-5.3). It returns false, having changed nothing, if the lease is lost.
func Permanent(ctx context.Context, db DB, f Fence, msg string) (bool, error) {
	var n int
	err := db.QueryRow(ctx, `
		WITH dead AS (
		  UPDATE jobs SET state = 'dead', dead_at = statement_timestamp(), dead_reason = $5,
		                  last_error = $4, lease_owner = NULL, lease_expires_at = NULL
		   WHERE `+fenced+`
		  RETURNING shop_id),
		counted AS (
		  UPDATE shop_state s SET backlog = s.backlog - 1 FROM dead WHERE s.shop_id = dead.shop_id)
		SELECT count(*) FROM dead`,
		f.JobID, f.ClaimSeq, f.Owner, CleanError(msg), ReasonPermanent,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("store: permanent failure of job %d: %w", f.JobID, err)
	}
	return n == 1, nil
}

// Heartbeat extends every listed lease that is still live, in one statement
// (design §7), and returns the IDs it extended. A job missing from the result
// has lost its lease. An expired lease is never revived (Q2).
func Heartbeat(ctx context.Context, db DB, leases []Fence) ([]int64, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	ids := make([]int64, len(leases))
	seqs := make([]int32, len(leases))
	owners := make([]string, len(leases))
	for i, l := range leases {
		ids[i], seqs[i], owners[i] = l.JobID, l.ClaimSeq, l.Owner
	}
	rows, err := db.Query(ctx, `
		UPDATE jobs j SET lease_expires_at = statement_timestamp() + k.lease_ms * interval '1 millisecond'
		  FROM job_kinds k, unnest($1::bigint[], $2::int[], $3::text[]) AS h (id, seq, owner)
		 WHERE k.kind = j.kind
		   AND j.id = h.id AND j.claim_seq = h.seq AND j.lease_owner = h.owner
		   AND j.state = 'running' AND j.lease_expires_at > statement_timestamp()
		RETURNING j.id`, ids, seqs, owners)
	if err != nil {
		return nil, fmt.Errorf("store: heartbeat: %w", err)
	}
	alive, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("store: heartbeat: %w", err)
	}
	return alive, nil
}

// ReapResult counts what one reaper pass did.
type ReapResult struct {
	Requeued int // runnable again at once, with one more attempt (D-031)
	Dead     int // reached the kind's max attempts
}

// Reap requeues up to limit jobs whose lease has expired by the database clock
// (design §9). Each counts a failed attempt and is runnable at once (D-031), or
// goes dead with "attempts exhausted" at the kind's max, leaving the backlog.
// Rows a fenced write or heartbeat holds are skipped, not waited for. Like
// Claim, it locks FOR NO KEY UPDATE, so an open step of the job doesn't hide
// its expired lease (D-047).
func Reap(ctx context.Context, db DB, limit int) (ReapResult, error) {
	var r ReapResult
	err := db.QueryRow(ctx, `
		WITH expired AS (
		  SELECT id FROM jobs
		   WHERE state = 'running' AND lease_expires_at <= statement_timestamp()
		   ORDER BY lease_expires_at
		   LIMIT $1
		   FOR NO KEY UPDATE SKIP LOCKED),
		reaped AS (
		  UPDATE jobs j
		     SET attempts = j.attempts + 1, lease_owner = NULL, lease_expires_at = NULL,
		         last_error = 'lease expired', run_at = statement_timestamp(),
		         state       = CASE WHEN j.attempts + 1 >= k.max_attempts THEN 'dead' ELSE 'queued' END,
		         dead_at     = CASE WHEN j.attempts + 1 >= k.max_attempts THEN statement_timestamp() END,
		         dead_reason = CASE WHEN j.attempts + 1 >= k.max_attempts THEN $2 END
		    FROM expired e, job_kinds k
		   WHERE j.id = e.id AND k.kind = j.kind
		  RETURNING j.shop_id, j.state),
		dead_per_shop AS (
		  SELECT shop_id, count(*) AS n FROM reaped WHERE state = 'dead' GROUP BY shop_id),
		counted AS (
		  UPDATE shop_state s SET backlog = s.backlog - d.n FROM dead_per_shop d WHERE s.shop_id = d.shop_id)
		SELECT count(*) FILTER (WHERE state = 'queued'), count(*) FILTER (WHERE state = 'dead') FROM reaped`,
		limit, ReasonAttemptsExhausted,
	).Scan(&r.Requeued, &r.Dead)
	if err != nil {
		return ReapResult{}, fmt.Errorf("store: reap: %w", err)
	}
	return r, nil
}

// Retention says how long the pruner keeps finished rows (design §2, §9).
type Retention struct {
	Completed  time.Duration // completed jobs, counted from completed_at (D-015: 24 h)
	Discarded  time.Duration // discarded dead jobs, counted from discarded_at (D-015: 24 h)
	DedupeKeys time.Duration // dedupe keys, counted from created_at: the dedupe window (D-018: 7 days)
}

// Validate checks that every window is positive.
func (r Retention) Validate() error {
	if r.Completed <= 0 || r.Discarded <= 0 || r.DedupeKeys <= 0 {
		return errors.New("store: every retention window must be positive")
	}
	return nil
}

// PruneResult counts the rows one pruner pass deleted.
type PruneResult struct {
	Completed  int
	Discarded  int
	DedupeKeys int
}

// The pruner's deletes (design §9). Each takes at most $2 rows past the window
// $1 (in milliseconds) by the database clock, oldest first, and skips rows
// another transaction holds. A job's idempotency keys go with it through
// ON DELETE CASCADE.
const (
	pruneCompleted = `
		DELETE FROM jobs WHERE state = 'completed' AND id IN (
		  SELECT id FROM jobs
		   WHERE state = 'completed' AND completed_at < statement_timestamp() - $1 * interval '1 millisecond'
		   ORDER BY completed_at
		   LIMIT $2
		   FOR UPDATE SKIP LOCKED)`
	pruneDiscarded = `
		DELETE FROM jobs WHERE state = 'dead' AND id IN (
		  SELECT id FROM jobs
		   WHERE state = 'dead' AND discarded_at < statement_timestamp() - $1 * interval '1 millisecond'
		   ORDER BY discarded_at
		   LIMIT $2
		   FOR UPDATE SKIP LOCKED)`
	pruneDedupeKeys = `
		DELETE FROM dedupe_keys d USING (
		  SELECT shop_id, key FROM dedupe_keys
		   WHERE created_at < statement_timestamp() - $1 * interval '1 millisecond'
		   ORDER BY created_at
		   LIMIT $2
		   FOR UPDATE SKIP LOCKED) old
		 WHERE d.shop_id = old.shop_id AND d.key = old.key`
)

// Prune deletes completed jobs and discarded jobs older than their retention
// windows, and dedupe keys older than the dedupe window, in batches of at most
// batch rows, repeating each delete while a batch comes back full (design §9).
// Queued and running jobs, and dead jobs that aren't discarded, are never
// touched (P3-7, D-041).
func Prune(ctx context.Context, db DB, r Retention, batch int) (PruneResult, error) {
	if err := r.Validate(); err != nil {
		return PruneResult{}, err
	}
	if batch <= 0 {
		return PruneResult{}, errors.New("store: the prune batch must be positive")
	}
	var res PruneResult
	for _, step := range []struct {
		what   string
		sql    string
		window time.Duration
		count  *int
	}{
		{"completed jobs", pruneCompleted, r.Completed, &res.Completed},
		{"discarded jobs", pruneDiscarded, r.Discarded, &res.Discarded},
		{"dedupe keys", pruneDedupeKeys, r.DedupeKeys, &res.DedupeKeys},
	} {
		for {
			tag, err := db.Exec(ctx, step.sql, step.window.Milliseconds(), batch)
			if err != nil {
				return res, fmt.Errorf("store: prune %s: %w", step.what, err)
			}
			*step.count += int(tag.RowsAffected())
			if tag.RowsAffected() < int64(batch) {
				break
			}
		}
	}
	return res, nil
}

// isDataException reports whether Postgres rejected a value: SQLSTATE class 22
// (data exception, such as an unstorable jsonb string) or 54001 (a payload
// nested too deeply for the jsonb parser).
func isDataException(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "22") || pgErr.Code == "54001")
}

// CleanError makes an error message safe to store in last_error: valid UTF-8,
// no NUL bytes (Postgres text can't hold them), and at most LastErrorMax bytes,
// cut on a rune boundary.
func CleanError(msg string) string {
	msg = strings.ToValidUTF8(msg, "\uFFFD")
	msg = strings.ReplaceAll(msg, "\x00", "\uFFFD")
	if len(msg) <= LastErrorMax {
		return msg
	}
	cut := LastErrorMax
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut]
}
