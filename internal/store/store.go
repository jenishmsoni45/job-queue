// Package store holds the Postgres queries for jobs and shop state (design §3,
// §5–§9): seeding from config, enqueue, the claim transaction, heartbeats, the
// fenced result writes and the reaper.
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
}

// Enqueue inserts one queued job and adds it to the shop's backlog, in one
// transaction (design §5.1, without dedupe and admission, which arrive in phases
// 3 and 4). The shop_state row lock serializes the shop's enqueues, as admission
// will need. It notifies the dispatcher at commit. On any error nothing is created.
func Enqueue(ctx context.Context, db DB, j NewJob) (id int64, err error) {
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var backlog int32
		if err := tx.QueryRow(ctx,
			`SELECT backlog FROM shop_state WHERE shop_id = $1 FOR UPDATE`, j.Shop,
		).Scan(&backlog); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrUnknownShop
			}
			return fmt.Errorf("store: lock shop_state: %w", err)
		}
		var cost int32
		if err := tx.QueryRow(ctx, `SELECT cost FROM job_kinds WHERE kind = $1`, j.Kind).Scan(&cost); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrUnknownKind
			}
			return fmt.Errorf("store: read job kind: %w", err)
		}
		// enqueued_at defaults to now(), the transaction's start, and run_at and
		// first_runnable_at use the same instant, so a job enqueued without run_at
		// has run_at = first_runnable_at = enqueued_at (D-011).
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
		if _, err := tx.Exec(ctx, `UPDATE shop_state SET backlog = backlog + 1 WHERE shop_id = $1`, j.Shop); err != nil {
			return fmt.Errorf("store: count backlog: %w", err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, EnqueueNotifyChannel, j.Shop); err != nil {
			return fmt.Errorf("store: notify: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
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

// Claim claims up to len(owners) runnable jobs, oldest first by (run_at, id),
// and gives the i-th claimed job to owners[i] (design §6.3). This is the FIFO
// claim without the cap step, which phase 4 adds between the two statements.
//
// FOR UPDATE SKIP LOCKED means concurrent claimers never wait on each other and
// never take the same row, and the state check means only a queued job can be
// claimed, so a job has at most one live lease (Q3).
func Claim(ctx context.Context, db DB, owners []string) ([]Claimed, error) {
	if len(owners) == 0 {
		return nil, nil
	}
	var claimed []Claimed
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id FROM jobs
			 WHERE state = 'queued' AND run_at <= statement_timestamp()
			 ORDER BY run_at, id
			 LIMIT $1
			 FOR UPDATE SKIP LOCKED`, len(owners))
		if err != nil {
			return fmt.Errorf("store: claim candidates: %w", err)
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return fmt.Errorf("store: claim candidates: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}
		rows, err = tx.Query(ctx, `
			UPDATE jobs j
			   SET state = 'running', claim_seq = j.claim_seq + 1, lease_owner = c.owner,
			       lease_expires_at = statement_timestamp() + k.lease_ms * interval '1 millisecond',
			       first_claimed_at = coalesce(j.first_claimed_at, statement_timestamp())
			  FROM unnest($1::bigint[], $2::text[]) AS c (id, owner), job_kinds k
			 WHERE j.id = c.id AND k.kind = j.kind AND j.state = 'queued'
			RETURNING j.id, j.shop_id, j.kind, j.payload, j.claim_seq, j.attempts, j.lease_owner,
			          k.lease_ms, k.heartbeat_ms, k.timeout_ms, statement_timestamp(), j.lease_expires_at`,
			ids, owners[:len(ids)])
		if err != nil {
			return fmt.Errorf("store: claim: %w", err)
		}
		updated, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Claimed, error) {
			var c Claimed
			err := row.Scan(&c.ID, &c.Shop, &c.Kind, &c.Payload, &c.ClaimSeq, &c.Attempts, &c.Owner,
				&c.LeaseMS, &c.HeartbeatMS, &c.TimeoutMS, &c.ClaimedAt, &c.LeaseExpiresAt)
			return c, err
		})
		if err != nil {
			return fmt.Errorf("store: claim: %w", err)
		}
		byID := make(map[int64]Claimed, len(updated))
		for _, c := range updated {
			byID[c.ID] = c
		}
		if len(byID) != len(ids) {
			// Every candidate is locked by this transaction and was queued, so this
			// would mean a broken invariant. Roll back rather than hand out half a claim.
			return fmt.Errorf("store: claim updated %d of %d locked candidates", len(byID), len(ids))
		}
		claimed = claimed[:0]
		for _, id := range ids {
			claimed = append(claimed, byID[id])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

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
// Rows a fenced write or heartbeat holds are skipped, not waited for.
func Reap(ctx context.Context, db DB, limit int) (ReapResult, error) {
	var r ReapResult
	err := db.QueryRow(ctx, `
		WITH expired AS (
		  SELECT id FROM jobs
		   WHERE state = 'running' AND lease_expires_at <= statement_timestamp()
		   ORDER BY lease_expires_at
		   LIMIT $1
		   FOR UPDATE SKIP LOCKED),
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
