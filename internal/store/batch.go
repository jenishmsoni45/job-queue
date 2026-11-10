package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/admission"
)

var (
	// ErrEmptyBatch means the batch has no jobs (D-048).
	ErrEmptyBatch = errors.New("batch has no jobs")
	// ErrMixedShops means the batch's jobs are not all for one shop (D-030).
	ErrMixedShops = errors.New("batch jobs are for more than one shop")
	// ErrDuplicateKeyInBatch means one dedupe key appears twice in the batch (AC-2.4).
	ErrDuplicateKeyInBatch = errors.New("a dedupe key appears twice in the batch")
)

// BatchResult is the outcome of a batch enqueue (D-048).
type BatchResult struct {
	// IDs has one ID per job of the batch, in order: the new job's, or the
	// existing job's for a job whose dedupe key already existed.
	IDs []int64
	// Duplicates are the IDs among IDs that already existed.
	Duplicates []int64
	// Created is how many jobs the batch created.
	Created int
}

// EnqueueBatch enqueues jobs, all for one shop, all or nothing (design §5.2,
// D-030). The jobs' dedupe keys must differ from each other.
//
// Jobs whose dedupe key the shop already has are reported with their existing
// IDs, create nothing and don't count toward the backlog limit (AC-2.3). The
// shop's row lock is taken once, and the new jobs are checked against the limit
// as a whole: if they don't fit, nothing is created and the error is an
// *admission.Rejected whose Shed is the number of new jobs (D-049).
//
// Two batches that carry the same new key can both pass the key check before
// the lock; the second one's key insert then conflicts, and it starts over and
// finds the first one's job, as a single enqueue does (Q4).
func EnqueueBatch(ctx context.Context, db DB, jobs []NewJob) (BatchResult, error) {
	if len(jobs) == 0 {
		return BatchResult{}, ErrEmptyBatch
	}
	keys := map[string]bool{}
	for _, j := range jobs {
		if j.Shop != jobs[0].Shop {
			return BatchResult{}, ErrMixedShops
		}
		if j.DedupeKey != "" {
			if keys[j.DedupeKey] {
				return BatchResult{}, ErrDuplicateKeyInBatch
			}
			keys[j.DedupeKey] = true
		}
	}
	for range maxKeyRetries {
		res, err := enqueueBatchOnce(ctx, db, jobs)
		if !errors.Is(err, errKeyTaken) {
			return res, err
		}
	}
	return BatchResult{}, errors.New("store: enqueue batch: dedupe keys kept changing")
}

func enqueueBatchOnce(ctx context.Context, db DB, jobs []NewJob) (res BatchResult, err error) {
	shop := jobs[0].Shop
	var rej *admission.Rejected
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		res = BatchResult{}
		costs, err := kindCosts(ctx, tx, jobs)
		if err != nil {
			return err
		}
		existing, err := existingKeys(ctx, tx, shop, jobs)
		if err != nil {
			return err
		}
		var fresh []int // positions of the jobs this batch creates
		for i, j := range jobs {
			if _, dup := existing[j.DedupeKey]; j.DedupeKey == "" || !dup {
				fresh = append(fresh, i)
			}
		}
		ids := make([]int64, len(jobs))
		if len(fresh) > 0 {
			limits, err := lockShop(ctx, tx, shop)
			if err != nil {
				return err
			}
			if !admission.Fits(limits.backlog, limits.limit, len(fresh)) {
				rej, err = shed(ctx, tx, shop, len(fresh), limits)
				return err // nil: commit the shed count, then report the rejection
			}
			if err := insertBatch(ctx, tx, jobs, fresh, costs, ids); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE shop_state SET backlog = backlog + $2 WHERE shop_id = $1`, shop, len(fresh)); err != nil {
				return fmt.Errorf("store: count backlog: %w", err)
			}
			if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, EnqueueNotifyChannel, shop); err != nil {
				return fmt.Errorf("store: notify: %w", err)
			}
		}
		for i, j := range jobs {
			if id, dup := existing[j.DedupeKey]; j.DedupeKey != "" && dup {
				ids[i] = id
				res.Duplicates = append(res.Duplicates, id)
			}
		}
		res.IDs, res.Created = ids, len(fresh)
		return nil
	})
	if err != nil {
		return BatchResult{}, err
	}
	if rej != nil {
		return BatchResult{}, rej
	}
	return res, nil
}

// kindCosts returns the DRR cost of each distinct kind in the batch, or
// ErrUnknownKind if one of them isn't a kind.
func kindCosts(ctx context.Context, tx pgx.Tx, jobs []NewJob) (map[string]int32, error) {
	kinds := map[string]struct{}{}
	for _, j := range jobs {
		kinds[j.Kind] = struct{}{}
	}
	names := make([]string, 0, len(kinds))
	for k := range kinds {
		names = append(names, k)
	}
	rows, err := tx.Query(ctx, `SELECT kind, cost FROM job_kinds WHERE kind = ANY($1)`, names)
	if err != nil {
		return nil, fmt.Errorf("store: read job kinds: %w", err)
	}
	costs := make(map[string]int32, len(names))
	for rows.Next() {
		var kind string
		var cost int32
		if err := rows.Scan(&kind, &cost); err != nil {
			return nil, fmt.Errorf("store: read job kinds: %w", err)
		}
		costs[kind] = cost
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read job kinds: %w", err)
	}
	if len(costs) != len(names) {
		return nil, ErrUnknownKind
	}
	return costs, nil
}

// existingKeys returns the job each of the batch's dedupe keys already points to.
func existingKeys(ctx context.Context, tx pgx.Tx, shop string, jobs []NewJob) (map[string]int64, error) {
	var keys []string
	for _, j := range jobs {
		if j.DedupeKey != "" {
			keys = append(keys, j.DedupeKey)
		}
	}
	found := map[string]int64{}
	if len(keys) == 0 {
		return found, nil
	}
	rows, err := tx.Query(ctx, `SELECT key, job_id FROM dedupe_keys WHERE shop_id = $1 AND key = ANY($2)`, shop, keys)
	if err != nil {
		return nil, keyError(err)
	}
	for rows.Next() {
		var key string
		var id int64
		if err := rows.Scan(&key, &id); err != nil {
			return nil, keyError(err)
		}
		found[key] = id
	}
	if err := rows.Err(); err != nil {
		return nil, keyError(err)
	}
	return found, nil
}

func keyError(err error) error {
	if isDataException(err) {
		return fmt.Errorf("%w: dedupe key: %w", ErrInvalidPayload, err)
	}
	return fmt.Errorf("store: read dedupe keys: %w", err)
}

// insertBatch inserts the jobs at positions fresh, and their dedupe keys, in
// one round trip, and records each new job's ID in ids at its position. A key
// that a concurrent enqueue took first makes it return errKeyTaken, which rolls
// the whole batch back.
func insertBatch(ctx context.Context, tx pgx.Tx, jobs []NewJob, fresh []int, costs map[string]int32, ids []int64) error {
	// enqueued_at defaults to now(), the transaction's start, and run_at and
	// first_runnable_at use the same instant (D-011).
	const insert = `
		WITH j AS (
		  INSERT INTO jobs (shop_id, kind, cost, payload, state, run_at, first_runnable_at)
		  VALUES ($1, $2, $3, $4, 'queued', coalesce($5, now()), greatest(now(), coalesce($5, now())))
		  RETURNING id),
		k AS (
		  INSERT INTO dedupe_keys (shop_id, key, job_id)
		  SELECT $1, $6::text, j.id FROM j WHERE $6::text <> ''
		  ON CONFLICT (shop_id, key) DO NOTHING
		  RETURNING 1)
		SELECT j.id, (SELECT count(*) FROM k) FROM j`
	var batch pgx.Batch
	for _, i := range fresh {
		j := jobs[i]
		batch.Queue(insert, j.Shop, j.Kind, costs[j.Kind], j.Payload, j.RunAt, j.DedupeKey)
	}
	results := tx.SendBatch(ctx, &batch)
	var firstErr error
	for _, i := range fresh {
		var id int64
		var keyed int
		if err := results.QueryRow().Scan(&id, &keyed); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if firstErr == nil && jobs[i].DedupeKey != "" && keyed == 0 {
			firstErr = errKeyTaken
		}
		ids[i] = id
	}
	if err := results.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	switch {
	case firstErr == nil:
		return nil
	case errors.Is(firstErr, errKeyTaken):
		return firstErr
	case isDataException(firstErr):
		return fmt.Errorf("%w: %w", ErrInvalidPayload, firstErr)
	}
	return fmt.Errorf("store: insert jobs: %w", firstErr)
}
