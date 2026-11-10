package store

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
)

// ClaimSpec says what one claim may take (design §6.3). The zero value claims
// the globally oldest runnable jobs of any shop, with no cost limit: the FIFO
// claim. The cap step applies to every claim.
type ClaimSpec struct {
	// Shops limits the claim to jobs of these shops. Empty means every shop.
	Shops []string
	// MaxCost, if positive, is the most cost units the claim may take. The claim
	// takes jobs oldest first and stops at the first one that doesn't fit (DRR's
	// allowance, design §6.2).
	MaxCost int64
	// Limits caps how many jobs the claim takes per shop, on top of the shop's
	// concurrency cap: the whole tokens in the shop's bucket (design §6.3). A
	// shop that isn't listed has no extra limit.
	Limits map[string]int
}

// ClaimEnd says why a claim stopped.
type ClaimEnd int

const (
	// ClaimFull: every owner got a job.
	ClaimFull ClaimEnd = iota
	// ClaimCost: the next job's cost didn't fit within MaxCost.
	ClaimCost
	// ClaimBlocked: a shop's concurrency cap, its Limits entry or a throttle kept
	// at least one runnable job from being claimed.
	ClaimBlocked
	// ClaimDry: there were fewer runnable jobs than owners.
	ClaimDry
)

// ClaimOutcome is what one claim did.
type ClaimOutcome struct {
	Jobs []Claimed // the i-th job went to owners[i]
	Cost int64     // the jobs' cost units
	End  ClaimEnd
}

// Claim claims up to len(owners) runnable jobs, oldest first by (run_at, id),
// and gives the i-th claimed job to owners[i]. It is ClaimFor with the zero
// ClaimSpec: FIFO, with every shop's cap enforced.
func Claim(ctx context.Context, db DB, owners []string) ([]Claimed, error) {
	out, err := ClaimFor(ctx, db, owners, ClaimSpec{})
	return out.Jobs, err
}

// ClaimFor is the claim transaction (design §6.3). It claims up to len(owners)
// runnable jobs, oldest first by (run_at, id), within spec.
//
// FOR NO KEY UPDATE SKIP LOCKED means concurrent claimers never wait on each
// other and never take the same row, and the state check means only a queued
// job can be claimed, so a job has at most one live lease (Q3). It is not FOR
// UPDATE, which would also skip a job whose row a foreign key has locked FOR
// KEY SHARE, such as one with a run.Step transaction open (D-047).
//
// The cap step makes Q5 hold even with two dispatchers claiming at once. Each
// candidate shop's advisory lock is taken in shop-ID order (so claimers can't
// deadlock), and then the shop's running jobs are counted exactly: the count is
// read after the lock, so it includes every claim that committed before this one
// began holding it. A claim that starts after a throttle committed skips the shop
// (AC-6.2). Candidates the cap, the limits or a throttle rule out are left
// queued, and their row locks go at commit.
func ClaimFor(ctx context.Context, db DB, owners []string, spec ClaimSpec) (ClaimOutcome, error) {
	if len(owners) == 0 {
		return ClaimOutcome{}, nil
	}
	var out ClaimOutcome
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		out = ClaimOutcome{}
		cands, err := claimCandidates(ctx, tx, len(owners), spec.Shops)
		if err != nil {
			return err
		}
		if len(cands) == 0 {
			out.End = ClaimDry
			return nil
		}
		room, err := shopRoom(ctx, tx, cands, spec.Limits)
		if err != nil {
			return err
		}

		var kept []candidate
		blocked, overCost := false, false
		for _, c := range cands {
			switch {
			case room[c.shop] <= 0:
				blocked = true // later jobs of this shop are blocked too; other shops' may not be
				continue
			case spec.MaxCost > 0 && out.Cost+int64(c.cost) > spec.MaxCost:
				overCost = true
			default:
				kept = append(kept, c)
				room[c.shop]--
				out.Cost += int64(c.cost)
				continue
			}
			break // the next job doesn't fit the allowance: stop, don't skip to a cheaper one
		}
		switch {
		case overCost:
			out.End = ClaimCost
		case blocked:
			out.End = ClaimBlocked
		case len(cands) < len(owners):
			out.End = ClaimDry
		default:
			out.End = ClaimFull
		}
		if len(kept) == 0 {
			return nil
		}

		ids := make([]int64, len(kept))
		for i, c := range kept {
			ids[i] = c.id
		}
		out.Jobs, err = claimRows(ctx, tx, ids, owners[:len(ids)])
		return err
	})
	if err != nil {
		return ClaimOutcome{}, err
	}
	return out, nil
}

type candidate struct {
	id   int64
	shop string
	cost int32
}

// claimCandidates locks up to n queued, runnable jobs, oldest first, skipping
// rows another transaction holds. With shops, only their jobs are taken.
func claimCandidates(ctx context.Context, tx pgx.Tx, n int, shops []string) ([]candidate, error) {
	const head = `SELECT id, shop_id, cost FROM jobs WHERE state = 'queued' AND run_at <= statement_timestamp()`
	const tail = ` ORDER BY run_at, id LIMIT $1 FOR NO KEY UPDATE SKIP LOCKED`
	var (
		rows pgx.Rows
		err  error
	)
	// Two statements rather than one with an optional filter, so the planner can
	// use jobs_queued_shop for a claim that names its shops.
	if len(shops) == 0 {
		rows, err = tx.Query(ctx, head+tail, n)
	} else {
		rows, err = tx.Query(ctx, head+` AND shop_id = ANY($2)`+tail, n, shops)
	}
	if err != nil {
		return nil, fmt.Errorf("store: claim candidates: %w", err)
	}
	cands, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (candidate, error) {
		var c candidate
		return c, row.Scan(&c.id, &c.shop, &c.cost)
	})
	if err != nil {
		return nil, fmt.Errorf("store: claim candidates: %w", err)
	}
	return cands, nil
}

// shopRoom takes each candidate shop's claim lock, in shop-ID order, and returns
// how many more jobs each may start: its concurrency cap less its running jobs,
// at most its limit, and none while it is throttled.
func shopRoom(ctx context.Context, tx pgx.Tx, cands []candidate, limits map[string]int) (map[string]int, error) {
	shopSet := map[string]struct{}{}
	for _, c := range cands {
		shopSet[c.shop] = struct{}{}
	}
	room := make(map[string]int, len(shopSet))
	for _, shop := range slices.Sorted(maps.Keys(shopSet)) {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('claim:' || $1::text, 0))`, shop); err != nil {
			return nil, fmt.Errorf("store: claim lock for shop %s: %w", shop, err)
		}
		var maxConc, running int32
		var throttled bool
		err := tx.QueryRow(ctx, `
			SELECT sh.max_concurrency,
			       (SELECT count(*) FROM jobs j WHERE j.shop_id = sh.id AND j.state = 'running'),
			       coalesce(st.throttled_until > statement_timestamp(), false)
			  FROM shops sh JOIN shop_state st ON st.shop_id = sh.id
			 WHERE sh.id = $1`, shop).Scan(&maxConc, &running, &throttled)
		if err != nil {
			return nil, fmt.Errorf("store: claim cap check for shop %s: %w", shop, err)
		}
		r := int(maxConc) - int(running)
		if limit, ok := limits[shop]; ok {
			r = min(r, limit)
		}
		if throttled {
			r = 0
		}
		room[shop] = r
	}
	return room, nil
}

// claimRows turns the locked jobs ids into running jobs, the i-th for owners[i].
func claimRows(ctx context.Context, tx pgx.Tx, ids []int64, owners []string) ([]Claimed, error) {
	rows, err := tx.Query(ctx, `
		UPDATE jobs j
		   SET state = 'running', claim_seq = j.claim_seq + 1, lease_owner = c.owner,
		       lease_expires_at = statement_timestamp() + k.lease_ms * interval '1 millisecond',
		       first_claimed_at = coalesce(j.first_claimed_at, statement_timestamp())
		  FROM unnest($1::bigint[], $2::text[]) AS c (id, owner), job_kinds k
		 WHERE j.id = c.id AND k.kind = j.kind AND j.state = 'queued'
		RETURNING j.id, j.shop_id, j.kind, j.payload, j.claim_seq, j.attempts, j.lease_owner,
		          k.lease_ms, k.heartbeat_ms, k.timeout_ms, statement_timestamp(), j.lease_expires_at`,
		ids, owners)
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	updated, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Claimed, error) {
		var c Claimed
		err := row.Scan(&c.ID, &c.Shop, &c.Kind, &c.Payload, &c.ClaimSeq, &c.Attempts, &c.Owner,
			&c.LeaseMS, &c.HeartbeatMS, &c.TimeoutMS, &c.ClaimedAt, &c.LeaseExpiresAt)
		return c, err
	})
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	byID := make(map[int64]Claimed, len(updated))
	for _, c := range updated {
		byID[c.ID] = c
	}
	if len(byID) != len(ids) {
		// Every candidate is locked by this transaction and was queued, so this
		// would mean a broken invariant. Roll back rather than hand out half a claim.
		return nil, fmt.Errorf("store: claim updated %d of %d locked candidates", len(byID), len(ids))
	}
	claimed := make([]Claimed, len(ids))
	for i, id := range ids {
		claimed[i] = byID[id]
	}
	return claimed, nil
}

// ShopSnapshot is what the dispatcher's refresh learns about one shop (design §6.1).
type ShopSnapshot struct {
	ID             string
	Weight         int
	MaxConcurrency int
	RatePerSec     float64
	RateBurst      int
	// Running is the shop's running jobs.
	Running int
	// Runnable means the shop has a queued job whose run_at has passed.
	Runnable bool
	// Throttled means the shop's throttled_until is in the future.
	Throttled bool
}

// SnapshotShops reads every shop's settings and state in one statement, so
// "now" is one instant for all of them. Both time comparisons use the database
// clock, statement_timestamp(), as the claim transaction does. The result only
// hints at what a claim can do: the claim transaction is the authority (design §6.1).
func SnapshotShops(ctx context.Context, db DB) ([]ShopSnapshot, error) {
	rows, err := db.Query(ctx, `
		SELECT s.id, s.weight, s.max_concurrency, s.rate_per_sec, s.rate_burst,
		       (SELECT count(*) FROM jobs j WHERE j.shop_id = s.id AND j.state = 'running'),
		       EXISTS (SELECT 1 FROM jobs j WHERE j.shop_id = s.id AND j.state = 'queued' AND j.run_at <= statement_timestamp()),
		       coalesce(st.throttled_until > statement_timestamp(), false)
		  FROM shops s JOIN shop_state st ON st.shop_id = s.id
		 ORDER BY s.id`)
	if err != nil {
		return nil, fmt.Errorf("store: snapshot shops: %w", err)
	}
	snaps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (ShopSnapshot, error) {
		var s ShopSnapshot
		return s, row.Scan(&s.ID, &s.Weight, &s.MaxConcurrency, &s.RatePerSec, &s.RateBurst, &s.Running, &s.Runnable, &s.Throttled)
	})
	if err != nil {
		return nil, fmt.Errorf("store: snapshot shops: %w", err)
	}
	return snaps, nil
}
