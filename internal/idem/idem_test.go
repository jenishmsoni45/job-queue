package idem_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/idem"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

func once(t *testing.T, tx pgx.Tx, jobID int64, step string) bool {
	t.Helper()
	first, err := idem.Once(t.Context(), tx, jobID, step)
	if err != nil {
		t.Fatal(err)
	}
	return first
}

// Once is true the first time a (job, step) is recorded and false once a
// committed transaction has recorded it. A rolled-back record doesn't count.
// Steps and jobs are independent.
func TestOnce(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	job := env.Enqueue(t, queuetest.ShopA, testkinds.Sleep, nil)
	other := env.Enqueue(t, queuetest.ShopA, testkinds.Sleep, nil)
	ctx := t.Context()

	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !once(t, tx, job, "charge") {
		t.Fatal("first Once = false")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	err = pgx.BeginFunc(ctx, env.Pool, func(tx pgx.Tx) error {
		if !once(t, tx, job, "charge") {
			t.Error("Once after a rolled-back record = false, want true")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = pgx.BeginFunc(ctx, env.Pool, func(tx pgx.Tx) error {
		if once(t, tx, job, "charge") {
			t.Error("Once after a committed record = true, want false")
		}
		if !once(t, tx, job, "notify") {
			t.Error("another step of the same job = false, want true")
		}
		if !once(t, tx, other, "charge") {
			t.Error("the same step of another job = false, want true")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// AC-7.1 at the helper: two overlapping transactions record the same step. The
// second waits for the first: false if the first commits, true if it rolls
// back. So at most one of them can commit the step's effect.
func TestOnceWaitsForAnOverlappingRecord(t *testing.T) {
	t.Parallel()
	for _, firstCommits := range []bool{true, false} {
		env := queuetest.New(t, nil)
		job := env.Enqueue(t, queuetest.ShopA, testkinds.Sleep, nil)
		ctx := t.Context()

		first, err := env.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !once(t, first, job, "apply") {
			t.Fatal("first Once = false")
		}
		secondResult := make(chan bool, 1)
		go func() {
			defer close(secondResult)
			_ = pgx.BeginFunc(context.Background(), env.Pool, func(tx pgx.Tx) error {
				got, err := idem.Once(context.Background(), tx, job, "apply")
				if err != nil {
					t.Error(err)
					return err
				}
				secondResult <- got
				return nil
			})
		}()
		select {
		case got := <-secondResult:
			t.Fatalf("the overlapping Once returned %v without waiting for the first transaction", got)
		case <-time.After(200 * time.Millisecond):
		}
		if firstCommits {
			err = first.Commit(ctx)
		} else {
			err = first.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		if got := <-secondResult; got == firstCommits {
			t.Errorf("first commits=%v: the overlapping Once = %v, want %v", firstCommits, got, !firstCommits)
		}
	}
}

func TestOnceRejectsBadSteps(t *testing.T) {
	t.Parallel()
	env := queuetest.New(t, nil)
	job := env.Enqueue(t, queuetest.ShopA, testkinds.Sleep, nil)
	for _, step := range []string{"", "-lead", "has space", "new\nline", strings.Repeat("s", 101), "ünïcode"} {
		err := pgx.BeginFunc(t.Context(), env.Pool, func(tx pgx.Tx) error {
			_, err := idem.Once(t.Context(), tx, job, step)
			return err
		})
		if !errors.Is(err, idem.ErrBadStep) {
			t.Errorf("step %q: err = %v, want ErrBadStep", step, err)
		}
	}
	for _, step := range []string{"tag", "order.tag:v2", "A_1-b", strings.Repeat("s", 100)} {
		if err := idem.ValidStep(step); err != nil {
			t.Errorf("step %q rejected: %v", step, err)
		}
	}
}

// AC-7.3: the key depends only on the job and the step, so every run of a job
// sends the same Idempotency-Key.
func TestKey(t *testing.T) {
	t.Parallel()
	if got := idem.Key(42, "tag"); got != "job-42-tag" {
		t.Errorf("Key(42, tag) = %q, want job-42-tag", got)
	}
	if idem.Key(42, "tag") == idem.Key(43, "tag") || idem.Key(42, "tag") == idem.Key(42, "note") {
		t.Error("two different jobs or steps share a key")
	}
}
