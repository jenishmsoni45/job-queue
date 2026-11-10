// Package testkinds holds the test-only job handlers (design §11.3). They are
// compiled into queued but registered only when QUEUED_TEST_HANDLERS=1, for
// invariant tests and chaos runs. Each kind must also be listed in the config
// file the run uses, as tests/testdata/shops.json does.
package testkinds

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/queue"
)

// The test kinds.
const (
	// Sleep waits payload.ms milliseconds, stopping early if cancelled.
	Sleep = "test.sleep"
	// Fail returns an error while the job has fewer than payload.fail_times
	// failed attempts (a negative value fails forever), then succeeds. With
	// payload.permanent it fails with queue.Permanent instead.
	Fail = "test.fail"
	// Panic panics with payload.message.
	Panic = "test.panic"
	// Hang sleeps payload.ms milliseconds and ignores cancellation, like a
	// handler stuck in a call with no context.
	Hang = "test.hang"
	// Effect waits payload.ms milliseconds, stopping early if cancelled, then
	// writes one row (job_id, claim_seq) to the test_effects table through
	// run.Tx(). The row commits only with the fenced completion, so a completed
	// job has exactly one row and any other job has none (design §18, AC-7.2).
	// The table must exist: create it with EffectsTableSQL.
	Effect = "test.effect"
)

// EffectsTableSQL creates the table the Effect kind writes to. It is test-only
// and not part of the migrations; chaos runs and tests create it themselves.
const EffectsTableSQL = `CREATE TABLE IF NOT EXISTS test_effects (
  job_id     bigint      NOT NULL,
  claim_seq  int         NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT statement_timestamp()
)`

// Payload is the payload every test kind reads. Unused fields are ignored.
type Payload struct {
	MS        int    `json:"ms,omitempty"`
	FailTimes int    `json:"fail_times,omitempty"`
	Permanent bool   `json:"permanent,omitempty"`
	Message   string `json:"message,omitempty"`
}

// Register adds every test kind to r.
func Register(r *queue.Registry) {
	r.Register(Sleep, sleep)
	r.Register(Fail, fail)
	r.Register(Panic, panicking)
	r.Register(Hang, hang)
	r.Register(Effect, effect)
}

func decode(run *queue.Run) (Payload, error) {
	var p Payload
	dec := json.NewDecoder(bytes.NewReader(run.Job.Payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, queue.Permanent(fmt.Errorf("bad test payload: %w", err))
	}
	if p.MS < 0 {
		return p, queue.Permanent(errors.New("bad test payload: ms is negative"))
	}
	return p, nil
}

func sleep(ctx context.Context, run *queue.Run) error {
	p, err := decode(run)
	if err != nil {
		return err
	}
	t := time.NewTimer(time.Duration(p.MS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

func fail(_ context.Context, run *queue.Run) error {
	p, err := decode(run)
	if err != nil {
		return err
	}
	if p.FailTimes >= 0 && int(run.Job.Attempts) >= p.FailTimes {
		return nil
	}
	msg := p.Message
	if msg == "" {
		msg = fmt.Sprintf("test failure on attempt %d", run.Job.Attempts+1)
	}
	if p.Permanent {
		return queue.Permanent(errors.New(msg))
	}
	return errors.New(msg)
}

func panicking(_ context.Context, run *queue.Run) error {
	p, err := decode(run)
	if err != nil {
		return err
	}
	panic("test panic: " + p.Message)
}

func hang(_ context.Context, run *queue.Run) error {
	p, err := decode(run)
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(p.MS) * time.Millisecond)
	return nil
}

func effect(ctx context.Context, run *queue.Run) error {
	if err := sleep(ctx, run); err != nil {
		return err
	}
	tx, err := run.Tx(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO test_effects (job_id, claim_seq) VALUES ($1, $2)`,
		run.Job.ID, run.Job.ClaimSeq); err != nil {
		return fmt.Errorf("write the test effect: %w", err)
	}
	return nil
}
