package testkinds_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

func run(t *testing.T, ctx context.Context, kind, payload string, attempts int32) error {
	t.Helper()
	reg := queue.NewRegistry()
	testkinds.Register(reg)
	h, ok := reg.Lookup(kind)
	if !ok {
		t.Fatalf("kind %s not registered", kind)
	}
	return h(ctx, &queue.Run{Job: queue.Job{Kind: kind, Payload: []byte(payload), Attempts: attempts}})
}

func TestRegisterAddsEveryKind(t *testing.T) {
	reg := queue.NewRegistry()
	testkinds.Register(reg)
	if got := reg.Kinds(); len(got) != 5 {
		t.Errorf("kinds = %v, want the five test kinds", got)
	}
}

func TestFail(t *testing.T) {
	ctx := t.Context()
	if err := run(t, ctx, testkinds.Fail, `{"fail_times": 2}`, 1); err == nil || queue.IsPermanent(err) {
		t.Errorf("attempt 2 of fail_times 2: %v, want a temporary failure", err)
	}
	if err := run(t, ctx, testkinds.Fail, `{"fail_times": 2}`, 2); err != nil {
		t.Errorf("attempt 3 of fail_times 2: %v, want success", err)
	}
	if err := run(t, ctx, testkinds.Fail, `{"fail_times": -1}`, 100); err == nil {
		t.Error("fail_times -1 succeeded, want it to fail forever")
	}
	if err := run(t, ctx, testkinds.Fail, `{"fail_times": -1, "permanent": true, "message": "no"}`, 0); !queue.IsPermanent(err) || err.Error() != "no" {
		t.Errorf("permanent: %v", err)
	}
}

func TestSleepHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("lease lost")
	cancel(cause)
	if err := run(t, ctx, testkinds.Sleep, `{"ms": 60000}`, 0); !errors.Is(err, cause) {
		t.Errorf("cancelled sleep returned %v, want the cancel cause", err)
	}
	if err := run(t, t.Context(), testkinds.Sleep, `{"ms": 1}`, 0); err != nil {
		t.Errorf("short sleep: %v", err)
	}
}

func TestHangIgnoresCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now()
	if err := run(t, ctx, testkinds.Hang, `{"ms": 50}`, 0); err != nil || time.Since(start) < 50*time.Millisecond {
		t.Errorf("hang returned %v after %v, want nil after the full 50ms", err, time.Since(start))
	}
}

func TestPanic(t *testing.T) {
	defer func() {
		if p := recover(); p != "test panic: boom" {
			t.Errorf("recovered %v, want the test panic", p)
		}
	}()
	_ = run(t, t.Context(), testkinds.Panic, `{"message": "boom"}`, 0)
}

func TestBadPayloadIsPermanent(t *testing.T) {
	for _, payload := range []string{`{"ms": "x"}`, `{"unknown": 1}`, `{"ms": -1}`, `not json`} {
		if err := run(t, t.Context(), testkinds.Sleep, payload, 0); !queue.IsPermanent(err) {
			t.Errorf("payload %s: %v, want a permanent failure", payload, err)
		}
	}
}
