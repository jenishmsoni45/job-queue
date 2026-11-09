package queue_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/queue"
)

func TestPermanent(t *testing.T) {
	base := errors.New("bad input")
	err := queue.Permanent(base)
	if !queue.IsPermanent(err) || !errors.Is(err, base) || err.Error() != "bad input" {
		t.Errorf("Permanent(base) = %v; permanent %v, wraps base %v", err, queue.IsPermanent(err), errors.Is(err, base))
	}
	if wrapped := errors.Join(errors.New("context"), err); !queue.IsPermanent(wrapped) {
		t.Error("a wrapped Permanent error is no longer permanent")
	}
	if !queue.IsPermanent(queue.Permanent(nil)) {
		t.Error("Permanent(nil) is not permanent")
	}
	if queue.IsPermanent(base) || queue.IsPermanent(nil) {
		t.Error("a plain error counts as permanent")
	}
}

func TestRegistry(t *testing.T) {
	r := queue.NewRegistry()
	h := func(context.Context, *queue.Run) error { return nil }
	r.Register("b.kind", h)
	r.Register("a.kind", h)
	if _, ok := r.Lookup("a.kind"); !ok {
		t.Error("registered kind not found")
	}
	if _, ok := r.Lookup("c.kind"); ok {
		t.Error("unregistered kind found")
	}
	if got := r.Kinds(); !slices.Equal(got, []string{"a.kind", "b.kind"}) {
		t.Errorf("Kinds = %v", got)
	}
	for name, register := range map[string]func(){
		"duplicate kind": func() { r.Register("a.kind", h) },
		"nil handler":    func() { r.Register("c.kind", nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: Register did not panic", name)
				}
			}()
			register()
		}()
	}
}

// AC-5.1: the delay after the n-th failure is uniform in [0, min(cap, base × 2^(n−1))).
func TestBackoffBounds(t *testing.T) {
	b := queue.DefaultBackoff
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	for attempts, want := range map[int32]time.Duration{
		0: 2 * time.Second, 1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second,
		8: 256 * time.Second, 9: 5 * time.Minute, 10: 5 * time.Minute, 1 << 30: 5 * time.Minute,
	} {
		if got := b.Bound(attempts); got != want {
			t.Errorf("Bound(%d) = %v, want %v", attempts, got, want)
		}
	}
	huge := queue.Backoff{Base: time.Second, Cap: time.Duration(1<<63 - 1)}
	if got := huge.Bound(100); got != huge.Cap {
		t.Errorf("Bound with a huge cap = %v, want the cap without overflow", got)
	}
}

func TestBackoffDelayIsFullJitter(t *testing.T) {
	b := queue.Backoff{Base: 100 * time.Millisecond, Cap: time.Second}
	for _, attempts := range []int32{1, 3, 10} {
		bound := b.Bound(attempts)
		lo, hi := bound, time.Duration(0)
		for range 4000 {
			d := b.Delay(attempts)
			if d < 0 || d >= bound {
				t.Fatalf("Delay(%d) = %v, outside [0, %v)", attempts, d, bound)
			}
			lo, hi = min(lo, d), max(hi, d)
		}
		// Full jitter spreads over the whole range, not just its top half.
		if lo > bound/10 || hi < bound*9/10 {
			t.Errorf("Delay(%d) over 4000 draws spans [%v, %v], want close to [0, %v)", attempts, lo, hi, bound)
		}
	}
}

func TestBackoffValidate(t *testing.T) {
	for _, b := range []queue.Backoff{{}, {Base: time.Second}, {Base: 2 * time.Second, Cap: time.Second}, {Base: -1, Cap: time.Second}} {
		if b.Validate() == nil {
			t.Errorf("Validate(%+v) = nil, want an error", b)
		}
	}
}
