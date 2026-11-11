package mockshop

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

var t0 = time.Date(2026, 12, 21, 10, 0, 0, 0, time.UTC)

// P5-5: the level, the leak and the Retry-After math of design §12, step by step.
func TestBucketMath(t *testing.T) {
	b := NewBucket(3, 1, t0) // capacity 3, leaking 1 per second
	for i := range 3 {
		if ok, _ := b.Take(t0); !ok {
			t.Fatalf("request %d into an empty bucket of 3 was refused", i+1)
		}
	}
	if got := b.CallLimit(t0); got != "3/3" {
		t.Errorf("call limit when full = %q, want 3/3", got)
	}
	// Full: level 3, so level + 1 > capacity. One slot frees in 1 s.
	if ok, wait := b.Take(t0); ok || wait != time.Second {
		t.Errorf("full bucket: allowed %v, Retry-After %v; want refused, 1s", ok, wait)
	}
	// Half a second later the level is 2.5: still no room, 0.5 s to go.
	at := t0.Add(500 * time.Millisecond)
	if ok, wait := b.Take(at); ok || wait != 500*time.Millisecond {
		t.Errorf("at +0.5s: allowed %v, Retry-After %v; want refused, 500ms", ok, wait)
	}
	if got := b.Level(at); math.Abs(got-2.5) > 1e-9 {
		t.Errorf("level at +0.5s = %v, want 2.5 (a refused request adds nothing)", got)
	}
	if got := b.CallLimit(at); got != "3/3" {
		t.Errorf("call limit at level 2.5 = %q, want 3/3 (rounded up)", got)
	}
	// At +1 s the level is 2, so one request fits, taking it back to 3.
	at = t0.Add(time.Second)
	if ok, _ := b.Take(at); !ok {
		t.Error("at +1s a request was refused")
	}
	// The level never drains below 0.
	at = t0.Add(time.Hour)
	if got := b.Level(at); got != 0 {
		t.Errorf("level after an hour = %v, want 0", got)
	}
	if got := b.CallLimit(at); got != "0/3" {
		t.Errorf("call limit when empty = %q, want 0/3", got)
	}
	if b.Capacity() != 3 {
		t.Errorf("Capacity = %d", b.Capacity())
	}
}

// Retry-After is rounded up to the millisecond: 1/3 s becomes 334 ms, and
// waiting exactly that long is enough while 333 ms is not.
func TestRetryAfterRoundsUp(t *testing.T) {
	b := NewBucket(1, 3, t0)
	b.Take(t0)
	_, wait := b.Take(t0)
	if wait != 334*time.Millisecond {
		t.Fatalf("Retry-After = %v, want 334ms", wait)
	}
	if ok, _ := b.Take(t0.Add(333 * time.Millisecond)); ok {
		t.Error("allowed after 333ms, before one slot freed")
	}
	if ok, _ := b.Take(t0.Add(334 * time.Millisecond)); !ok {
		t.Error("refused after waiting the Retry-After it was given")
	}
}

// P5-5, as a property: against a model of design §12, for random capacities,
// leak rates and request times, a request is allowed exactly when level + 1 ≤
// capacity, the level stays in [0, capacity], and a refused caller that waits
// its Retry-After gets in, while one that waits a millisecond less does not.
func TestBucketAgreesWithTheModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		capacity := rapid.IntRange(1, 50).Draw(rt, "capacity")
		leak := rapid.Float64Range(0.1, 100).Draw(rt, "leak")
		b := NewBucket(capacity, leak, t0)
		level, now := 0.0, t0
		for i := range rapid.IntRange(1, 200).Draw(rt, "requests") {
			step := time.Duration(rapid.Int64Range(0, int64(2*time.Second)).Draw(rt, "step")) / time.Microsecond * time.Microsecond
			now = now.Add(step)
			level = math.Max(0, level-step.Seconds()*leak)
			ok, wait := b.Take(now)
			if want := level+1 <= float64(capacity)+1e-9; ok != want {
				rt.Fatalf("request %d at level %v of %d: allowed %v, model says %v", i, level, capacity, ok, want)
			}
			if !ok {
				if wait <= 0 {
					rt.Fatalf("request %d refused with Retry-After %v", i, wait)
				}
				probe := *b
				exact := (level + 1 - float64(capacity)) / leak // seconds until one slot frees
				if early := wait - time.Millisecond; early > 0 && early.Seconds() < exact-1e-6 {
					if ok, _ := probe.Take(now.Add(early)); ok {
						rt.Fatalf("request %d: allowed a millisecond before its Retry-After %v", i, wait)
					}
				}
				probe = *b
				if late, _ := probe.Take(now.Add(wait)); !late {
					rt.Fatalf("request %d: refused after waiting its Retry-After %v", i, wait)
				}
				continue
			}
			level++
			if got := b.Level(now); math.Abs(got-level) > 1e-6 || got < 0 || got > float64(capacity)+1e-6 {
				rt.Fatalf("request %d: level %v, model %v, capacity %d", i, got, level, capacity)
			}
		}
	})
}
