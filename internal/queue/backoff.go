package queue

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Backoff is exponential backoff with full jitter (D-004, design §8).
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
}

// DefaultBackoff is the D-004 setting: base 2 s, cap 5 min.
var DefaultBackoff = Backoff{Base: 2 * time.Second, Cap: 5 * time.Minute}

// Validate checks that 0 < Base <= Cap.
func (b Backoff) Validate() error {
	if b.Base <= 0 || b.Cap < b.Base {
		return errors.New("queue: backoff needs 0 < base <= cap")
	}
	return nil
}

// Bound returns min(cap, base × 2^(attempts − 1)), the exclusive upper bound of
// the delay after the attempts-th failed attempt. attempts below 1 count as 1.
func (b Backoff) Bound(attempts int32) time.Duration {
	bound := b.Base
	for i := int32(1); i < attempts && bound < b.Cap; i++ {
		if bound > b.Cap/2 {
			bound = b.Cap // doubling would pass the cap (and could overflow)
			break
		}
		bound *= 2
	}
	return min(bound, b.Cap)
}

// Delay draws the delay before the next run after the attempts-th failed
// attempt: uniform in [0, Bound(attempts)) (AC-5.1). The worker draws it and
// the database adds it to its own clock (D-036).
func (b Backoff) Delay(attempts int32) time.Duration {
	bound := b.Bound(attempts)
	if bound <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(bound))) //nolint:gosec // jitter, not a secret
}

// ThrottleJitter draws the jitter added to a throttle's wait: uniform in
// [0, 10% of retryAfter) (design §2), so jobs throttled together don't all come
// back at the same instant.
func ThrottleJitter(retryAfter time.Duration) time.Duration {
	bound := retryAfter / 10
	if bound <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(bound))) //nolint:gosec // jitter, not a secret
}
