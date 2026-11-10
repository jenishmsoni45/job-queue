package dispatcher

import (
	"math"
	"time"
)

// bucket is a token bucket: it holds up to burst tokens, refills at rate tokens
// per second, and each claimed job costs one token (design §6.3). It starts
// full. Over any interval it hands out at most rate × elapsed + burst tokens
// (AC-10.2). The times it is given come from the dispatcher's monotonic clock:
// a bucket is a rate limit, not a lease or a timestamp, so NFR-14 doesn't apply.
type bucket struct {
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rate float64, burst int, now time.Time) *bucket {
	return &bucket{rate: rate, burst: float64(burst), tokens: float64(burst), last: now}
}

// configure applies the shop's current settings. Tokens beyond a smaller burst are lost.
func (b *bucket) configure(rate float64, burst int, now time.Time) {
	b.refill(now)
	b.rate, b.burst = rate, float64(burst)
	b.tokens = math.Min(b.tokens, b.burst)
}

func (b *bucket) refill(now time.Time) {
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(b.burst, b.tokens+elapsed.Seconds()*b.rate)
		b.last = now
	}
}

// whole returns the whole tokens in the bucket at now.
func (b *bucket) whole(now time.Time) int {
	b.refill(now)
	return int(math.Floor(b.tokens))
}

// spend takes n tokens, never going below zero.
func (b *bucket) spend(n int, now time.Time) {
	b.refill(now)
	b.tokens = math.Max(0, b.tokens-float64(n))
}
