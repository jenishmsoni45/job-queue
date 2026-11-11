// Package mockshop is a local simulation of Shopify (design §12, D-010): an
// Admin API with a leaky bucket per shop, Idempotency-Key replay and a mutation
// log, and a webhook sender that signs orders/create webhooks, sometimes sends
// them twice, and retries failed deliveries. All of its state lives in memory.
// It never calls the real Shopify.
package mockshop

import (
	"math"
	"time"
)

// Bucket is a request-count leaky bucket (design §12). Each request adds 1 to
// the level if level + 1 ≤ capacity, and is refused otherwise. The level drains
// continuously at the leak rate. It is not safe for concurrent use; the API
// guards each shop's bucket with that shop's lock.
type Bucket struct {
	capacity float64
	leak     float64 // per second
	level    float64
	last     time.Time
}

// NewBucket returns an empty bucket.
func NewBucket(capacity int, leakPerSec float64, now time.Time) *Bucket {
	return &Bucket{capacity: float64(capacity), leak: leakPerSec, last: now}
}

// drain lowers the level by what leaked since the last call.
func (b *Bucket) drain(now time.Time) {
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.level = math.Max(0, b.level-elapsed.Seconds()*b.leak)
		b.last = now
	}
}

// Take tries to add one request at now. It reports whether the request is
// allowed, and when it isn't, how long until one slot frees: the time for the
// level to drain to capacity − 1, rounded up to the millisecond, so a caller
// that waits exactly that long is allowed.
func (b *Bucket) Take(now time.Time) (allowed bool, retryAfter time.Duration) {
	b.drain(now)
	// The epsilon absorbs floating-point error, so a caller that waited the
	// Retry-After it was given is never refused by a rounding error.
	if b.level+1 <= b.capacity+1e-9 {
		b.level++
		return true, 0
	}
	secs := (b.level + 1 - b.capacity) / b.leak
	return false, time.Duration(math.Ceil(secs*1000)) * time.Millisecond
}

// Level returns the level at now.
func (b *Bucket) Level(now time.Time) float64 {
	b.drain(now)
	return b.level
}

// Capacity returns the bucket's capacity.
func (b *Bucket) Capacity() int { return int(b.capacity) }

// CallLimit is the X-Shopify-Shop-Api-Call-Limit value at now: the level,
// rounded up to a whole request, over the capacity.
func (b *Bucket) CallLimit(now time.Time) string {
	level := int(math.Ceil(b.Level(now) - 1e-9))
	return itoa(level) + "/" + itoa(int(b.capacity))
}
