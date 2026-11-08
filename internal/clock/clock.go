// Package clock is the worker's only source of wall-clock time (NFR-14).
//
// The worker never uses its own clock for a lease decision: claims, expiry and
// fencing are judged by Postgres with statement_timestamp() (D-036), and the
// worker's local safety timers measure elapsed time with Go's monotonic timers,
// not by reading the wall clock. The worker reads wall-clock time only to stamp
// its run reports, and it does so through a Clock, so that tests/adversarial can
// skew it and show that lease outcomes don't change (P2-10). A test there also
// checks that the worker packages never call time.Now, time.Since or time.Until.
package clock

import "time"

// Clock tells the time.
type Clock interface {
	Now() time.Time
}

// System is the real clock.
type System struct{}

// Now returns the current time.
func (System) Now() time.Time { return time.Now() }
