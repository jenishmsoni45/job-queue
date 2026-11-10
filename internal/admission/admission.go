// Package admission is the per-shop backlog limit (design §5, D-007). A shop's
// backlog is its unfinished jobs, queued or running. The enqueue transaction
// checks the limit exactly, under the shop's row lock, and an enqueue that would
// pass it is rejected: nothing is created (Q8), and only that shop is affected.
package admission

import (
	"fmt"
	"time"
)

// Fits reports whether adding n jobs to a backlog of backlog jobs stays within
// limit. A batch is checked as a whole, so a batch that doesn't fit creates none
// of its jobs (D-030).
func Fits(backlog, limit, n int) bool {
	return int64(backlog)+int64(n) <= int64(limit)
}

// Rejected is the error of an enqueue the backlog limit refused. The API turns it
// into a 429 with a Retry-After header. By the time a caller sees it, the shop's
// shed count already includes the jobs it refused (AC-11.3).
type Rejected struct {
	Shop string
	// Shed is how many jobs were refused: 1 for a single enqueue, and for a
	// batch the jobs that would have been created, not the duplicates (D-049).
	Shed int
	// RetryAfter is the shop's backlog_retry_after_ms.
	RetryAfter time.Duration
}

func (e *Rejected) Error() string {
	return fmt.Sprintf("shop %s is at its backlog limit: %d job(s) rejected", e.Shop, e.Shed)
}

// RetryAfterSeconds is the Retry-After header value for d: whole seconds,
// rounded up, and at least 1 (an HTTP delay is an integer number of seconds).
func RetryAfterSeconds(d time.Duration) int {
	secs := int((d + time.Second - 1) / time.Second)
	return max(secs, 1)
}
