// Package lease is the worker side of leases (design §7): one batched heartbeat
// per worker process that renews every lease the process holds, and the local
// safety timer that gives up on a lease when heartbeats keep failing.
//
// The database is the only authority on a lease (NFR-14, D-036). The keeper
// cancels a handler when a heartbeat reports its lease gone (AC-4.3), and when
// heartbeats have failed for about a whole lease, which only saves wasted work:
// the fenced write would fail anyway. It never decides from its own clock that a
// lease is still valid.
package lease

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/store"
)

var (
	// ErrLeaseLost is the cancel cause when a heartbeat no longer finds the lease:
	// it expired, or the job was reaped or claimed again.
	ErrLeaseLost = errors.New("lease lost: the heartbeat found it expired or taken")
	// ErrHeartbeatsFailing is the cancel cause when heartbeats have failed for
	// about a whole lease, so the lease has probably expired.
	ErrHeartbeatsFailing = errors.New("heartbeats failed for a whole lease")
)

// HeartbeatFunc renews the given leases in one statement and returns the job IDs
// whose leases are still live. store.Heartbeat is the real one.
type HeartbeatFunc func(ctx context.Context, leases []store.Fence) ([]int64, error)

// Keeper renews a worker process's leases.
type Keeper struct {
	interval time.Duration
	beat     HeartbeatFunc
	log      *slog.Logger

	mu   sync.Mutex
	held map[int64]*held
}

type held struct {
	fence  store.Fence
	lease  time.Duration
	cancel context.CancelCauseFunc
	// failsafe fires if no heartbeat has renewed the lease for about a lease.
	// It is a runtime timer, so it measures elapsed time and never reads the
	// wall clock.
	failsafe *time.Timer
}

// New returns a keeper that heartbeats every interval. The interval must be
// shorter than every lease it will hold (config validation guarantees that).
func New(interval time.Duration, beat HeartbeatFunc, log *slog.Logger) *Keeper {
	return &Keeper{interval: interval, beat: beat, log: log, held: map[int64]*held{}}
}

// Hold starts renewing the lease identified by f, whose length is lease. When
// the lease is lost, the keeper calls cancel with ErrLeaseLost or
// ErrHeartbeatsFailing and stops renewing it. The returned release stops renewal
// without cancelling; call it when the run is over.
func (k *Keeper) Hold(f store.Fence, lease time.Duration, cancel context.CancelCauseFunc) (release func()) {
	h := &held{fence: f, lease: lease, cancel: cancel}
	k.mu.Lock()
	h.failsafe = time.AfterFunc(k.renewedFor(lease), func() { k.drop(h, ErrHeartbeatsFailing) })
	k.held[f.JobID] = h
	k.mu.Unlock()
	return func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		if k.held[f.JobID] == h {
			delete(k.held, f.JobID)
			h.failsafe.Stop()
		}
	}
}

// renewedFor is how long a lease is known to stay live after a successful
// heartbeat. A heartbeat statement is bounded by the interval, so the database
// renewed the lease at most one interval before the worker learned of it.
func (k *Keeper) renewedFor(lease time.Duration) time.Duration {
	return max(lease-k.interval, k.interval)
}

// drop stops holding h, if it is still held, and cancels its run with cause.
func (k *Keeper) drop(h *held, cause error) {
	k.mu.Lock()
	if k.held[h.fence.JobID] != h {
		k.mu.Unlock()
		return
	}
	delete(k.held, h.fence.JobID)
	h.failsafe.Stop()
	k.mu.Unlock()
	k.log.Warn("lease given up", "job", h.fence.JobID, "claim_seq", h.fence.ClaimSeq, "cause", cause.Error())
	h.cancel(cause)
}

// Run heartbeats every interval until ctx is done.
func (k *Keeper) Run(ctx context.Context) {
	ticker := time.NewTicker(k.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			k.Beat(ctx)
		}
	}
}

// Beat sends one batched heartbeat for every held lease and acts on the answer.
// Run calls it every interval; tests call it directly.
func (k *Keeper) Beat(ctx context.Context) {
	k.mu.Lock()
	batch := make([]*held, 0, len(k.held))
	fences := make([]store.Fence, 0, len(k.held))
	for _, h := range k.held {
		batch = append(batch, h)
		fences = append(fences, h.fence)
	}
	k.mu.Unlock()
	if len(batch) == 0 {
		return
	}

	beatCtx, cancel := context.WithTimeout(ctx, k.interval)
	alive, err := k.beat(beatCtx, fences)
	cancel()
	if err != nil {
		// Keep the leases: the next beat may succeed. If none does for about a
		// lease, each lease's failsafe timer gives up on it.
		if ctx.Err() == nil {
			k.log.Warn("heartbeat failed", "leases", len(fences), "err", err.Error())
		}
		return
	}
	live := make(map[int64]bool, len(alive))
	for _, id := range alive {
		live[id] = true
	}
	for _, h := range batch {
		if !live[h.fence.JobID] {
			k.drop(h, ErrLeaseLost)
			continue
		}
		k.mu.Lock()
		if k.held[h.fence.JobID] == h {
			h.failsafe.Reset(k.renewedFor(h.lease))
		}
		k.mu.Unlock()
	}
}

// Held returns the number of leases being renewed.
func (k *Keeper) Held() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.held)
}
