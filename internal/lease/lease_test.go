package lease_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/lease"
	"github.com/jenishmsoni45/job-queue/internal/store"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeBeat answers heartbeats from a script.
type fakeBeat struct {
	mu    sync.Mutex
	calls [][]store.Fence
	alive func([]store.Fence) ([]int64, error)
}

func (f *fakeBeat) beat(_ context.Context, leases []store.Fence) ([]int64, error) {
	f.mu.Lock()
	f.calls = append(f.calls, leases)
	alive := f.alive
	f.mu.Unlock()
	return alive(leases)
}

func (f *fakeBeat) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func allAlive(leases []store.Fence) ([]int64, error) {
	ids := make([]int64, len(leases))
	for i, l := range leases {
		ids[i] = l.JobID
	}
	return ids, nil
}

// canceller records the cause its run was cancelled with.
type canceller struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func newCanceller() canceller {
	ctx, cancel := context.WithCancelCause(context.Background())
	return canceller{ctx, cancel}
}

func (c canceller) cause() error {
	if c.ctx.Err() == nil {
		return nil
	}
	return context.Cause(c.ctx)
}

// AC-4.3: a heartbeat that no longer finds a lease cancels that run, and only that run.
func TestBeatCancelsALostLease(t *testing.T) {
	fb := &fakeBeat{alive: func([]store.Fence) ([]int64, error) { return []int64{1}, nil }}
	k := lease.New(time.Hour, fb.beat, discard)
	kept, lost := newCanceller(), newCanceller()
	k.Hold(store.Fence{JobID: 1, ClaimSeq: 1, Owner: "w"}, 2*time.Hour, kept.cancel)
	k.Hold(store.Fence{JobID: 2, ClaimSeq: 1, Owner: "w"}, 2*time.Hour, lost.cancel)

	k.Beat(t.Context())
	if !errors.Is(lost.cause(), lease.ErrLeaseLost) {
		t.Errorf("lost run's cause = %v, want ErrLeaseLost", lost.cause())
	}
	if kept.cause() != nil {
		t.Errorf("kept run was cancelled: %v", kept.cause())
	}
	if k.Held() != 1 {
		t.Errorf("Held = %d, want 1", k.Held())
	}
	k.Beat(t.Context())
	if got := fb.calls[1]; len(got) != 1 || got[0].JobID != 1 {
		t.Errorf("second beat renewed %v, want only job 1", got)
	}
}

// AC-4.3 through Run: the cancellation lands within one heartbeat interval.
func TestRunCancelsALostLeaseWithinOneInterval(t *testing.T) {
	const interval = 50 * time.Millisecond
	var mu sync.Mutex
	lost := false
	fb := &fakeBeat{alive: func(leases []store.Fence) ([]int64, error) {
		mu.Lock()
		defer mu.Unlock()
		if lost {
			return nil, nil
		}
		return allAlive(leases)
	}}
	k := lease.New(interval, fb.beat, discard)
	run := newCanceller()
	k.Hold(store.Fence{JobID: 1}, time.Second, run.cancel)
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	go k.Run(ctx)

	time.Sleep(3 * interval)
	if run.cause() != nil {
		t.Fatalf("run cancelled while its lease was live: %v", run.cause())
	}
	mu.Lock()
	lost = true
	mu.Unlock()
	start := time.Now()
	<-run.ctx.Done()
	if elapsed := time.Since(start); elapsed > interval+150*time.Millisecond {
		t.Errorf("cancelled %v after the lease was lost, want within one interval (%v) plus scheduling", elapsed, interval)
	}
	if !errors.Is(run.cause(), lease.ErrLeaseLost) {
		t.Errorf("cause = %v, want ErrLeaseLost", run.cause())
	}
}

// When every heartbeat fails, the keeper gives up on a lease after about a
// lease, never before lease minus one interval.
func TestFailingBeatsGiveUpAfterAboutALease(t *testing.T) {
	const interval, leaseLen = 20 * time.Millisecond, 200 * time.Millisecond
	fb := &fakeBeat{alive: func([]store.Fence) ([]int64, error) { return nil, errors.New("database down") }}
	k := lease.New(interval, fb.beat, discard)
	run := newCanceller()
	start := time.Now()
	k.Hold(store.Fence{JobID: 1}, leaseLen, run.cancel)
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	go k.Run(ctx)

	select {
	case <-run.ctx.Done():
	case <-time.After(5 * leaseLen):
		t.Fatal("run never cancelled although every heartbeat failed")
	}
	elapsed := time.Since(start)
	if elapsed < leaseLen-interval {
		t.Errorf("gave up after %v, before lease minus one interval (%v)", elapsed, leaseLen-interval)
	}
	if !errors.Is(run.cause(), lease.ErrHeartbeatsFailing) {
		t.Errorf("cause = %v, want ErrHeartbeatsFailing", run.cause())
	}
	if fb.callCount() < 3 {
		t.Errorf("only %d heartbeats were tried", fb.callCount())
	}
}

// AC-4.1: successful heartbeats keep a run alive well past its lease length.
func TestSuccessfulBeatsKeepTheLease(t *testing.T) {
	const interval, leaseLen = 20 * time.Millisecond, 300 * time.Millisecond
	fb := &fakeBeat{alive: allAlive}
	k := lease.New(interval, fb.beat, discard)
	run := newCanceller()
	k.Hold(store.Fence{JobID: 1}, leaseLen, run.cancel)
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	go k.Run(ctx)
	time.Sleep(5 * leaseLen)
	if run.cause() != nil {
		t.Fatalf("run cancelled although every heartbeat succeeded: %v", run.cause())
	}
}

func TestReleaseStopsRenewalWithoutCancelling(t *testing.T) {
	fb := &fakeBeat{alive: func([]store.Fence) ([]int64, error) { return nil, nil }}
	k := lease.New(time.Hour, fb.beat, discard)
	run := newCanceller()
	release := k.Hold(store.Fence{JobID: 1}, 50*time.Millisecond, run.cancel)
	release()
	release() // idempotent
	k.Beat(t.Context())
	if fb.callCount() != 0 {
		t.Errorf("heartbeat sent with nothing held")
	}
	time.Sleep(100 * time.Millisecond) // past the failsafe
	if run.cause() != nil {
		t.Errorf("released run was cancelled: %v", run.cause())
	}
}

// A lease taken while a heartbeat is in flight is not judged by that heartbeat.
func TestHoldDuringABeatIsNotJudgedByIt(t *testing.T) {
	inBeat, finish := make(chan struct{}), make(chan struct{})
	fb := &fakeBeat{alive: func([]store.Fence) ([]int64, error) {
		close(inBeat)
		<-finish
		return nil, nil // nothing alive
	}}
	k := lease.New(time.Hour, fb.beat, discard)
	first, second := newCanceller(), newCanceller()
	k.Hold(store.Fence{JobID: 1}, time.Hour, first.cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		k.Beat(t.Context())
	}()
	<-inBeat
	k.Hold(store.Fence{JobID: 2}, time.Hour, second.cancel)
	close(finish)
	<-done
	if !errors.Is(first.cause(), lease.ErrLeaseLost) {
		t.Errorf("first cause = %v, want ErrLeaseLost", first.cause())
	}
	if second.cause() != nil {
		t.Errorf("a lease held after the beat started was cancelled: %v", second.cause())
	}
}
