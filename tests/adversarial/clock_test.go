package adversarial

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/clock"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/queue"
	"github.com/jenishmsoni45/job-queue/internal/queuetest"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/testkinds"
)

// skewed is a worker clock a fixed offset from the real one, with Go's
// monotonic reading stripped, so differences between its readings use the
// skewed wall time.
type skewed struct{ offset time.Duration }

func (s skewed) Now() time.Time { return time.Now().Round(0).Add(s.offset) }

// jumping is a worker clock that jumps an hour forward on every reading, like a
// wall clock stepped by a bad NTP sync.
type jumping struct {
	mu    sync.Mutex
	jumps time.Duration
}

func (j *jumping) Now() time.Time {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.jumps += time.Hour
	return time.Now().Round(0).Add(j.jumps)
}

// outcome is what a scenario's lease decisions came to.
type outcome struct {
	Outcome  queue.Outcome
	State    string
	ClaimSeq int32
	Attempts int32
}

// P2-10, NFR-14: the worker never uses its own clock for a lease decision. The
// same scenarios run with the worker's clock correct, skewed by an hour either
// way, ten years ahead, and jumping an hour on every reading. The lease
// outcomes are identical, because the database clock decides every lease.
func TestSkewedWorkerClockChangesNoLeaseOutcome(t *testing.T) {
	t.Parallel()
	clocks := map[string]func() clock.Clock{
		"correct":       func() clock.Clock { return clock.System{} },
		"one hour fast": func() clock.Clock { return skewed{time.Hour} },
		"one hour slow": func() clock.Clock { return skewed{-time.Hour} },
		"ten years":     func() clock.Clock { return skewed{10 * 365 * 24 * time.Hour} },
		"jumping":       func() clock.Clock { return &jumping{} },
	}
	scenarios := map[string]func(*testing.T, clock.Clock) outcome{
		"heartbeats keep a long run alive": longRun,
		"a lost lease cancels the run":     lostLease,
		"a stale completion is fenced out": staleCompletion,
	}
	for sname, scenario := range scenarios {
		t.Run(sname, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			got := map[string]outcome{}
			var wg sync.WaitGroup
			for cname, mk := range clocks {
				wg.Go(func() {
					t.Run(cname, func(t *testing.T) {
						o := scenario(t, mk())
						mu.Lock()
						got[cname] = o
						mu.Unlock()
					})
				})
			}
			wg.Wait()
			want, ok := got["correct"]
			if !ok {
				t.Fatal("the correct-clock run did not finish")
			}
			for cname, o := range got {
				if o != want {
					t.Errorf("clock %s: %+v, want the same as the correct clock: %+v", cname, o, want)
				}
			}
		})
	}
}

func env(t *testing.T) *queuetest.Env {
	return queuetest.New(t, queuetest.Config(t, func(c *config.File) {
		queuetest.SetKind(c, testkinds.Sleep, func(k *config.Kind) { k.LeaseMS, k.HeartbeatMS = 1000, 200 })
	}))
}

func startWorker(t *testing.T, e *queuetest.Env, clk clock.Clock, h queue.Handler, opts ...queuetest.WorkerOptions) *queuetest.Reports {
	reg := queue.NewRegistry()
	reg.Register(testkinds.Sleep, h)
	_, reports := e.StartWorker(t, reg, append(opts, func(c *queue.Config) { c.Clock = clk })...)
	return reports
}

func result(t *testing.T, e *queuetest.Env, id int64, rep queue.Report, clk clock.Clock) outcome {
	t.Helper()
	// The skewed clock did reach the worker: its report is stamped with it.
	if s, ok := clk.(skewed); ok {
		if off := time.Until(rep.Started); off < s.offset-time.Minute || off > s.offset+time.Minute {
			t.Errorf("report stamped %v from now, want about %v: the clock was not injected", off, s.offset)
		}
	}
	j := e.Job(t, id)
	return outcome{Outcome: rep.Outcome, State: j.State, ClaimSeq: j.ClaimSeq, Attempts: j.Attempts}
}

// A run three leases long completes on its first claim, with a reaper running.
func longRun(t *testing.T, clk clock.Clock) outcome {
	e := env(t)
	reports := startWorker(t, e, clk, func(ctx context.Context, _ *queue.Run) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
			return nil
		}
	})
	stop := reaper(t, e)
	defer stop()
	id := e.Enqueue(t, queuetest.ShopA, testkinds.Sleep, nil)
	return result(t, e, id, reports.Next(t, 10*time.Second), clk)
}

// A lease that expires by the database clock cancels the run, and the worker
// writes nothing.
func lostLease(t *testing.T, clk clock.Clock) outcome {
	e := env(t)
	running := make(chan struct{})
	reports := startWorker(t, e, clk, func(ctx context.Context, _ *queue.Run) error {
		close(running)
		<-ctx.Done()
		return nil
	})
	id := e.Enqueue(t, queuetest.ShopA, testkinds.Sleep, nil)
	<-running
	if _, err := e.Pool.Exec(t.Context(), `UPDATE jobs SET lease_expires_at = statement_timestamp() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	return result(t, e, id, reports.Next(t, 10*time.Second), clk)
}

// A worker that believes its lease is live (its heartbeats report success but
// renew nothing) returns after the lease expired; its completion affects 0 rows.
func staleCompletion(t *testing.T, clk clock.Clock) outcome {
	e := env(t)
	reports := startWorker(t, e, clk, func(context.Context, *queue.Run) error {
		time.Sleep(1500 * time.Millisecond) // past the lease
		return nil
	}, func(c *queue.Config) {
		c.Heartbeat = func(_ context.Context, leases []store.Fence) ([]int64, error) {
			ids := make([]int64, len(leases))
			for i, l := range leases {
				ids[i] = l.JobID
			}
			return ids, nil
		}
	})
	id := e.Enqueue(t, queuetest.ShopA, testkinds.Sleep, nil)
	return result(t, e, id, reports.Next(t, 10*time.Second), clk)
}

func reaper(t *testing.T, e *queuetest.Env) func() {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			_, _ = store.Reap(ctx, e.Pool, 500)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	return func() { cancel(); <-done }
}

// NFR-14 by construction: the worker packages read no clock except the
// injected one. Any call to time.Now, time.Since or time.Until in them would
// be a way around the skewed clock above.
func TestWorkerPackagesReadNoWallClock(t *testing.T) {
	root := queuetest.RepoRoot(t)
	forbidden := map[string]bool{"Now": true, "Since": true, "Until": true}
	for _, dir := range []string{"internal/queue", "internal/lease", "internal/store"} {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		files := 0
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			files++
			path := filepath.Join(root, dir, name)
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			timeName := ""
			for _, imp := range f.Imports {
				if p, _ := strconv.Unquote(imp.Path.Value); p == "time" {
					timeName = "time"
					if imp.Name != nil {
						timeName = imp.Name.Name
					}
				}
			}
			if timeName == "" {
				continue
			}
			ast.Inspect(f, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if x, ok := sel.X.(*ast.Ident); ok && x.Name == timeName && forbidden[sel.Sel.Name] {
					t.Errorf("%s: calls %s.%s; worker code reads time only through clock.Clock (NFR-14)",
						fmt.Sprint(fset.Position(sel.Pos())), timeName, sel.Sel.Name)
				}
				return true
			})
		}
		if files == 0 {
			t.Errorf("no Go files found in %s", dir)
		}
	}
}
