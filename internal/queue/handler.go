// Package queue is the handler API (design §11.3) and the worker that runs
// handlers (design §7, §8).
//
// A handler is registered by job kind and called once per run. Delivery is
// at-least-once: a job can run more than once, so handlers must be idempotent.
// A handler's result decides the job's next state, and the worker writes it
// with a fenced write, so a run that lost its lease changes nothing (Q2).
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/jenishmsoni45/job-queue/internal/idem"
)

// Handler runs one job. It returns nil on success, Permanent(err) to send the
// job straight to dead, or any other error for a temporary failure that is
// retried with backoff (design §8). It must stop promptly when ctx is cancelled:
// that means the lease was lost, the hard timeout passed, or the worker is
// stopping.
type Handler func(ctx context.Context, run *Run) error

// Job is what a handler sees of its job.
type Job struct {
	ID       int64
	Shop     string
	Kind     string
	Payload  json.RawMessage
	ClaimSeq int32 // the fencing token of this run (D-004)
	Attempts int32 // failed attempts before this run
}

// Run is one run of a job.
type Run struct {
	Job Job

	db beginner

	mu        sync.Mutex
	tx        pgx.Tx // begun on the first call to Tx
	finished  bool   // the handler returned and the worker owns tx
	abandoned bool   // the worker gave up on the handler; it rolls tx back itself
}

type beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ErrRunOver is returned by Run.Tx after the handler has returned or the worker
// has given up on it.
var ErrRunOver = errors.New("queue: the run is over")

// ErrManagedTx is returned when a handler tries to commit or roll back the
// transaction from Run.Tx. The worker does that.
var ErrManagedTx = errors.New("queue: run.Tx() is committed or rolled back by the worker, not the handler")

// Tx returns the run's transaction, beginning it on the first call. Writes made
// in it commit only together with the fenced completion when the handler
// returns nil, and roll back otherwise, including when the lease is lost (design
// §10, AC-7.2). The handler must not commit or roll it back.
func (r *Run) Tx(ctx context.Context) (pgx.Tx, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || r.abandoned {
		return nil, ErrRunOver
	}
	if r.tx == nil {
		tx, err := r.db.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("queue: begin run transaction: %w", err)
		}
		r.tx = tx
	}
	return managedTx{r.tx}, nil
}

// Step runs one step of the handler that commits on its own (design §10,
// D-017). It begins a transaction, records idem.Once(job, name) in it, runs fn,
// and commits fn's writes and the record together. If an earlier run of this
// job already committed the step, Step skips fn and returns nil, so the step's
// effect is applied at most once per job, even when runs overlap (AC-7.1).
//
// fn must not commit or roll back tx; Step does that. An error from fn rolls
// the step back and is returned. Use Step for effects that must stick even if
// this run later fails or loses its lease. Effects that should commit only
// with the job's completion belong in Tx instead.
func (r *Run) Step(ctx context.Context, name string, fn func(ctx context.Context, tx pgx.Tx) error) error {
	if err := idem.ValidStep(name); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		first, err := idem.Once(ctx, tx, r.Job.ID, name)
		if err != nil || !first {
			return err
		}
		if err := fn(ctx, managedTx{tx}); err != nil {
			return fmt.Errorf("queue: step %q: %w", name, err)
		}
		return nil
	})
}

// finish is called by the handler goroutine when the handler returns. It
// reports whether the worker still wants the result; if the worker abandoned the
// run, finish rolls the transaction back instead.
func (r *Run) finish(ctx context.Context) bool {
	r.mu.Lock()
	if r.abandoned {
		tx := r.tx
		r.tx = nil
		r.mu.Unlock()
		if tx != nil {
			_ = tx.Rollback(ctx)
		}
		return false
	}
	r.finished = true
	r.mu.Unlock()
	return true
}

// abandon gives up on a handler that is still running. It returns false if the
// handler has already finished, in which case the worker owns the result.
func (r *Run) abandon() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return false
	}
	r.abandoned = true
	return true
}

// takeTx hands the run's transaction, if any, to the worker. It returns nil
// for an abandoned run, whose handler goroutine may still be using the
// transaction and rolls it back itself when it returns.
func (r *Run) takeTx() pgx.Tx {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.abandoned {
		return nil
	}
	tx := r.tx
	r.tx = nil
	return tx
}

// managedTx is the handler's view of the run transaction: everything but
// Commit and Rollback.
type managedTx struct{ pgx.Tx }

func (managedTx) Commit(context.Context) error   { return ErrManagedTx }
func (managedTx) Rollback(context.Context) error { return ErrManagedTx }

// permanentError marks a failure that must not be retried.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so that the job goes straight to dead with reason
// "permanent" (AC-5.3).
func Permanent(err error) error {
	if err == nil {
		err = errors.New("permanent failure")
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was made by Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// Registry maps job kinds to handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

// Register adds the handler for kind. Registering a kind twice, or a nil
// handler, is a programming error and panics.
func (r *Registry) Register(kind string, h Handler) {
	if h == nil {
		panic("queue: nil handler for kind " + kind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[kind]; dup {
		panic("queue: kind registered twice: " + kind)
	}
	r.handlers[kind] = h
}

// Lookup returns the handler for kind.
func (r *Registry) Lookup(kind string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[kind]
	return h, ok
}

// Kinds returns the registered kinds, sorted.
func (r *Registry) Kinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	kinds := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		kinds = append(kinds, k)
	}
	slices.Sort(kinds)
	return kinds
}
