// Package handlers holds the production job handlers. There is one, order.tag
// (D-020): an orders/create webhook, or a loadgen job, becomes an order.tag job,
// whose handler waits its kind's handler_sleep_ms, standing in for I/O, then
// tags the order through mockshop's Admin API with an Idempotency-Key that is
// the same on every run of the job (design §10, AC-7.3).
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/idem"
	"github.com/jenishmsoni45/job-queue/internal/queue"
)

const (
	// OrderTag is the kind of job an orders/create webhook becomes.
	OrderTag = "order.tag"
	// TagStep names the Admin API call, so its Idempotency-Key is
	// idem.Key(job, TagStep): "job-<id>-tag".
	TagStep = "tag"
	// TagValue is the tag the handler adds to the order.
	TagValue = "queued"
)

// OrderTagPayload is the payload of an order.tag job (D-020).
type OrderTagPayload struct {
	OrderID int64 `json:"order_id"`
}

// Register adds every production handler to reg. The handlers call the Admin
// API through admin, and order.tag sleeps its kind's handler_sleep_ms from cfg
// (0 if cfg doesn't list the kind).
func Register(reg *queue.Registry, cfg *config.File, admin *adminapi.Client) {
	var sleep time.Duration
	if k, ok := cfg.Kind(OrderTag); ok {
		sleep = time.Duration(k.HandlerSleepMS) * time.Millisecond
	}
	reg.Register(OrderTag, OrderTagHandler(admin, sleep))
}

// OrderTagHandler returns the order.tag handler: wait sleep, then tag the
// job's order in its shop, once per job however often it runs.
func OrderTagHandler(admin *adminapi.Client, sleep time.Duration) queue.Handler {
	return func(ctx context.Context, run *queue.Run) error {
		var p OrderTagPayload
		dec := json.NewDecoder(bytes.NewReader(run.Job.Payload))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&p); err != nil {
			return queue.Permanent(fmt.Errorf("order.tag: bad payload: %w", err))
		}
		if p.OrderID <= 0 {
			return queue.Permanent(errors.New("order.tag: order_id must be a positive integer"))
		}
		if err := Sleep(ctx, sleep); err != nil {
			return err
		}
		return TagOrder(ctx, admin, run.Job.Shop, run.Job.ID, p.OrderID)
	}
}

// TagOrder tags the order through the Admin API with the job's
// Idempotency-Key, and returns the handler result its answer means (Result).
func TagOrder(ctx context.Context, admin *adminapi.Client, shop string, jobID, orderID int64) error {
	if admin == nil {
		return queue.Permanent(adminapi.ErrNoClient)
	}
	return Result(admin.TagOrder(ctx, shop, orderID, TagValue, idem.Key(jobID, TagStep)))
}

// Result turns an Admin API call's error into a handler result (design §8,
// §14, D-004):
//
//   - nil: success;
//   - 429: queue.Throttled with the Retry-After, so the job waits and its shop
//     pauses without using up an attempt;
//   - any other 4xx: queue.Permanent, since the same request will fail again;
//   - a 5xx, a timeout or no answer (mockshop down): a temporary failure,
//     retried with backoff.
func Result(err error) error {
	if err == nil {
		return nil
	}
	var th *adminapi.ThrottledError
	if errors.As(err, &th) {
		return queue.Throttled(th.RetryAfter)
	}
	var se *adminapi.StatusError
	if errors.As(err, &se) && se.ClientError() {
		return queue.Permanent(err)
	}
	return err
}

// Sleep waits d, stopping early with the context's cause if ctx ends.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}
