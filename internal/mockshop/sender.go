package mockshop

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

// Webhook is one signed orders/create webhook. Every copy of it, whether a
// retry or a duplicate, carries the same ID, body and signature, as Shopify's
// redeliveries do, so the receiver can dedupe on the ID (Q4).
type Webhook struct {
	ID      string
	Shop    string
	OrderID int64
	Body    []byte
	HMAC    string
}

// OrderBody is the body of an orders/create webhook: a small part of
// Shopify's order payload.
type OrderBody struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
}

// NewWebhook builds and signs a webhook for the shop's order, with a fresh ID
// from crypto/rand (design §12).
func NewWebhook(secret []byte, shop string, orderID int64, now time.Time) (Webhook, error) {
	id, err := newID()
	if err != nil {
		return Webhook{}, err
	}
	body, err := json.Marshal(OrderBody{ID: orderID, Name: fmt.Sprintf("#%d", orderID), CreatedAt: now.UTC().Format(time.RFC3339)})
	if err != nil {
		return Webhook{}, err
	}
	return Webhook{ID: id, Shop: shop, OrderID: orderID, Body: body, HMAC: webhook.Sign(secret, body)}, nil
}

// newID returns a random version 4 UUID, the shape of X-Shopify-Webhook-Id.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mockshop: webhook id: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
}

// SenderStats counts what a sender did.
type SenderStats struct {
	Webhooks   int64 // distinct webhooks emitted
	Duplicates int64 // extra copies sent, later or concurrently
	Attempts   int64 // POSTs made, retries included
	Retries    int64 // attempts after the first, per copy
	Delivered  int64 // copies that got a 2xx
	GaveUp     int64 // copies still failing at the retry horizon
}

// Sender emits webhooks for each shop at its configured rate (design §12).
type Sender struct {
	// Target is the receiver's URL, such as http://api:8080/webhooks/shopify.
	Target string
	Secret []byte
	// Settings holds the retry, timeout and duplicate settings.
	Settings config.Mockshop
	// Shops are the shops to send for; each sends at its WebhooksPerSec.
	Shops []config.Shop
	HTTP  *http.Client
	Log   *slog.Logger
	// Rand decides duplicates and their delays. Nil means a randomly seeded one.
	Rand *mrand.Rand
	// Sleep waits for d or until ctx ends, reporting whether the full wait passed.
	// Nil means a timer. Tests replace it to skip the waits.
	Sleep func(ctx context.Context, d time.Duration) bool
	// Now is the clock the retry horizon is measured by. Nil means time.Now.
	Now func() time.Time

	initOnce sync.Once
	randMu   sync.Mutex
	nextID   atomic.Int64

	webhooks, duplicates, attempts, retries, delivered, gaveUp atomic.Int64
}

func (s *Sender) init() {
	s.initOnce.Do(func() {
		if s.Rand == nil {
			s.Rand = mrand.New(mrand.NewPCG(mrand.Uint64(), mrand.Uint64())) //nolint:gosec // duplicate odds, not secrets
		}
		if s.Sleep == nil {
			s.Sleep = sleepCtx
		}
		if s.Now == nil {
			s.Now = time.Now
		}
		if s.Log == nil {
			s.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
		}
		// Order IDs only need to differ between webhooks; starting from the boot
		// time keeps them apart across restarts too.
		s.nextID.Store(time.Now().UnixMilli())
	})
}

// Stats returns the counts so far.
func (s *Sender) Stats() SenderStats {
	return SenderStats{
		Webhooks: s.webhooks.Load(), Duplicates: s.duplicates.Load(), Attempts: s.attempts.Load(),
		Retries: s.retries.Load(), Delivered: s.delivered.Load(), GaveUp: s.gaveUp.Load(),
	}
}

// Run emits webhooks for every shop with a positive rate until ctx ends, then
// waits for the deliveries in flight to stop.
func (s *Sender) Run(ctx context.Context) {
	s.init()
	var wg sync.WaitGroup
	for _, sh := range s.Shops {
		if sh.WebhooksPerSec <= 0 {
			continue
		}
		every := time.Duration(float64(time.Second) / sh.WebhooksPerSec)
		wg.Go(func() {
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				wg.Go(func() {
					if err := s.Emit(ctx, sh.ID, s.nextID.Add(1)); err != nil && ctx.Err() == nil {
						s.Log.Warn("webhook not emitted", "shop", sh.ID, "err", err.Error())
					}
				})
			}
		})
	}
	wg.Wait()
}

// Emit builds one webhook for the shop's order and delivers it. With the
// configured odds it also sends a second copy at the same time, and schedules
// another after a random delay (design §12). It returns once every copy is
// delivered, given up on, or ctx ends.
func (s *Sender) Emit(ctx context.Context, shop string, orderID int64) error {
	s.init()
	wh, err := NewWebhook(s.Secret, shop, orderID, s.Now())
	if err != nil {
		return err
	}
	s.webhooks.Add(1)
	concurrent, later, delay := s.decide()
	var wg sync.WaitGroup
	if concurrent {
		s.duplicates.Add(1)
		wg.Go(func() { s.Deliver(ctx, wh) })
	}
	if later {
		s.duplicates.Add(1)
		wg.Go(func() {
			if s.Sleep(ctx, delay) {
				s.Deliver(ctx, wh)
			}
		})
	}
	s.Deliver(ctx, wh)
	wg.Wait()
	return nil
}

// decide draws whether this webhook gets a concurrent copy and a later copy,
// and the later copy's delay.
func (s *Sender) decide() (concurrent, later bool, delay time.Duration) {
	s.randMu.Lock()
	defer s.randMu.Unlock()
	concurrent = s.Rand.Float64() < s.Settings.ConcurrentDuplicateProbability
	later = s.Rand.Float64() < s.Settings.DuplicateProbability
	if later && s.Settings.DuplicateDelayMaxMS > 0 {
		delay = time.Duration(s.Rand.Int64N(int64(s.Settings.DuplicateDelayMaxMS)+1)) * time.Millisecond
	}
	return concurrent, later, delay
}

// Outcome is how one copy's delivery ended.
type Outcome struct {
	Delivered bool
	Attempts  int
	// Status is the last answer's status, or 0 if the last attempt got none.
	Status int
}

// Deliver sends one copy of wh until it gets a 2xx. After a non-2xx answer, a
// timeout or a transport error, it waits with exponential backoff, or longer if
// the answer's Retry-After says so, and tries again, until the next attempt
// would start past the retry horizon, counted from the first attempt (design
// §12). The horizon is shorter than the dedupe window (D-018), so every attempt
// maps to the same job.
func (s *Sender) Deliver(ctx context.Context, wh Webhook) Outcome {
	s.init()
	horizon := s.Now().Add(time.Duration(s.Settings.RetryHorizonMS) * time.Millisecond)
	var out Outcome
	for {
		out.Attempts++
		s.attempts.Add(1)
		if out.Attempts > 1 {
			s.retries.Add(1)
		}
		status, retryAfter, err := s.post(ctx, wh)
		out.Status = status
		if err == nil && status >= 200 && status < 300 {
			out.Delivered = true
			s.delivered.Add(1)
			return out
		}
		if ctx.Err() != nil {
			return out
		}
		wait := backoff(s.Settings, out.Attempts)
		if retryAfter > wait {
			wait = retryAfter
		}
		if s.Now().Add(wait).After(horizon) {
			s.gaveUp.Add(1)
			s.Log.Warn("webhook delivery given up at the retry horizon",
				"webhook", wh.ID, "shop", wh.Shop, "attempts", out.Attempts, "last_status", status)
			return out
		}
		if !s.Sleep(ctx, wait) {
			return out
		}
	}
}

// backoff is the wait before attempt n+1: retry_base_ms × 2^(n−1), capped at
// retry_cap_ms.
func backoff(m config.Mockshop, n int) time.Duration {
	d := time.Duration(m.RetryBaseMS) * time.Millisecond
	limit := time.Duration(m.RetryCapMS) * time.Millisecond
	for i := 1; i < n && d < limit; i++ {
		d *= 2
	}
	return min(d, limit)
}

// post makes one delivery attempt. It returns the status and any Retry-After
// the answer carried.
func (s *Sender) post(ctx context.Context, wh Webhook) (int, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.Settings.DeliveryTimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Target, bytes.NewReader(wh.Body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(webhook.HeaderTopic, webhook.TopicOrdersCreate)
	req.Header.Set(webhook.HeaderShopDomain, wh.Shop)
	req.Header.Set(webhook.HeaderWebhookID, wh.ID)
	req.Header.Set(webhook.HeaderHMAC, wh.HMAC)
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	retryAfter, _ := adminapi.ParseRetryAfter(resp.Header.Get(adminapi.HeaderRetryAfter), s.Now())
	return resp.StatusCode, retryAfter, nil
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ErrNoSecret means webhooks would be sent unsigned.
var ErrNoSecret = errors.New("mockshop: SHOPIFY_WEBHOOK_SECRET is required to send webhooks")
