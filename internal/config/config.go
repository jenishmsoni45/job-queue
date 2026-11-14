// Package config loads and validates the shop and job-kind file, config/shops.json
// (design §11.4, D-019). Every process validates it at startup and fails fast; the
// api role then seeds the shops, shop_state and job_kinds tables from it.
//
// The same file carries the dedupe window, each shop's mockshop settings, the
// mockshop webhook sender's settings and, for the bench, the loadgen settings, so
// that every process can check them against each other (D-050).
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"time"
)

// File is the whole config file.
type File struct {
	// DedupeWindowMS is how long a dedupe key is kept (D-018: 7 days). The
	// pruner deletes older keys, and mockshop's retry horizon must be shorter.
	DedupeWindowMS int64    `json:"dedupe_window_ms"`
	Shops          []Shop   `json:"shops"`
	Kinds          []Kind   `json:"kinds"`
	Mockshop       Mockshop `json:"mockshop"`
	// Loadgen is the bench load (design §17). Only bench files have it, and
	// loadgen refuses a file without it.
	Loadgen *Loadgen `json:"loadgen,omitempty"`
}

// Shop is one tenant and its limits.
type Shop struct {
	ID                  string  `json:"id"`
	Weight              int     `json:"weight"`
	MaxConcurrency      int     `json:"max_concurrency"`
	RatePerSec          float64 `json:"rate_per_sec"`
	RateBurst           int     `json:"rate_burst"`
	BacklogLimit        int     `json:"backlog_limit"`
	BacklogRetryAfterMS int     `json:"backlog_retry_after_ms"`
	// MockAPICapacity and MockAPILeakPerSec are the shop's Admin API leaky
	// bucket in mockshop (design §12, D-010).
	MockAPICapacity   int     `json:"mock_api_capacity"`
	MockAPILeakPerSec float64 `json:"mock_api_leak_per_sec"`
	// WebhooksPerSec is how many orders/create webhooks mockshop sends for the
	// shop each second; 0 sends none (D-054).
	WebhooksPerSec float64 `json:"webhooks_per_sec"`
}

// Mockshop is the mockshop webhook sender's settings (design §12).
type Mockshop struct {
	// RetryHorizonMS is how long the sender keeps retrying one delivery,
	// counted from its first attempt. It must be shorter than the dedupe window,
	// so every redelivery maps to the job the first one created (D-018).
	RetryHorizonMS int64 `json:"retry_horizon_ms"`
	// RetryBaseMS and RetryCapMS shape the exponential backoff between retries.
	// A Retry-After header longer than the backoff wins.
	RetryBaseMS int `json:"retry_base_ms"`
	RetryCapMS  int `json:"retry_cap_ms"`
	// DeliveryTimeoutMS bounds one delivery attempt; a timeout is retried.
	DeliveryTimeoutMS int `json:"delivery_timeout_ms"`
	// DuplicateProbability is the chance a webhook is sent again later, after
	// a uniform delay of up to DuplicateDelayMaxMS.
	DuplicateProbability float64 `json:"duplicate_probability"`
	DuplicateDelayMaxMS  int     `json:"duplicate_delay_max_ms"`
	// ConcurrentDuplicateProbability is the chance a webhook is sent twice at once.
	ConcurrentDuplicateProbability float64 `json:"concurrent_duplicate_probability"`
}

// Loadgen is the Black Friday load (design §17, D-009).
type Loadgen struct {
	// Seed seeds every shop's arrival schedule, so both runs get the same one.
	Seed uint64 `json:"seed"`
	// TickMS is how often loadgen sends: each tick's arrivals for a shop go as
	// one batch.
	TickMS int `json:"tick_ms"`
	// RatePerShop is every shop's steady rate, in jobs per second.
	RatePerShop float64 `json:"rate_per_shop"`
	// WarmupMS is how long every shop sends at the steady rate first.
	WarmupMS int `json:"warmup_ms"`
	// SpikeShop sends SpikeRate jobs per second for SpikeMS after the warm-up,
	// while the other shops stay at the steady rate.
	SpikeShop string  `json:"spike_shop"`
	SpikeRate float64 `json:"spike_rate"`
	SpikeMS   int     `json:"spike_ms"`
	// DrainMaxMS bounds the steady tail after the spike, which otherwise lasts
	// until every job accepted before the spike ended is terminal (D-056).
	DrainMaxMS int `json:"drain_max_ms"`
}

// Kind is one job kind and its settings.
type Kind struct {
	Kind           string `json:"kind"`
	Cost           int    `json:"cost"`
	LeaseMS        int    `json:"lease_ms"`
	HeartbeatMS    int    `json:"heartbeat_ms"`
	TimeoutMS      int    `json:"timeout_ms"`
	MaxAttempts    int    `json:"max_attempts"`
	MaxThrottles   int    `json:"max_throttles"`
	HandlerSleepMS int    `json:"handler_sleep_ms"`
}

// maxFileSize bounds the config file, which is a small, hand-written JSON file.
const maxFileSize = 1 << 20

var (
	// A shop ID is a lowercase domain-like name, such as shop-01.mockshop.test.
	shopIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$`)
	// A kind is a lowercase dotted name, such as order.tag or test.sleep.
	kindPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)
)

// Load reads, parses and validates the file at path.
func Load(path string) (*File, error) {
	f, err := os.Open(path) //nolint:gosec // the path is the operator's own config setting
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	defer func() { _ = f.Close() }()
	cfg, err := Parse(io.LimitReader(f, maxFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes and validates a config file. Unknown fields are errors, so a
// typo in a setting name fails fast instead of silently using a default.
func Parse(r io.Reader) (*File, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(data) > maxFileSize {
		return nil, fmt.Errorf("file is larger than %d bytes", maxFileSize)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg File
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("parse: unexpected data after the top-level object")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate reports every problem in the file at once.
func (c *File) Validate() error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.DedupeWindowMS < 1 {
		bad("dedupe_window_ms must be positive, got %d", c.DedupeWindowMS)
	}
	if len(c.Shops) == 0 {
		bad("no shops")
	}
	seenShops := map[string]bool{}
	for i, s := range c.Shops {
		at := fmt.Sprintf("shops[%d] (%q)", i, s.ID)
		if !shopIDPattern.MatchString(s.ID) {
			bad("%s: id must be a lowercase domain-like name", at)
		}
		if seenShops[s.ID] {
			bad("%s: duplicate id", at)
		}
		seenShops[s.ID] = true
		positive(&errs, at, "weight", s.Weight)
		positive(&errs, at, "max_concurrency", s.MaxConcurrency)
		positiveRate(&errs, at, "rate_per_sec", s.RatePerSec)
		positive(&errs, at, "rate_burst", s.RateBurst)
		positive(&errs, at, "backlog_limit", s.BacklogLimit)
		positive(&errs, at, "backlog_retry_after_ms", s.BacklogRetryAfterMS)
		positive(&errs, at, "mock_api_capacity", s.MockAPICapacity)
		positiveRate(&errs, at, "mock_api_leak_per_sec", s.MockAPILeakPerSec)
		if !(s.WebhooksPerSec >= 0) || math.IsInf(s.WebhooksPerSec, 0) {
			bad("%s: webhooks_per_sec must be a number of at least 0", at)
		}
	}

	if len(c.Kinds) == 0 {
		bad("no kinds")
	}
	seenKinds := map[string]bool{}
	for i, k := range c.Kinds {
		at := fmt.Sprintf("kinds[%d] (%q)", i, k.Kind)
		if len(k.Kind) > 100 || !kindPattern.MatchString(k.Kind) {
			bad("%s: kind must be a lowercase dotted name such as order.tag", at)
		}
		if seenKinds[k.Kind] {
			bad("%s: duplicate kind", at)
		}
		seenKinds[k.Kind] = true
		positive(&errs, at, "cost", k.Cost)
		positive(&errs, at, "lease_ms", k.LeaseMS)
		positive(&errs, at, "heartbeat_ms", k.HeartbeatMS)
		if k.HeartbeatMS >= k.LeaseMS {
			bad("%s: heartbeat_ms (%d) must be shorter than lease_ms (%d)", at, k.HeartbeatMS, k.LeaseMS)
		}
		positive(&errs, at, "timeout_ms", k.TimeoutMS)
		positive(&errs, at, "max_attempts", k.MaxAttempts)
		nonNegative(&errs, at, "max_throttles", k.MaxThrottles)
		nonNegative(&errs, at, "handler_sleep_ms", k.HandlerSleepMS)
	}

	m := c.Mockshop
	if m.RetryHorizonMS < 1 {
		bad("mockshop: retry_horizon_ms must be positive, got %d", m.RetryHorizonMS)
	} else if m.RetryHorizonMS >= c.DedupeWindowMS {
		// D-018: a redelivery after the window would create a second job (Q4).
		bad("mockshop: retry_horizon_ms (%d) must be shorter than dedupe_window_ms (%d)", m.RetryHorizonMS, c.DedupeWindowMS)
	}
	positive(&errs, "mockshop", "retry_base_ms", m.RetryBaseMS)
	positive(&errs, "mockshop", "retry_cap_ms", m.RetryCapMS)
	if m.RetryCapMS < m.RetryBaseMS {
		bad("mockshop: retry_cap_ms (%d) must be at least retry_base_ms (%d)", m.RetryCapMS, m.RetryBaseMS)
	}
	positive(&errs, "mockshop", "delivery_timeout_ms", m.DeliveryTimeoutMS)
	probability(&errs, "mockshop", "duplicate_probability", m.DuplicateProbability)
	nonNegative(&errs, "mockshop", "duplicate_delay_max_ms", m.DuplicateDelayMaxMS)
	probability(&errs, "mockshop", "concurrent_duplicate_probability", m.ConcurrentDuplicateProbability)

	if l := c.Loadgen; l != nil {
		positive(&errs, "loadgen", "tick_ms", l.TickMS)
		positiveRate(&errs, "loadgen", "rate_per_shop", l.RatePerShop)
		positive(&errs, "loadgen", "warmup_ms", l.WarmupMS)
		if !seenShops[l.SpikeShop] {
			bad("loadgen: spike_shop %q is not one of the shops", l.SpikeShop)
		}
		positiveRate(&errs, "loadgen", "spike_rate", l.SpikeRate)
		positive(&errs, "loadgen", "spike_ms", l.SpikeMS)
		positive(&errs, "loadgen", "drain_max_ms", l.DrainMaxMS)
	}
	return errors.Join(errs...)
}

// positiveRate checks that v is a positive, finite number.
func positiveRate(errs *[]error, at, field string, v float64) {
	if !(v > 0) || math.IsInf(v, 0) {
		*errs = append(*errs, fmt.Errorf("%s: %s must be a positive number", at, field))
	}
}

// probability checks that v is in [0, 1].
func probability(errs *[]error, at, field string, v float64) {
	if !(v >= 0 && v <= 1) {
		*errs = append(*errs, fmt.Errorf("%s: %s must be between 0 and 1, got %v", at, field, v))
	}
}

// DedupeWindow returns how long dedupe keys are kept (D-018).
func (c *File) DedupeWindow() time.Duration {
	return time.Duration(c.DedupeWindowMS) * time.Millisecond
}

// Shop returns the shop with the ID.
func (c *File) Shop(id string) (Shop, bool) {
	for _, s := range c.Shops {
		if s.ID == id {
			return s, true
		}
	}
	return Shop{}, false
}

// Kind returns the kind's settings.
func (c *File) Kind(kind string) (Kind, bool) {
	for _, k := range c.Kinds {
		if k.Kind == kind {
			return k, true
		}
	}
	return Kind{}, false
}

// positive checks that v is in [1, MaxInt32], the range of a Postgres int.
func positive(errs *[]error, at, field string, v int) {
	if v < 1 || v > math.MaxInt32 {
		*errs = append(*errs, fmt.Errorf("%s: %s must be between 1 and %d, got %d", at, field, math.MaxInt32, v))
	}
}

// nonNegative checks that v is in [0, MaxInt32].
func nonNegative(errs *[]error, at, field string, v int) {
	if v < 0 || v > math.MaxInt32 {
		*errs = append(*errs, fmt.Errorf("%s: %s must be between 0 and %d, got %d", at, field, math.MaxInt32, v))
	}
}

// MinHeartbeat returns the shortest heartbeat interval of any kind. A worker
// sends its one batched heartbeat at this interval, so every kind's lease is
// renewed at least as often as that kind asks for (design §7).
func (c *File) MinHeartbeat() time.Duration {
	minMS := 0
	for _, k := range c.Kinds {
		if minMS == 0 || k.HeartbeatMS < minMS {
			minMS = k.HeartbeatMS
		}
	}
	return time.Duration(minMS) * time.Millisecond
}

// MaxCost returns the largest job cost of any kind. It is the DRR base quantum
// (design §2): with it, a shop's first job always fits in the first turn it gets.
func (c *File) MaxCost() int {
	m := 1
	for _, k := range c.Kinds {
		m = max(m, k.Cost)
	}
	return m
}
