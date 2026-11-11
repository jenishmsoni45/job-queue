// Package config loads and validates the shop and job-kind file, config/shops.json
// (design §11.4, D-019). Every process validates it at startup and fails fast; the
// api role then seeds the shops, shop_state and job_kinds tables from it.
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
	Shops []Shop `json:"shops"`
	Kinds []Kind `json:"kinds"`
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
		if !(s.RatePerSec > 0) || math.IsInf(s.RatePerSec, 0) {
			bad("%s: rate_per_sec must be a positive number", at)
		}
		positive(&errs, at, "rate_burst", s.RateBurst)
		positive(&errs, at, "backlog_limit", s.BacklogLimit)
		positive(&errs, at, "backlog_retry_after_ms", s.BacklogRetryAfterMS)
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
	return errors.Join(errs...)
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
