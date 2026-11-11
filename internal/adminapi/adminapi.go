// Package adminapi is the handlers' client for mockshop's Admin API (design
// §12): it tags an order, sending an Idempotency-Key, and turns the answer into
// an error a handler can map to a queue result. It never calls the real Shopify.
package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Headers the Admin API uses.
const (
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderCallLimit      = "X-Shopify-Shop-Api-Call-Limit"
	HeaderRetryAfter     = "Retry-After"
)

// DefaultRetryAfter is the wait a 429 implies when it carries no usable
// Retry-After header.
const DefaultRetryAfter = time.Second

// maxErrorBody bounds how much of an error answer is kept in a StatusError.
const maxErrorBody = 512

// TagPath is the Admin API path that tags one order of a shop.
func TagPath(shop string, orderID int64) string {
	return "/admin/api/mock/shops/" + url.PathEscape(shop) + "/orders/" + strconv.FormatInt(orderID, 10) + "/tags"
}

// TagRequest is the body of a tag call.
type TagRequest struct {
	Tag string `json:"tag"`
}

// ThrottledError means the Admin API answered 429: the shop's bucket is full.
// RetryAfter is how long it said to wait (design §12, D-004).
type ThrottledError struct {
	RetryAfter time.Duration
}

func (e *ThrottledError) Error() string {
	return fmt.Sprintf("admin API throttled the call; retry after %s", e.RetryAfter)
}

// StatusError is any other answer that wasn't 2xx.
type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("admin API answered %d: %s", e.Status, e.Body)
}

// ClientError reports whether the answer was a 4xx: the request itself is
// wrong, so sending it again won't help.
func (e *StatusError) ClientError() bool { return e.Status >= 400 && e.Status < 500 }

// Client calls the Admin API at BaseURL, such as http://mockshop:8090.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	// Now reads the clock for an HTTP-date Retry-After. Nil means time.Now.
	Now func() time.Time
}

// TagOrder adds tag to the shop's order, sending idempotencyKey (if not empty)
// as the Idempotency-Key header, so a repeated call is applied once (design §10,
// AC-7.3). It returns nil on 2xx, a *ThrottledError on 429, a *StatusError on
// any other answer, and the transport's error if there was no answer.
func (c *Client) TagOrder(ctx context.Context, shop string, orderID int64, tag, idempotencyKey string) error {
	body, err := json.Marshal(TagRequest{Tag: tag})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+TagPath(shop, orderID), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("adminapi: build the request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set(HeaderIdempotencyKey, idempotencyKey)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("adminapi: tag order %d of %s: %w", orderID, shop, err)
	}
	defer func() { _ = resp.Body.Close() }()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)) // let the connection be reused
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests:
		now := time.Now
		if c.Now != nil {
			now = c.Now
		}
		d, ok := ParseRetryAfter(resp.Header.Get(HeaderRetryAfter), now())
		if !ok {
			d = DefaultRetryAfter
		}
		return &ThrottledError{RetryAfter: d}
	default:
		return &StatusError{Status: resp.StatusCode, Body: strings.TrimSpace(string(snippet))}
	}
}

// maxRetryAfter bounds a parsed Retry-After, so a nonsense header can't pause a
// shop for years or overflow a duration.
const maxRetryAfter = 24 * time.Hour

// ParseRetryAfter reads a Retry-After header: decimal seconds, as mockshop
// sends it ("0.25", "2"), or an HTTP date. It reports false for anything else,
// or a negative wait. A date in the past is a wait of 0.
func ParseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if math.IsNaN(secs) || math.IsInf(secs, 0) || secs < 0 || strings.ContainsAny(v, "eExXpP") {
			return 0, false
		}
		d := time.Duration(math.Ceil(secs * float64(time.Second)))
		return min(d, maxRetryAfter), true
	}
	if t, err := http.ParseTime(v); err == nil {
		return min(max(0, t.Sub(now)), maxRetryAfter), true
	}
	return 0, false
}

// ErrNoClient is returned by a handler that needs the Admin API but has no client.
var ErrNoClient = errors.New("adminapi: no Admin API client configured")
