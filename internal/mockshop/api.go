package mockshop

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/adminapi"
	"github.com/jenishmsoni45/job-queue/internal/config"
)

// Paths the API serves.
const (
	tagRoute = "POST /admin/api/mock/shops/{shop}/orders/{order_id}/tags"
	// MutationsPath returns a shop's applied mutation count per Idempotency-Key
	// (GET /mock/v1/mutations?shop=), which feeds NFR-11.
	MutationsPath = "/mock/v1/mutations"
	// HeaderReplayed marks an answer replayed for a repeated Idempotency-Key.
	HeaderReplayed = "Idempotent-Replayed"
)

const (
	maxTagBody = 64 << 10
	maxTagLen  = 255
)

// API is the simulated Admin API (design §12).
type API struct {
	now   func() time.Time
	shops map[string]*shop
}

// shop is one shop's state. mu guards all of it, so a shop's bucket check,
// idempotency check and mutation happen as one step: of two concurrent calls
// with the same key, exactly one applies (AC-7.3).
type shop struct {
	mu      sync.Mutex
	bucket  *Bucket
	stored  map[string]storedAnswer // by Idempotency-Key: the answer to the call that applied
	applied map[string]int          // by Idempotency-Key: mutations applied
	unkeyed int                     // mutations applied by calls without a key
	replays int
	orders  map[int64][]string // tags per order
}

type storedAnswer struct {
	status int
	body   []byte
}

// NewAPI returns the Admin API for the config's shops, each with its own
// bucket (D-010). now is the clock buckets drain by; nil means time.Now.
func NewAPI(cfg *config.File, now func() time.Time) *API {
	if now == nil {
		now = time.Now
	}
	a := &API{now: now, shops: make(map[string]*shop, len(cfg.Shops))}
	t := now()
	for _, s := range cfg.Shops {
		a.shops[s.ID] = &shop{
			bucket:  NewBucket(s.MockAPICapacity, s.MockAPILeakPerSec, t),
			stored:  map[string]storedAnswer{},
			applied: map[string]int{},
			orders:  map[int64][]string{},
		}
	}
	return a
}

// Register adds the API's routes to mux.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc(tagRoute, a.tag)
	mux.HandleFunc("GET "+MutationsPath, a.mutations)
}

type orderAnswer struct {
	Order struct {
		ID   int64    `json:"id"`
		Tags []string `json:"tags"`
	} `json:"order"`
}

// tag answers POST /admin/api/mock/shops/{shop}/orders/{order_id}/tags with
// body {tag}. Every call to a known shop costs one slot in its bucket; a full
// bucket answers 429 with Retry-After in decimal seconds. With an
// Idempotency-Key, the first call that gets through applies the tag and its
// answer is stored; a repeat replays that answer and applies nothing. Every
// answer for a known shop carries X-Shopify-Shop-Api-Call-Limit.
func (a *API) tag(w http.ResponseWriter, r *http.Request) {
	s, ok := a.shops[r.PathValue("shop")]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"errors": "Not Found"})
		return
	}
	orderID, err := strconv.ParseInt(r.PathValue("order_id"), 10, 64)
	if err != nil || orderID <= 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"errors": "Not Found"})
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxTagBody))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"errors": "body too large"})
		return
	}
	key := r.Header.Get(adminapi.HeaderIdempotencyKey)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := a.now()
	allowed, wait := s.bucket.Take(now)
	w.Header().Set(adminapi.HeaderCallLimit, s.bucket.CallLimit(now))
	if !allowed {
		w.Header().Set(adminapi.HeaderRetryAfter, strconv.FormatFloat(wait.Seconds(), 'f', 3, 64))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"errors": "Exceeded the API call limit for this shop. Retry after the time in Retry-After.",
		})
		return
	}
	if key != "" {
		if prev, ok := s.stored[key]; ok {
			s.replays++
			w.Header().Set(HeaderReplayed, "true")
			writeRaw(w, prev.status, prev.body)
			return
		}
	}
	var req adminapi.TagRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || req.Tag == "" || len(req.Tag) > maxTagLen {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"errors": "body must be {\"tag\": \"...\"}"})
		return
	}

	// Apply the mutation and log it.
	tags := s.orders[orderID]
	if !slices.Contains(tags, req.Tag) {
		tags = append(tags, req.Tag)
		s.orders[orderID] = tags
	}
	if key == "" {
		s.unkeyed++
	} else {
		s.applied[key]++
	}
	var ans orderAnswer
	ans.Order.ID, ans.Order.Tags = orderID, slices.Clone(tags)
	body, _ := json.Marshal(ans)
	if key != "" {
		s.stored[key] = storedAnswer{status: http.StatusOK, body: body}
	}
	writeRaw(w, http.StatusOK, body)
}

// Mutations is one shop's mutation log: how many mutations each
// Idempotency-Key applied, how many calls without a key applied one, and how
// many repeats were replayed.
type Mutations struct {
	Shop    string         `json:"shop"`
	Applied map[string]int `json:"applied"`
	Unkeyed int            `json:"unkeyed"`
	Replays int            `json:"replays"`
}

// Mutations returns a copy of the shop's mutation log.
func (a *API) Mutations(shopID string) (Mutations, bool) {
	s, ok := a.shops[shopID]
	if !ok {
		return Mutations{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	applied := make(map[string]int, len(s.applied))
	for k, n := range s.applied {
		applied[k] = n
	}
	return Mutations{Shop: shopID, Applied: applied, Unkeyed: s.unkeyed, Replays: s.replays}, true
}

// mutations answers GET /mock/v1/mutations?shop=: 200 with the shop's log,
// 404 for an unknown shop, 422 without a shop.
func (a *API) mutations(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("shop")
	if id == "" {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"errors": "shop is required"})
		return
	}
	m, ok := a.Mutations(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"errors": "Not Found"})
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, _ := json.Marshal(v)
	writeRaw(w, status, body)
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func itoa(n int) string { return strconv.Itoa(n) }
