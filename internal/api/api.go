// Package api is the api role's job endpoints (design §11.1): enqueue one job,
// or a batch of jobs for one shop, with optional dedupe keys; read one job; and
// receive Shopify webhooks, each checked by HMAC and deduped on its webhook ID.
// An enqueue that would pass the shop's backlog limit is answered with 429 and a
// Retry-After header, and creates nothing (D-007). The read API and the DLQ
// arrive in phase 6.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/admission"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

// MaxBodyBytes is the max request body (design §2: 1 MiB).
const MaxBodyBytes = 1 << 20

// MaxBatchSize is the most jobs one batch holds (design §2, D-030).
const MaxBatchSize = 1000

const (
	maxShopLen = 255
	maxKindLen = 100
	// MaxDedupeKeyLen is the longest dedupe key (D-045).
	MaxDedupeKeyLen = 255
)

// Server serves the job endpoints.
type Server struct {
	db            store.DB
	log           *slog.Logger
	webhookSecret []byte
}

// New returns a server that reads and writes jobs in db.
func New(db store.DB, log *slog.Logger) *Server { return &Server{db: db, log: log} }

// Register adds the endpoints to mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/jobs", s.enqueue)
	mux.HandleFunc("POST /v1/jobs/batch", s.enqueueBatch)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	mux.HandleFunc("POST "+webhook.Path, s.receiveWebhook)
}

// jobFields are the fields of one job in a request.
type jobFields struct {
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
	RunAt     *string         `json:"run_at"`
	DedupeKey *string         `json:"dedupe_key"`
}

type enqueueRequest struct {
	Shop string `json:"shop"`
	jobFields
}

type batchRequest struct {
	Shop string      `json:"shop"`
	Jobs []jobFields `json:"jobs"`
}

// requestError is a client error with its status.
type requestError struct {
	status int
	msg    string
	// retryAfter, if positive, is sent as the Retry-After header (429).
	retryAfter time.Duration
}

func (e *requestError) Error() string { return e.msg }

func invalid(format string, args ...any) error {
	return &requestError{status: http.StatusUnprocessableEntity, msg: fmt.Sprintf(format, args...)}
}

// readJSON reads the body, up to MaxBodyBytes, and decodes it strictly into v:
// one JSON object, with no field v doesn't have. what describes the expected
// body for the error message.
func readJSON(w http.ResponseWriter, r *http.Request, v any, what string) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return &requestError{status: http.StatusRequestEntityTooLarge,
				msg: fmt.Sprintf("body is larger than %d bytes", MaxBodyBytes)}
		}
		return &requestError{status: http.StatusBadRequest, msg: "could not read the body"}
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return invalid("body must be one JSON object with %s: %v", what, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid("body has data after the JSON object")
	}
	return nil
}

// parseEnqueue reads and validates the whole request before anything is
// written (D-039), so every rejected request creates nothing (AC-1.3).
func parseEnqueue(w http.ResponseWriter, r *http.Request) (store.NewJob, error) {
	var req enqueueRequest
	if err := readJSON(w, r, &req, "fields shop, kind, payload and optional run_at and dedupe_key"); err != nil {
		return store.NewJob{}, err
	}
	if err := checkName("shop", req.Shop, maxShopLen); err != nil {
		return store.NewJob{}, err
	}
	return buildJob(req.Shop, req.jobFields)
}

// parseBatch reads and validates a whole batch before anything is written (D-030,
// D-039). Any invalid job, a dedupe key used twice, or a batch with no jobs or
// more than MaxBatchSize is 422 and creates nothing (AC-2.4).
func parseBatch(w http.ResponseWriter, r *http.Request) ([]store.NewJob, error) {
	var req batchRequest
	if err := readJSON(w, r, &req, "fields shop and jobs, each job with kind, payload and optional run_at and dedupe_key"); err != nil {
		return nil, err
	}
	if err := checkName("shop", req.Shop, maxShopLen); err != nil {
		return nil, err
	}
	switch {
	case len(req.Jobs) == 0:
		return nil, invalid("jobs must hold at least one job")
	case len(req.Jobs) > MaxBatchSize:
		return nil, invalid("jobs holds %d jobs, more than the most a batch may hold, %d", len(req.Jobs), MaxBatchSize)
	}
	jobs := make([]store.NewJob, len(req.Jobs))
	firstUse := map[string]int{}
	for i, f := range req.Jobs {
		job, err := buildJob(req.Shop, f)
		if err != nil {
			var re *requestError
			if errors.As(err, &re) {
				return nil, &requestError{status: re.status, msg: fmt.Sprintf("jobs[%d]: %s", i, re.msg)}
			}
			return nil, err
		}
		if job.DedupeKey != "" {
			if first, seen := firstUse[job.DedupeKey]; seen {
				return nil, invalid("jobs[%d]: dedupe_key is already used by jobs[%d]", i, first)
			}
			firstUse[job.DedupeKey] = i
		}
		jobs[i] = job
	}
	return jobs, nil
}

// buildJob checks one job's fields and returns the job for shop.
func buildJob(shop string, f jobFields) (store.NewJob, error) {
	if err := checkName("kind", f.Kind, maxKindLen); err != nil {
		return store.NewJob{}, err
	}
	if p := bytes.TrimSpace(f.Payload); len(p) == 0 || p[0] != '{' {
		return store.NewJob{}, invalid("payload must be a JSON object")
	}
	job := store.NewJob{Shop: shop, Kind: f.Kind, Payload: f.Payload}
	if f.DedupeKey != nil {
		// D-045: a non-empty string of at most 255 bytes with no control characters.
		if *f.DedupeKey == "" {
			return store.NewJob{}, invalid("dedupe_key must not be empty")
		}
		if err := checkName("dedupe_key", *f.DedupeKey, MaxDedupeKeyLen); err != nil {
			return store.NewJob{}, err
		}
		job.DedupeKey = *f.DedupeKey
	}
	if f.RunAt != nil {
		t, err := time.Parse(time.RFC3339Nano, *f.RunAt)
		if err != nil {
			return store.NewJob{}, invalid("run_at must be an RFC 3339 time such as 2026-12-01T10:00:00Z")
		}
		job.RunAt = &t
	}
	return job, nil
}

func checkName(field, v string, maxLen int) error {
	switch {
	case v == "":
		return invalid("%s is required", field)
	case len(v) > maxLen:
		return invalid("%s is longer than %d bytes", field, maxLen)
	case strings.ContainsFunc(v, func(r rune) bool { return r < 0x20 || r == 0x7f }):
		return invalid("%s contains control characters", field)
	}
	return nil
}

// enqueue answers POST /v1/jobs: 201 {id} for a new job, 200 {id, duplicate}
// when the shop already has the dedupe key, 404 for an unknown shop, 422 for an
// invalid request or unknown kind, 413 for an oversized body (AC-1.1 to AC-1.4),
// and 429 with Retry-After when the shop is at its backlog limit (AC-11.1).
func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	job, err := parseEnqueue(w, r)
	if err != nil {
		s.fail(w, err)
		return
	}
	res, err := store.Enqueue(r.Context(), s.db, job)
	if err != nil {
		s.fail(w, enqueueError(err))
		return
	}
	// The job is committed before 201 is sent, so it survives any crash (AC-1.1).
	w.Header().Set("Location", "/v1/jobs/"+strconv.FormatInt(res.ID, 10))
	if res.Duplicate {
		// AC-1.4: the shop already has this dedupe key; nothing was created.
		writeJSON(w, http.StatusOK, enqueueResponse{ID: res.ID, Duplicate: true})
		return
	}
	writeJSON(w, http.StatusCreated, enqueueResponse{ID: res.ID})
}

// enqueueError turns what an enqueue returned into an error fail understands:
// a client error with its status, or the error itself, which is a 500.
func enqueueError(err error) error {
	var rej *admission.Rejected
	switch {
	case errors.As(err, &rej):
		return &requestError{status: http.StatusTooManyRequests, retryAfter: rej.RetryAfter,
			msg: "the shop is at its backlog limit; try again later"}
	case errors.Is(err, store.ErrUnknownShop):
		return &requestError{status: http.StatusNotFound, msg: "unknown shop"}
	case errors.Is(err, store.ErrUnknownKind):
		return invalid("unknown job kind")
	case errors.Is(err, store.ErrInvalidPayload):
		return invalid("payload is not storable JSON: %v", err)
	case errors.Is(err, store.ErrDuplicateKeyInBatch):
		return invalid("a dedupe_key is used twice in the batch")
	}
	return err
}

// enqueueBatch answers POST /v1/jobs/batch (D-030, D-048): 201 {ids, duplicates}
// when it created at least one job, 200 when every job was a duplicate, 404 for
// an unknown shop, 413 or 422 for an invalid batch, and 429 with Retry-After when
// the new jobs don't fit under the shop's backlog limit. A batch is all or
// nothing (AC-2.1).
func (s *Server) enqueueBatch(w http.ResponseWriter, r *http.Request) {
	jobs, err := parseBatch(w, r)
	if err != nil {
		s.fail(w, err)
		return
	}
	res, err := store.EnqueueBatch(r.Context(), s.db, jobs)
	if err != nil {
		s.fail(w, enqueueError(err))
		return
	}
	status := http.StatusCreated
	if res.Created == 0 {
		status = http.StatusOK // every job was a duplicate
	}
	if res.Duplicates == nil {
		res.Duplicates = []int64{}
	}
	writeJSON(w, status, batchResponse{IDs: res.IDs, Duplicates: res.Duplicates})
}

// batchResponse is the body of a batch answer: one ID per request job, in
// order, and the IDs among them that already existed.
type batchResponse struct {
	IDs        []int64 `json:"ids"`
	Duplicates []int64 `json:"duplicates"`
}

// enqueueResponse is the body of a 201 or 200 enqueue answer.
type enqueueResponse struct {
	ID        int64 `json:"id"`
	Duplicate bool  `json:"duplicate,omitempty"`
}

// jobView is what GET /v1/jobs/{id} shows: state, counters and timestamps.
type jobView struct {
	ID              int64      `json:"id"`
	Shop            string     `json:"shop"`
	Kind            string     `json:"kind"`
	State           string     `json:"state"`
	RunAt           time.Time  `json:"run_at"`
	EnqueuedAt      time.Time  `json:"enqueued_at"`
	FirstRunnableAt time.Time  `json:"first_runnable_at"`
	FirstClaimedAt  *time.Time `json:"first_claimed_at"`
	ClaimSeq        int32      `json:"claim_seq"`
	Attempts        int32      `json:"attempts"`
	Throttles       int32      `json:"throttles"`
	LeaseExpiresAt  *time.Time `json:"lease_expires_at"`
	CompletedAt     *time.Time `json:"completed_at"`
	DeadAt          *time.Time `json:"dead_at"`
	DeadReason      *string    `json:"dead_reason"`
	LastError       *string    `json:"last_error"`
	DiscardedAt     *time.Time `json:"discarded_at"`
}

// getJob answers GET /v1/jobs/{id}: 200 with the job, 404 if there is none.
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		s.fail(w, &requestError{status: http.StatusNotFound, msg: "job not found"})
		return
	}
	j, err := store.GetJob(r.Context(), s.db, id)
	if errors.Is(err, store.ErrNotFound) {
		s.fail(w, &requestError{status: http.StatusNotFound, msg: "job not found"})
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobView{
		ID: j.ID, Shop: j.Shop, Kind: j.Kind, State: j.State, RunAt: j.RunAt, EnqueuedAt: j.EnqueuedAt,
		FirstRunnableAt: j.FirstRunnableAt, FirstClaimedAt: j.FirstClaimedAt, ClaimSeq: j.ClaimSeq,
		Attempts: j.Attempts, Throttles: j.Throttles, LeaseExpiresAt: j.LeaseExpiresAt,
		CompletedAt: j.CompletedAt, DeadAt: j.DeadAt, DeadReason: j.DeadReason, LastError: j.LastError,
		DiscardedAt: j.DiscardedAt,
	})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	var re *requestError
	if errors.As(err, &re) {
		if re.retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(admission.RetryAfterSeconds(re.retryAfter)))
		}
		writeJSON(w, re.status, map[string]string{"error": re.msg})
		return
	}
	s.log.Error("request failed", "err", err.Error())
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
