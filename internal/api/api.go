// Package api is the api role's job endpoints (design §11.1): enqueue one job,
// with an optional dedupe key, and read one job. Batch enqueue, webhooks, the read API and the DLQ arrive in
// later phases.
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

	"github.com/jenishmsoni45/job-queue/internal/store"
)

// MaxBodyBytes is the max request body (design §2: 1 MiB).
const MaxBodyBytes = 1 << 20

const (
	maxShopLen = 255
	maxKindLen = 100
	// MaxDedupeKeyLen is the longest dedupe key (D-045).
	MaxDedupeKeyLen = 255
)

// Server serves the job endpoints.
type Server struct {
	db  store.DB
	log *slog.Logger
}

// New returns a server that reads and writes jobs in db.
func New(db store.DB, log *slog.Logger) *Server { return &Server{db: db, log: log} }

// Register adds the endpoints to mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/jobs", s.enqueue)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
}

type enqueueRequest struct {
	Shop      string          `json:"shop"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
	RunAt     *string         `json:"run_at"`
	DedupeKey *string         `json:"dedupe_key"`
}

// requestError is a client error with its status.
type requestError struct {
	status int
	msg    string
}

func (e *requestError) Error() string { return e.msg }

func invalid(format string, args ...any) error {
	return &requestError{status: http.StatusUnprocessableEntity, msg: fmt.Sprintf(format, args...)}
}

// parseEnqueue reads and validates the whole request before anything is
// written (D-039), so every rejected request creates nothing (AC-1.3).
func parseEnqueue(w http.ResponseWriter, r *http.Request) (store.NewJob, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return store.NewJob{}, &requestError{status: http.StatusRequestEntityTooLarge,
				msg: fmt.Sprintf("body is larger than %d bytes", MaxBodyBytes)}
		}
		return store.NewJob{}, &requestError{status: http.StatusBadRequest, msg: "could not read the body"}
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req enqueueRequest
	if err := dec.Decode(&req); err != nil {
		return store.NewJob{}, invalid("body must be one JSON object with fields shop, kind, payload and optional run_at and dedupe_key: %v", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return store.NewJob{}, invalid("body has data after the JSON object")
	}

	if err := checkName("shop", req.Shop, maxShopLen); err != nil {
		return store.NewJob{}, err
	}
	if err := checkName("kind", req.Kind, maxKindLen); err != nil {
		return store.NewJob{}, err
	}
	if p := bytes.TrimSpace(req.Payload); len(p) == 0 || p[0] != '{' {
		return store.NewJob{}, invalid("payload must be a JSON object")
	}
	job := store.NewJob{Shop: req.Shop, Kind: req.Kind, Payload: req.Payload}
	if req.DedupeKey != nil {
		// D-045: a non-empty string of at most 255 bytes with no control characters.
		if *req.DedupeKey == "" {
			return store.NewJob{}, invalid("dedupe_key must not be empty")
		}
		if err := checkName("dedupe_key", *req.DedupeKey, MaxDedupeKeyLen); err != nil {
			return store.NewJob{}, err
		}
		job.DedupeKey = *req.DedupeKey
	}
	if req.RunAt != nil {
		t, err := time.Parse(time.RFC3339Nano, *req.RunAt)
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
// invalid request or unknown kind, 413 for an oversized body (AC-1.1 to AC-1.4).
func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	job, err := parseEnqueue(w, r)
	if err != nil {
		s.fail(w, err)
		return
	}
	res, err := store.Enqueue(r.Context(), s.db, job)
	switch {
	case errors.Is(err, store.ErrUnknownShop):
		s.fail(w, &requestError{status: http.StatusNotFound, msg: "unknown shop"})
		return
	case errors.Is(err, store.ErrUnknownKind):
		s.fail(w, invalid("unknown job kind"))
		return
	case errors.Is(err, store.ErrInvalidPayload):
		s.fail(w, invalid("payload is not storable JSON: %v", err))
		return
	case err != nil:
		s.fail(w, err)
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
