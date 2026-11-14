package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/jenishmsoni45/job-queue/internal/handlers"
	"github.com/jenishmsoni45/job-queue/internal/store"
	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

// OrderTagKind is the job kind an orders/create webhook becomes (D-020).
const OrderTagKind = handlers.OrderTag

// WithWebhookSecret sets the HMAC secret webhooks are checked with. Without
// one, every webhook is refused with 401.
func (s *Server) WithWebhookSecret(secret []byte) *Server {
	s.webhookSecret = secret
	return s
}

// OrderTagPayload is the payload of an order.tag job (D-020).
type OrderTagPayload = handlers.OrderTagPayload

// receiveWebhook answers POST /webhooks/shopify (design §11.1):
//
//  1. It reads the raw body, up to MaxBodyBytes (413).
//  2. It checks X-Shopify-Hmac-Sha256 against the HMAC of the raw body, in
//     constant time, before parsing anything (401, AC-8.1).
//  3. X-Shopify-Topic must be orders/create, X-Shopify-Shop-Domain names the
//     shop, X-Shopify-Webhook-Id becomes the dedupe key, and the body's id is
//     the order (422).
//  4. It enqueues one order.tag job with payload {order_id} through the same
//     path as POST /v1/jobs: 200 {id} when created, 200 {id, duplicate} for a
//     webhook ID the shop already has, even at the backlog limit (AC-8.2 to
//     AC-8.4), 404 for an unknown shop, and 429 with Retry-After at the limit.
//
// A webhook that is refused for any reason creates nothing.
func (s *Server) receiveWebhook(w http.ResponseWriter, r *http.Request) {
	job, err := s.parseWebhook(w, r)
	if err != nil {
		s.fail(w, err)
		return
	}
	res, err := store.Enqueue(r.Context(), s.db, job)
	if err != nil {
		s.fail(w, enqueueError(err))
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+strconv.FormatInt(res.ID, 10))
	// Shopify only needs a 2xx, so a new webhook and a duplicate both get 200.
	writeJSON(w, http.StatusOK, enqueueResponse{ID: res.ID, Duplicate: res.Duplicate})
}

func (s *Server) parseWebhook(w http.ResponseWriter, r *http.Request) (store.NewJob, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return store.NewJob{}, &requestError{status: http.StatusRequestEntityTooLarge, msg: "body is too large"}
		}
		return store.NewJob{}, &requestError{status: http.StatusBadRequest, msg: "could not read the body"}
	}
	if !webhook.Verify(s.webhookSecret, body, r.Header.Get(webhook.HeaderHMAC)) {
		return store.NewJob{}, &requestError{status: http.StatusUnauthorized, msg: "missing or invalid " + webhook.HeaderHMAC}
	}
	// Only a signed body is looked at from here on.
	if topic := r.Header.Get(webhook.HeaderTopic); topic != webhook.TopicOrdersCreate {
		return store.NewJob{}, invalid("unsupported topic %q; only %s is accepted", topic, webhook.TopicOrdersCreate)
	}
	shop := r.Header.Get(webhook.HeaderShopDomain)
	if err := checkName(webhook.HeaderShopDomain, shop, maxShopLen); err != nil {
		return store.NewJob{}, err
	}
	id := r.Header.Get(webhook.HeaderWebhookID)
	if err := checkName(webhook.HeaderWebhookID, id, MaxDedupeKeyLen); err != nil { // D-045
		return store.NewJob{}, err
	}
	orderID, err := orderIDOf(body)
	if err != nil {
		return store.NewJob{}, err
	}
	payload, err := json.Marshal(OrderTagPayload{OrderID: orderID})
	if err != nil {
		return store.NewJob{}, err
	}
	return store.NewJob{Shop: shop, Kind: OrderTagKind, Payload: payload, DedupeKey: id}, nil
}

// orderIDOf reads the order ID from a webhook body: a JSON object whose id is
// a positive integer. Shopify's order payloads carry many more fields, which
// are ignored.
func orderIDOf(body []byte) (int64, error) {
	var order struct {
		ID json.RawMessage `json:"id"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&order); err != nil {
		return 0, invalid("body must be a JSON object: %v", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return 0, invalid("body has data after the JSON object")
	}
	// The raw JSON must itself be an integer: a quoted "7", 1.5 or 1e3 is refused.
	id, err := strconv.ParseInt(string(order.ID), 10, 64)
	if err != nil || id <= 0 {
		return 0, invalid("body must have an id that is a positive integer")
	}
	return id, nil
}
