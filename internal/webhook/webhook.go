// Package webhook holds what both ends of a Shopify-style webhook share: the
// header names, the topic the queue accepts, and the HMAC that signs the raw
// body (design §11.1, §12, AC-8.1). mockshop signs with Sign, and the api
// role's receiver checks with Verify before it parses anything.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
)

// The headers a webhook carries, as Shopify names them.
const (
	HeaderHMAC       = "X-Shopify-Hmac-Sha256"
	HeaderShopDomain = "X-Shopify-Shop-Domain"
	HeaderTopic      = "X-Shopify-Topic"
	HeaderWebhookID  = "X-Shopify-Webhook-Id"
)

// TopicOrdersCreate is the one topic the queue accepts (D-020).
const TopicOrdersCreate = "orders/create"

// Path is where the api role receives webhooks.
const Path = "/webhooks/shopify"

// Sign returns base64(HMAC-SHA256(secret, body)), the value of the
// X-Shopify-Hmac-Sha256 header.
func Sign(secret, body []byte) string {
	return base64.StdEncoding.EncodeToString(mac(secret, body))
}

// Verify reports whether header is the signature of the raw body under secret.
// It decodes the header and compares the MACs with hmac.Equal, which takes the
// same time whatever the bytes are, so a caller can't learn the MAC byte by byte
// (NFR-16). An empty secret verifies nothing.
func Verify(secret, body []byte, header string) bool {
	if len(secret) == 0 || header == "" {
		return false
	}
	got, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return false
	}
	return hmac.Equal(got, mac(secret, body))
}

func mac(secret, body []byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return m.Sum(nil)
}
