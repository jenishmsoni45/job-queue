// Package webhook will hold the Shopify webhook receiver: the HMAC check over the
// raw body and dedupe on X-Shopify-Webhook-Id (design §11.1). Phase 5 fills it.
package webhook
