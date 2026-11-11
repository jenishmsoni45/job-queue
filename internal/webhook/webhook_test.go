package webhook_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"github.com/jenishmsoni45/job-queue/internal/webhook"
)

// Sign is base64(HMAC-SHA256(secret, body)), computed independently here.
func TestSignMatchesHMACSHA256(t *testing.T) {
	secret, body := []byte("local-dummy-webhook-secret"), []byte(`{"id":1001}`)
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	want := base64.StdEncoding.EncodeToString(m.Sum(nil))
	if got := webhook.Sign(secret, body); got != want {
		t.Fatalf("Sign = %q, want %q", got, want)
	}
}

// AC-8.1: only the signature of these exact bytes under this secret verifies.
func TestVerify(t *testing.T) {
	secret, body := []byte("secret"), []byte(`{"id":1001}`)
	good := webhook.Sign(secret, body)
	cases := map[string]struct {
		secret, body []byte
		header       string
		want         bool
	}{
		"valid":            {secret, body, good, true},
		"missing header":   {secret, body, "", false},
		"wrong signature":  {secret, body, webhook.Sign(secret, []byte("other")), false},
		"tampered body":    {secret, []byte(`{"id":1002}`), good, false},
		"one byte added":   {secret, append(append([]byte{}, body...), ' '), good, false},
		"wrong secret":     {[]byte("other-secret"), body, good, false},
		"empty secret":     {nil, body, webhook.Sign(nil, body), false},
		"not base64":       {secret, body, "%%%not-base64%%%", false},
		"truncated MAC":    {secret, body, good[:len(good)-4], false},
		"hex, not base64":  {secret, body, "6162636465666768", false},
		"padding stripped": {secret, body, base64.RawStdEncoding.EncodeToString(mustDecode(t, good)), false},
	}
	for name, c := range cases {
		if got := webhook.Verify(c.secret, c.body, c.header); got != c.want {
			t.Errorf("%s: Verify = %v, want %v", name, got, c.want)
		}
	}
}

func mustDecode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
