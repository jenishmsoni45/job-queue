// Package adversarial holds the hostile-condition tests (design §16): skewed
// worker clocks in phase 2, replay storms on one dedupe key in phase 3, two
// dispatchers claiming at once in phase 4, and in phase 5 tampered webhook HMACs,
// oversized and malformed webhook bodies, and a webhook replay storm.
package adversarial
