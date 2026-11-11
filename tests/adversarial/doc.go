// Package adversarial holds the hostile-condition tests (design §16): skewed
// worker clocks in phase 2, replay storms on one dedupe key in phase 3, two
// dispatchers claiming at once in phase 4, and tampered HMACs in a later phase.
package adversarial
