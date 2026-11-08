// Package invariants holds the Q1–Q8 tests against the real Postgres in
// Compose (design §13, §16). Phase 2 adds Q2 (stale writes affect 0 rows), Q3
// (one live lease per job, a rapid property test), a basic in-process Q1 (no
// accepted job is lost when workers are killed) and the backlog counter
// property test.
package invariants
