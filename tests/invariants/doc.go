// Package invariants holds the Q1–Q8 tests against the real Postgres in
// Compose (design §13, §16). Phase 2 adds Q2 (stale writes affect 0 rows), Q3
// (one live lease per job, a rapid property test), a basic in-process Q1 (no
// accepted job is lost when workers are killed) and the backlog counter
// property test. Phase 3 adds Q4 through the enqueue path and the idempotency
// helper's AC-7.1 to AC-7.3. Phase 4 adds Q5 (a shop's running jobs never pass
// its cap, at the claim SQL with concurrent claimers) and Q8 (at the backlog
// limit, enqueue returns 429 and creates nothing), and extends the backlog
// counter test to batches and rejections. Phase 5 adds Q7 (a throttle never uses
// up an attempt, against a mockshop bucket of one call, with AC-6.2 checked
// through a real dispatcher), the throttle budget (AC-6.3), and Q4 end to end
// through mockshop's webhook sender and the webhook receiver (AC-8.2 to AC-8.4).
package invariants
