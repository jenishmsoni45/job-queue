# Project 3: Multi-tenant durable job queue

> A background-job system where many Shopify-style shops share workers, jobs survive crashes, and one shop's Black Friday can't starve the rest. Proven with a load test, and run from a small ops console.

| | |
|---|---|
| **Wave** | Wave 1 · stretch 3+ in wave 2 |
| **Dates** | Phase 0 before Nov 30 · build Nov 30–Jan 3 (the holidays fall inside it) · 3+: Mar 29–Apr 18 |
| **Built for** | Shopify and Clio, then Uber, DoorDash, Instacart, Faire, FAANG system design and Wealthsimple Platform Engineering |
| **Stack** | Go, Postgres 16, pgx, rapid, Toxiproxy, Prometheus; console in TypeScript + React (Vite); govulncheck and npm audit for dependency scans |
| **Install** | Docker Desktop, Go, golangci-lint, Node.js, git, gh |
| **Depends on** | Nothing. It reuses project 1's ideas (idempotency keys, at-least-once delivery) on purpose. |
| **Cost** | Free. Everything runs locally; Shopify is mocked. |

---

## 1. What you're building

Shopify scaled by splitting into isolated pods, so one failure or flash sale can't take down the platform. Shopify apps live with the same problems at a smaller scale: webhooks can arrive more than once, and API calls are throttled by a leaky bucket per app. Your queue processes that kind of workload correctly and fairly.

What makes it stand out:
- **It owns the delivery semantics:** leases, heartbeats and fenced completions. It doesn't wrap Sidekiq.
- **Fairness is proven, not claimed.** In the "Black Friday" test, one shop sends 100× traffic and every other shop's p99 wait stays nearly flat, compared against plain FIFO.
- **It names the prior art.** Solid Queue (the default in new Rails 8 apps) and River (Go) also claim jobs with `FOR UPDATE SKIP LOCKED`. The edge here is per-tenant fairness and admission control.

## 2. Scope

**In (wave 1)**
- Postgres-backed jobs claimed with `SELECT … FOR UPDATE SKIP LOCKED`
- At-least-once delivery with leases, heartbeats, a reaper and fenced completions; retries with exponential backoff and jitter; a dead-letter queue; delayed and scheduled jobs
- An idempotency helper, so a duplicate run of a handler has no extra side effects
- Per-shop fairness (deficit round robin with weights), concurrency caps and rate limits; admission control under overload
- `mockshop`, a local Shopify simulation: webhooks with duplicates and HMAC signatures, and an Admin API with a per-shop leaky bucket
- A webhook receiver that checks the HMAC and dedupes on `X-Shopify-Webhook-Id`
- Chaos tests and the Black Friday experiment
- An ops console: per-shop backlog and p99 wait, plus the dead-letter queue with retry and discard

**Stretch 3+ (wave 2):** partitions owned by nodes through leases, epoch fencing, rebalancing and failover tests.

**Out of scope:** exactly-once claims, a custom storage engine, multi-region, console auth, a real Shopify app, and any cloud or paid service.

## 3. Architecture (starting sketch; Phase 0 turns it into Mermaid diagrams)

```
mockshop ──webhooks (dup, HMAC)──▶ webhook receiver ──enqueue (dedupe key)──▶ Postgres `jobs`
                                       │ 429 if shop backlog over limit            │
                                       ▼                                           │ claim: SKIP LOCKED
                                  admission control                                ▼
                                                     dispatcher (deficit round robin, caps, rate limits)
                                                                                   │
                                                              worker pool ◀────────┘
                                                                   │ heartbeat extends lease
                                                                   │ handler calls mockshop Admin API
                                                                   ▼ (leaky bucket → 429 + Retry-After)
                                                     fenced complete / fail / reschedule
reaper: expired leases → available · too many attempts → dead-letter
console (React) ◀── read API: backlog, p99 wait per shop, DLQ retry/discard
```

**Fencing rule:** a worker may complete, fail or reschedule a job only if the job is still running with the same attempt number and lease owner. Zero rows updated means the worker lost its lease, and its result is discarded.

**Black Friday experiment (example settings; Phase 0 fixes them):** 10 shops at 20 jobs/s, a handler doing ~30 ms of work plus one mock API call, and 16 workers. Shop 1 then jumps to 2,000 jobs/s for 5 minutes. Run it once with FIFO and once with deficit round robin, and chart every shop's p99 wait over time.

## 4. Guarantees and numbers

| # | Invariant | How it's tested |
|---|---|---|
| Q1 | Every accepted job ends completed or dead; none is lost. | Chaos: kill -9 workers, restart Postgres, add latency; check every job ID |
| Q2 | A worker whose lease expired can't complete, fail or reschedule the job. | Pause a worker past its lease, let another claim the job, resume; the write affects 0 rows |
| Q3 | At most one live lease per job. | Property test with many concurrent claimers |
| Q4 | A webhook delivered k times creates exactly one job. | Concurrent replays of one webhook ID |
| Q5 | A shop's running jobs never exceed `max_concurrency`. | Sampled under load; property test on the dispatcher |
| Q6 | Under saturation, each backlogged shop gets at least its weighted share of claims, within tolerance. | Property test on the scheduler |
| Q7 | A throttle reschedules without using up an attempt. | Integration test with a tiny bucket |
| Q8 | At the backlog limit, enqueue returns 429 and creates nothing. | Integration test |

**Numbers to publish:**
- Throughput, and per-shop p50 and p99 queue wait
- The Black Friday chart, FIFO vs deficit round robin
- Jobs lost across N chaos runs (target 0), recovery time after a worker crash, and duplicate executions vs side effects applied

## 5. Questions Phase 0 must settle

1. Does a central dispatcher run deficit round robin, or does each worker schedule itself? Where does the round-robin state live?
2. What lease timeout and heartbeat interval? How does the reaper treat a slow but alive worker?
3. How is a throttle told apart from a failure, and how is each retried?
4. What does admission control shed first: the noisy shop's work, low-priority work, or new work?
5. Where does Postgres-as-a-queue stop scaling, and what would you move to? (This goes in the design doc, not the code.)
6. What are the final Black Friday settings (shops, rates, spike, workers, handler cost) for your machine?
7. Which pages and actions does the console have?
8. (3+) How many partitions, where do partition leases live, and how does rebalancing work when a node joins or leaves?

## 6. Phases

Each phase is one PR. Phase 0 writes the full acceptance criteria into `docs/phases.md`; these are the seeds.

| Phase | Target dates | Scope | Done when | Look closely at |
|---|---|---|---|---|
| **0** Plan | Before Nov 30 | Questions answered; requirements, architecture (Mermaid), design, phases, decisions | No open questions; docs consistent | Fencing rule; scheduler choice; experiment settings |
| **1** Scaffold | Nov 30 | Layout, migrations folder, Compose (Postgres, Toxiproxy, Prometheus, queued, mockshop), Makefile, CI, Vite app | `make up` healthy; CI green | Compose and CI |
| **2** Core queue | Dec 1–6 | Enqueue, claim, complete and fail; leases, heartbeats, reaper; backoff; dead-letter; scheduled jobs | Q2, Q3 pass; a basic Q1 run passes | Claim SQL; fencing; reaper |
| **3** Crash-proof | Dec 7–13 | Chaos harness, idempotency helper, enqueue dedupe | 100 chaos runs lose 0 jobs; Q4 passes | Chaos scenarios; idempotency contract |
| **4** Tenants | Dec 14–20 | Deficit round robin, caps, rate limits, admission control | Q5, Q6, Q8 pass | Scheduler fairness; admission rule |
| **5** Shopify workload and Black Friday | Dec 21–27 | mockshop, webhook receiver, throttle-aware retries, loadgen, FIFO vs DRR experiment | Q7 passes; the chart exists and reproduces | Experiment design; HMAC check |
| **6** Console and ship | Dec 28–Jan 3 | React console, Prometheus metrics, README, postmortem, talk outline | Definition of done met; `make bench` reproduces the chart | README claims vs the evidence |
| **7** Sharded queue (3+) | Mar 29–Apr 18 | Partitions, partition leases with epochs, rebalancing, failover tests | Failover measured; 0 lost jobs; duplicates explained | Epoch fencing; split-brain cases |

## 7. Repo layout (Phase 1 creates it)

```
project-3-job-queue/
├── CLAUDE.md  PLAN.md  README.md  .github/pull_request_template.md
├── docs/        requirements.md  architecture.md  design.md  decisions.md  phases.md  postmortem.md
├── cmd/         queued/  mockshop/  loadgen/
├── internal/    store/  lease/  scheduler/  admission/  webhook/
├── migrations/
├── console/     React + TypeScript (Vite)
├── tests/       invariants/  adversarial/  chaos/
├── bench/       experiment configs, results/, charts
├── docker-compose.yml  Makefile  .github/workflows/ci.yml
```

## 8. Prompts

Run `claude` inside this folder. The workflow rules live in `CLAUDE.md`, so the prompts stay short.

### P0 · Phase 0: plan (paste once)

```text
Start phase 0 (planning). Follow the Phase 0 rules in @CLAUDE.md: no code and no scaffolding.
1. Read @PLAN.md. List every open question: start with section 5, then add anything ambiguous, missing or contradictory. Ask me in batches of up to 5, each with options, trade-offs and your recommendation. Wait for my answers before the next batch.
2. Record every question and answer in docs/decisions.md.
3. When nothing is open, write:
   - docs/requirements.md: users, user stories with acceptance criteria, non-functional requirements with numbers, non-goals
   - docs/architecture.md: Mermaid diagrams for the components, the job lifecycle (state diagram), claim → heartbeat → complete with a lost lease (sequence), the webhook path, the data model, and the 3+ partition layout
   - docs/design.md: data model, APIs, invariants Q1–Q8, failure modes, alternatives rejected, prior art (Solid Queue, River, Sidekiq), test plan, the Black Friday experiment design
   - docs/phases.md: phases 1–7 from PLAN.md section 6, each with scope, testable acceptance criteria, tests to add and out of scope
4. Review the docs as a skeptical staff engineer: gaps, contradictions, untestable criteria, anything needing a paid or cloud service or an install on this Mac beyond the Install row. Fix them, and ask me if a fix needs a decision.
5. Create branch phase-0-plan, commit, push, and open a docs-only PR with the template. Then stop.
```

### P1 · Start a phase (every phase after 0)

```text
Start phase <N>. Follow the phase workflow in @CLAUDE.md: sync main and confirm earlier phase PRs are merged, branch, read the phase in @docs/phases.md, ask me before coding if anything is unclear, build the whole phase with tests, run lint and tests, self-review the full diff and fix what you find, then push and open the PR with the template. Stop when the PR is open.
```

### P2 · Address my review

```text
I've reviewed PR #<n>. Read every comment (`gh pr view <n> --comments` and the inline comments via `gh api repos/{owner}/{repo}/pulls/<n>/comments`). Fix each one on the same branch, or explain why you disagree. Run lint and tests, push, and reply with a short list of comment → what changed. Don't merge.
```

### P3 · Explain a PR to me (while reviewing)

```text
Walk me through PR #<n> as if onboarding me to code I must defend in an interview. Follow the review-guide order. For each file: what it does, why it's built this way, the alternative rejected, and how the tests prove it. For concurrency code, walk me through one interleaving that would break it without the fix. Then ask me 5 questions about it, one at a time, and correct what I get wrong. Don't change any code.
```

### P4 · Mock interview (after phase 6, and again after phase 7)

```text
Act as an engineer interviewing me at Shopify (platform) or Google (system design). Interview me about this project for 30 minutes, one question at a time. Start with "walk me through it", then go deep wherever my answer is vaguest: leases and fencing, fairness, throttling, Postgres limits. Finish with "now design it for 100,000 shops across regions". Use @PLAN.md section 9 and @docs/decisions.md as a starting point and follow up on my answers. Don't give me answers. At the end, score clarity, depth and ownership out of 5, and list 3 topics to study.
```

## 9. Finish line

**Definition of done** (after phase 6; 3+ after phase 7)
- [ ] Phase 0 docs merged and kept current, with Solid Queue and River named as prior art
- [ ] Q1–Q8 run in CI; the chaos suite runs with `make chaos`
- [ ] `docker compose up` + `make bench` reproduce the Black Friday chart and every README number
- [ ] `docs/postmortem.md` on one real bug found during the build (fencing and lease bugs make the best ones)
- [ ] 3-minute demo video (show the console during the spike) + 20-minute talk outline
- [ ] README section on how Claude Code built it under your plan and review, and how you verified it

**Resume line** (fill in from your own measurements):
> Designed a multi-tenant job queue in Go on Postgres with leases, fenced completions and dead-lettering, processing Shopify-style webhooks under per-shop API rate limits. Deficit-round-robin scheduling kept other shops' p99 queue wait under X ms while one shop sent 100× normal load (FIFO: Y s); crash tests across N runs lost zero jobs.

**Questions to be ready for**
- Why at-least-once and not exactly-once?
- How do you pick the lease timeout? What happens to a job whose worker is alive but slow?
- Why deficit round robin over strict priority?
- Where does Postgres-as-a-queue break, and what would you move to?
- What does yours do that Solid Queue doesn't?
- (3+) How do you stop two nodes from both thinking they own a partition?
