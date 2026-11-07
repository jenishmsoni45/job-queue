# Project 3: Multi-tenant durable job queue

A Go job queue on Postgres in which Shopify-style shops share workers. It provides at-least-once delivery with leases and fenced completions, per-shop fairness (deficit round robin), rate limits and admission control, a local mock of Shopify's webhooks and Admin API, and a small React ops console. The wave-2 stretch (3+) splits the queue across nodes.

## Source of truth

- Until the Phase 0 PR is merged, `PLAN.md` is the brief.
- After that, the docs are the source of truth: `docs/requirements.md`, `docs/architecture.md`, `docs/design.md`, `docs/decisions.md` and `docs/phases.md`. If code and docs disagree, the docs win. If the docs look wrong, stop and ask me.

## Workflow: one phase, one PR

I start every phase by name ("Start phase 3"). Never start a phase on your own.

1. **Sync.** `git checkout main && git pull`. Check with `gh pr list` that no earlier phase PR is still open. Create the branch `phase-<N>-<short-name>`.
2. **Read.** Read the phase in `docs/phases.md` and the docs it touches. If anything is ambiguous, missing or conflicts with the docs, ask me before writing code. Never invent requirements.
3. **Build the whole phase.** Write the code, a test for every acceptance criterion, and any doc updates the phase lists. Use small, logical commits with clear messages.
4. **Verify.** `make lint`, `make test` and `make audit` must pass, plus any check the phase names (for example `make chaos`). Never skip, weaken or delete a test to get green; if a test is wrong, say so in the PR.
5. **Self-review.** Review `git diff main...HEAD` as a senior engineer would. Check correctness under concurrency and failure (transactions, locks, leases, fencing), a test for each behaviour, security, and needless complexity. Fix what you find.
6. **Open the PR.** Push, then run `gh pr create` and fill in every section of `.github/pull_request_template.md`.
7. **Stop.** Send me the PR link and wait.

**When I ask you to address review comments,** read them with `gh pr view <n> --comments` and `gh api repos/{owner}/{repo}/pulls/<n>/comments`. Fix them on the same branch, run lint and tests, push, and reply with a list of what changed. Never merge.

## Phase 0 is planning only

Write no code and do no scaffolding. First ask me every open question: start with "Questions Phase 0 must settle" in PLAN.md, then add anything else unclear. Ask in batches of up to 5, each with options, trade-offs and your recommendation. Record every question and answer in `docs/decisions.md`. Only when nothing is open, write the Phase 0 docs listed in PLAN.md. Self-review them for gaps, contradictions and untestable criteria, then open a docs-only PR.

## Surprises mid-phase

If the design turns out to be wrong or a requirement can't be met, stop. Explain the problem and the options, then wait. Once I decide, record the decision in `docs/decisions.md` and continue. Out-of-scope ideas go under "Follow-ups" in the PR, not into the code.

## Hard limits

- Never push to `main`, merge a PR, force-push, or rewrite pushed history.
- I am the only author. Commits and PRs carry no `Co-Authored-By` trailer, no "Generated with Claude Code" line and no mention of Claude or AI in commit messages, PR titles or PR bodies.
- Free and local only. Everything runs in Docker Compose or on this machine. Use no cloud services (AWS, GCP, Azure, hosted databases or queues), no paid APIs, no sign-ups, and no credentials. If something seems to need one, stop and ask.
- No secrets in the repo. Use `.env.example` with dummy values; the webhook HMAC secret is a local dummy.
- Never install anything on this Mac: no `brew`, no global `npm` or `go install`, no `curl | sh`, no installers. Project dependencies through Go modules and npm inside `console/` are fine. If a tool is missing, stop and ask me.
- Add only well-known, widely used dependencies and Docker images (official or verified publisher), pinned to exact versions in the committed lockfiles (`go.sum`, `package-lock.json`) and compose file. Justify each new one in the PR.
- Never download or run an AI model. Nothing keeps running after a command finishes except the Docker Compose services, and `make down` stops those.
- Change `docs/requirements.md`, `docs/architecture.md` or `docs/design.md` only with my approval, and log each change in `docs/decisions.md`.
- Never put a number in `README.md` or `docs/` unless a script in the repo produced it.

## Project rules

- Leases and timestamps use the database clock (`now()`), never a worker's clock.
- Never describe the queue as "exactly-once". It is at-least-once with idempotent handlers.
- `mockshop` is a local simulation of Shopify's webhooks and Admin API. Never call real Shopify.
- Keep the console to one page with no auth and no UI framework beyond React.
- golangci-lint must be clean, and Go tests run with `-race`.
- The console's `.npmrc` sets `ignore-scripts=true`. If a package needs its install script, ask me.

## Stack and commands

- Go, Postgres 16 (pgx), rapid, Toxiproxy, Prometheus; console in TypeScript + React (Vite)
- `make up` / `make down` · `make test` · `make lint` · `make audit` (govulncheck and `npm audit`) · `make chaos` · `make bench` (Black Friday experiment, results in `bench/results/`)

## Invariants the tests must enforce

1. No accepted job is lost: each ends completed or dead.
2. A worker whose lease expired cannot complete, fail or reschedule the job.
3. At most one live lease per job.
4. A webhook delivered k times creates exactly one job.
5. A shop's running jobs never exceed its max_concurrency.
6. Under saturation, each backlogged shop gets at least its weighted share of cost units, within tolerance (`docs/decisions.md` D-013, D-026).
7. A throttle reschedules without using up an attempt.
8. At the backlog limit, enqueue returns 429 and creates nothing.
