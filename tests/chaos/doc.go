// Package chaos is the seeded fault-injection harness and its checks (design
// §18, D-023, D-025, D-042 to D-046).
//
// One run brings the chaos stack to a clean state, enqueues 2,000 test.effect
// jobs across the shops through the api, each with a dedupe key, and runs a
// fault schedule drawn from the run's seed: SIGKILL or pause a worker, restart
// Postgres cleanly or by SIGKILL, and add Toxiproxy latency, timeouts or
// connection resets on a process's link to Postgres. It then waits until every
// accepted job is completed or dead, and checks Q1, Q3, Q4, the backlog
// counter and that each completed job's run.Tx() effect was applied once.
//
// The harness runs in the chaos container, the only service that mounts the
// Docker socket, and only under the chaos profile of the separate jobqueue-chaos
// Compose project. It acts only on that project's worker-1, worker-2 and
// postgres containers. scripts/chaos.sh drives it; see `make chaos`,
// `make chaos-one` and `make chaos-smoke`. The unit tests here run in
// `make test` without the chaos stack.
package chaos
