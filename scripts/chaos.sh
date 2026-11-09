#!/usr/bin/env bash
# Runs the chaos harness (design §18) on its own Compose project, jobqueue-chaos (D-042),
# with docker-compose.chaos.yml on top of docker-compose.yml. It starts from a fresh chaos
# stack and removes that stack, volume included, when it ends, pass or fail. The `make up`
# stack and its data are never touched.
#
#   scripts/chaos.sh --runs 100 --recovery    make chaos
#   scripts/chaos.sh --seed <n>               make chaos-one
#   scripts/chaos.sh --runs 3 --results DIR   make chaos-smoke
#
# CHAOS_BASE_SEED in the environment repeats a whole batch (D-046).
set -euo pipefail

cd "$(dirname "$0")/.."

project=jobqueue-chaos
compose=(docker compose -p "$project" -f docker-compose.yml -f docker-compose.chaos.yml)
stack=(postgres toxiproxy migrate api dispatcher worker-1 worker-2)

runs=""
seed=""
recovery=0
results=""
while [ $# -gt 0 ]; do
  case "$1" in
    --runs) runs="$2"; shift 2 ;;
    --seed) seed="$2"; shift 2 ;;
    --recovery) recovery=1; shift ;;
    --results) results="$2"; shift 2 ;;
    *) echo "usage: $0 [--runs N | --seed N] [--recovery] [--results DIR]" >&2; exit 2 ;;
  esac
done
if [ -n "$seed" ] && ! [[ "$seed" =~ ^[0-9]+$ ]]; then
  echo "CHAOS_SEED must be a whole number, got '$seed'. Usage: CHAOS_SEED=<n> make chaos-one" >&2
  exit 2
fi

cleanup() {
  "${compose[@]}" --profile chaos down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup # start from a fresh chaos database

# The harness binary is built in the main project's tools container, where the Go caches
# live (D-021), and run in the chaos container, which has the same image.
docker compose run --rm --no-deps tools go test -c -race -o .cache/chaos.test ./tests/chaos

"${compose[@]}" up --detach --build --wait --quiet-pull "${stack[@]}"

args=(-e CHAOS_RUN=1 -e "CHAOS_RECOVERY=$recovery")
[ -n "$runs" ] && args+=(-e "CHAOS_RUNS=$runs")
[ -n "$seed" ] && args+=(-e "CHAOS_SEED=$seed")
[ -n "${CHAOS_BASE_SEED:-}" ] && args+=(-e "CHAOS_BASE_SEED=$CHAOS_BASE_SEED")
[ -n "$results" ] && args+=(-e "CHAOS_RESULTS_DIR=$results")

status=0
"${compose[@]}" --profile chaos run --rm --no-deps "${args[@]}" chaos \
  /src/.cache/chaos.test -test.run '^TestChaos$' -test.v -test.timeout 6h || status=$?

if [ "$status" -ne 0 ]; then
  "${compose[@]}" ps --all >&2 || true
  "${compose[@]}" logs --no-color --tail 40 dispatcher worker-1 worker-2 >&2 || true
fi
exit "$status"
