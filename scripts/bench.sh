#!/usr/bin/env bash
# Runs the Black Friday experiment (design §17) on its own Compose project, jobqueue-bench
# (D-053), with docker-compose.bench.yml on top of docker-compose.yml. For each scheduler,
# FIFO then DRR, it brings the bench stack up fresh (volume removed), runs loadgen, and has
# the bench runner wait for every accepted job and export the run. Then it writes the report.
# The bench stack is removed when it ends, pass or fail; the `make up` stack is never touched.
#
#   scripts/bench.sh    make bench
#
# Results land in bench/results/<UTC timestamp>/: machine.json, fifo/ and drr/ (each with
# loadgen.json, effective-config.json, run.json and raw/, which is git-ignored), summary.json,
# report.md and blackfriday.svg. It exits 1 if any target or validity check fails (P5-7).
set -euo pipefail

cd "$(dirname "$0")/.."

project=jobqueue-bench
compose=(docker compose -p "$project" -f docker-compose.yml -f docker-compose.bench.yml)
stack=(postgres migrate api dispatcher worker-1 worker-2 mockshop)
stamp=$(date -u +%Y%m%dT%H%M%SZ)
out="bench/results/$stamp"

cleanup() {
  "${compose[@]}" --profile bench down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

# The `make up` stack would compete for CPU, and the report can't tell (D-053).
dev_running=false
if [ -n "$(docker compose -p jobqueue ps --quiet 2>/dev/null)" ]; then
  dev_running=true
  echo "warning: the make up stack (jobqueue) is running and will skew the bench; stop it with make down" >&2
fi

# The runner binary is built in the main project's tools container, where the Go caches
# live (D-021), and run in the bench container, which has the same image.
mkdir -p .cache "$out"
docker compose run --rm --no-deps tools go build -o .cache/bench ./cmd/bench

# The machine, for the report (design §17: absolute numbers are machine-specific).
json_str() { printf '"%s"' "$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g')"; }
{
  printf '{\n'
  printf '  "host_model": %s,\n' "$(json_str "$(sysctl -n hw.model 2>/dev/null || uname -m)")"
  printf '  "host_cpu": %s,\n' "$(json_str "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -p)")"
  printf '  "host_memory_bytes": %s,\n' "$(sysctl -n hw.memsize 2>/dev/null || echo 0)"
  printf '  "host_os": %s,\n' "$(json_str "$(uname -sr)")"
  printf '  "docker_cpus": %s,\n' "$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 0)"
  printf '  "docker_memory_bytes": %s,\n' "$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
  printf '  "docker_server_version": %s,\n' "$(json_str "$(docker info --format '{{.ServerVersion}}' 2>/dev/null || echo unknown)")"
  printf '  "git_commit": %s,\n' "$(json_str "$(git rev-parse HEAD 2>/dev/null || echo unknown)")"
  printf '  "git_dirty": %s,\n' "$([ -n "$(git status --porcelain 2>/dev/null)" ] && echo true || echo false)"
  printf '  "make_up_stack_running": %s\n' "$dev_running"
  printf '}\n'
} > "$out/machine.json"

for scheduler in fifo drr; do
  echo "== $scheduler run" >&2
  run="$out/$scheduler"
  mkdir -p "$run"
  cleanup
  QUEUED_SCHEDULER=$scheduler "${compose[@]}" up --detach --build --wait --quiet-pull "${stack[@]}"
  QUEUED_SCHEDULER=$scheduler "${compose[@]}" --profile bench config --format json --no-path-resolution \
    > ".cache/bench-$scheduler-compose.json"
  "${compose[@]}" --profile bench run --rm --no-deps -T loadgen > "$run/loadgen.json"
  "${compose[@]}" --profile bench run --rm --no-deps -T bench \
    /src/.cache/bench collect -run "/src/$run" -scheduler "$scheduler" -compose "/src/.cache/bench-$scheduler-compose.json"
done

echo "== report" >&2
status=0
docker compose run --rm --no-deps -T tools .cache/bench report -dir "$out" || status=$?
echo "results: $out" >&2
exit "$status"
