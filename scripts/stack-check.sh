#!/usr/bin/env bash
# P1-1: `make up` brings every default-profile service to healthy, and `make down`
# leaves no project container running. One-shot services must instead have
# exited 0 (D-033). Run it with `make stack-check`; it ends with the stack down.
set -euo pipefail

cd "$(dirname "$0")/.."

project=jobqueue
one_shots=" migrate "
failures=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# Keep going if `make up` fails, so the report shows which service broke and the
# stack is still taken down.
make up || fail "make up exited non-zero"

# `config --services` lists the default-profile services only.
services=$(docker compose config --services | sort)
[ -n "$services" ] || { echo "FAIL: no services in the Compose file" >&2; exit 1; }

for svc in $services; do
  if [[ "$one_shots" == *" $svc "* ]]; then
    got=$(docker compose ps --all --format '{{.State}} {{.ExitCode}}' "$svc")
    want="exited 0"
  else
    got=$(docker compose ps --format '{{.State}} {{.Health}}' "$svc")
    want="running healthy"
  fi
  if [ "$got" = "$want" ]; then
    echo "ok    $svc: $got"
  else
    fail "$svc: got '${got:-not running}', want '$want'"
  fi
done

# Every service answers through its published 127.0.0.1 port, where it has one.
for url in http://127.0.0.1:8080/healthz http://127.0.0.1:8090/healthz http://127.0.0.1:9090/-/healthy; do
  if curl --silent --fail --max-time 5 --output /dev/null "$url"; then
    echo "ok    $url"
  else
    fail "$url did not answer 200"
  fi
done

if [ "$failures" -gt 0 ]; then
  docker compose ps --all >&2
  docker compose logs --no-color --tail 50 >&2
fi

make down

left=$(docker ps --all --quiet --filter "label=com.docker.compose.project=$project")
if [ -n "$left" ]; then
  fail "containers left after make down: $(echo "$left" | tr '\n' ' ')"
else
  echo "ok    make down left no project container"
fi

if [ "$failures" -gt 0 ]; then
  echo "stack-check: $failures failure(s)" >&2
  exit 1
fi
echo "stack-check: passed"
