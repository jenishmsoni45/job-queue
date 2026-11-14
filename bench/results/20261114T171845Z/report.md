# Black Friday: FIFO vs DRR

Written by `make bench` (design §17) on 2026-11-14T17:39:51Z. Every number here comes from this directory's `run.json` and `summary.json`.

**Overall: FAIL**

## Targets

| Check | Result | Detail |
|---|---|---|
| P5-6: the runs differ only in the scheduler | PASS | effective configs differ at [services.dispatcher.environment.QUEUED_SCHEDULER]; bench config sha256 47d2bb6a3424 vs 47d2bb6a3424 |
| NFR-9: Black Friday fairness (DRR run) | FAIL | 9 spike buckets over their bound; closest: shop-05.mockshop.test: worst bucket p99 383.9 ms against a bound of 186.4 ms |
| NFR-11: one effect per job (both runs) | PASS | FIFO: 275669 completed, 0 without exactly one mutation, 0 keys applied more than once, 0 mutations for no job; DRR: 251495 completed, 0 without exactly one mutation, 0 keys applied more than once, 0 mutations for no job |
| NFR-12: baseline keeps up (both runs) | PASS | limit 40 jobs; worst FIFO 10, worst DRR 10 |

## Validity

| Check | Result | Detail |
|---|---|---|
| FIFO: loadgen drained the spike's backlog | PASS | drained true, drain timed out false, interrupted false |
| FIFO: loadgen kept its schedule | PASS | 0 late ticks |
| FIFO: no enqueue failed | PASS | 0 jobs in failed batches |
| FIFO: every accepted job finished | PASS | 0 of 275669 jobs not completed or dead |
| DRR: loadgen drained the spike's backlog | PASS | drained true, drain timed out false, interrupted false |
| DRR: loadgen kept its schedule | PASS | 0 late ticks |
| DRR: no enqueue failed | PASS | 0 jobs in failed batches |
| DRR: every accepted job finished | PASS | 0 of 251495 jobs not completed or dead |
| Both runs got the same warm-up and spike arrivals | PASS | seed 20261221; identical per-shop counts in the warm-up and the spike |

## Machine

- docker_cpus: 5
- docker_memory_bytes: 8.32016384e+09
- docker_server_version: 29.9.2
- git_commit: 06216c3e2e330654a96bc2e1bb453f2ed0460ae4
- git_dirty: true
- host_cpu: Apple M4
- host_memory_bytes: 1.7179869184e+10
- host_model: Mac16,13
- host_os: Darwin 27.0.0
- make_up_stack_running: false

Everything shares one machine, including Docker Desktop's VM, so absolute numbers are specific to it (design §17).

## NFR-9: p99 queue wait in the spike, by shop

Each shop's worst 10 s bucket of the spike, bucketed by arrival (D-051), against its bound of 2 × its warm-up p99 + 100 ms (D-027). The target applies to DRR; FIFO is shown for comparison.

| Shop | Warm-up p99 (ms) | Bound (ms) | DRR worst bucket p99 (ms) | DRR | FIFO worst bucket p99 (ms) |
|---|---|---|---|---|---|
| shop-02.mockshop.test | 43.8 | 187.5 | 221.1 | FAIL | 159947.9 |
| shop-03.mockshop.test | 44.4 | 188.8 | 375.5 | FAIL | 159948.0 |
| shop-04.mockshop.test | 43.2 | 186.4 | 339.0 | FAIL | 159974.0 |
| shop-05.mockshop.test | 43.2 | 186.4 | 383.9 | FAIL | 159912.0 |
| shop-06.mockshop.test | 43.8 | 187.6 | 189.8 | FAIL | 159883.4 |
| shop-07.mockshop.test | 43.8 | 187.6 | 318.0 | FAIL | 159911.3 |
| shop-08.mockshop.test | 44.3 | 188.7 | 333.1 | FAIL | 159950.1 |
| shop-09.mockshop.test | 43.1 | 186.3 | 286.4 | FAIL | 159948.6 |
| shop-10.mockshop.test | 44.7 | 189.4 | 387.6 | FAIL | 159945.5 |

## NFR-12: most jobs in a shop's backlog during the warm-up

Limit: 40 jobs (2 s of arrivals).

| Shop | FIFO | DRR |
|---|---|---|
| shop-01.mockshop.test | 9 | 7 |
| shop-02.mockshop.test | 8 | 8 |
| shop-03.mockshop.test | 8 | 8 |
| shop-04.mockshop.test | 10 | 10 |
| shop-05.mockshop.test | 8 | 8 |
| shop-06.mockshop.test | 8 | 7 |
| shop-07.mockshop.test | 8 | 8 |
| shop-08.mockshop.test | 9 | 9 |
| shop-09.mockshop.test | 9 | 9 |
| shop-10.mockshop.test | 9 | 9 |

## NFR-11: one effect per job

| Run | Completed jobs | Without exactly one mutation | Keys applied more than once | Mutations for no job | Claims (runs) | Reruns | Mutations applied |
|---|---|---|---|---|---|---|---|
| FIFO | 275669 | 0 | 0 | 0 | 275669 | 0 | 275669 |
| DRR | 251495 | 0 | 0 | 0 | 251495 | 0 | 251495 |

## Runs

Published numbers with no target (requirements §3).

| Run | Jobs | Completed | Dead | Shed (429) | Duration (s) | Completed per s | Queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| FIFO | 275669 | 275669 | 0 | 436763 | 650.3 | 423.9 | 125495.0 / 159627.3 | 125526.9 / 159658.1 |
| DRR | 251495 | 251495 | 0 | 461984 | 591.6 | 425.1 | 49371.8 / 165001.3 | 49402.3 / 165032.5 |

### FIFO, per shop

| Shop | Jobs | Shed (429) | Queue wait p50 / p99 (ms) | Spike queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|
| shop-01.mockshop.test | 171170 | 436763 | 125377.7 / 159642.5 | 126518.3 / 159647.9 | 125408.8 / 159673.6 |
| shop-02.mockshop.test | 11546 | 0 | 123706.7 / 159571.9 | 147563.2 / 159713.4 | 123738.0 / 159604.4 |
| shop-03.mockshop.test | 11595 | 0 | 126526.1 / 159602.8 | 148060.0 / 159712.7 | 126557.1 / 159633.7 |
| shop-04.mockshop.test | 11790 | 0 | 124737.8 / 159619.6 | 147848.5 / 159741.7 | 124769.4 / 159651.0 |
| shop-05.mockshop.test | 11590 | 0 | 125674.6 / 159637.0 | 148027.5 / 159741.5 | 125706.2 / 159668.2 |
| shop-06.mockshop.test | 11474 | 0 | 126041.6 / 159606.2 | 148087.4 / 159700.3 | 126074.4 / 159636.9 |
| shop-07.mockshop.test | 11585 | 0 | 125950.6 / 159619.6 | 147812.7 / 159710.2 | 125982.6 / 159651.0 |
| shop-08.mockshop.test | 11701 | 0 | 126177.0 / 159552.0 | 147987.3 / 159702.0 | 126209.5 / 159584.5 |
| shop-09.mockshop.test | 11475 | 0 | 125885.6 / 159550.3 | 147768.5 / 159681.5 | 125916.4 / 159581.7 |
| shop-10.mockshop.test | 11743 | 0 | 125834.6 / 159609.9 | 147959.9 / 159711.9 | 125866.1 / 159643.1 |

### DRR, per shop

| Shop | Jobs | Shed (429) | Queue wait p50 / p99 (ms) | Spike queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|
| shop-01.mockshop.test | 146065 | 461984 | 163877.4 / 165078.2 | 163942.1 / 165081.1 | 163908.3 / 165108.9 |
| shop-02.mockshop.test | 11651 | 0 | 16.0 / 70.1 | 17.9 / 68.7 | 46.9 / 101.2 |
| shop-03.mockshop.test | 11703 | 0 | 16.6 / 69.5 | 18.6 / 68.7 | 47.5 / 100.7 |
| shop-04.mockshop.test | 11889 | 0 | 17.2 / 70.0 | 19.5 / 68.6 | 48.0 / 100.8 |
| shop-05.mockshop.test | 11678 | 0 | 17.8 / 70.3 | 20.0 / 70.6 | 48.6 / 101.5 |
| shop-06.mockshop.test | 11586 | 0 | 18.9 / 70.2 | 20.6 / 69.5 | 49.7 / 101.2 |
| shop-07.mockshop.test | 11705 | 0 | 19.6 / 71.6 | 21.6 / 71.7 | 50.3 / 102.3 |
| shop-08.mockshop.test | 11793 | 0 | 21.2 / 71.4 | 23.6 / 70.9 | 52.1 / 102.3 |
| shop-09.mockshop.test | 11577 | 0 | 22.4 / 71.5 | 25.2 / 70.0 | 53.2 / 102.5 |
| shop-10.mockshop.test | 11848 | 0 | 25.7 / 72.0 | 27.8 / 72.4 | 56.5 / 102.9 |

## Enqueue latency (loadgen's batch requests)

| Run | Batches | p50 (ms) | p99 (ms) | Max (ms) |
|---|---|---|---|---|
| FIFO | 50625 | 2.3 | 12.2 | 51.9 |
| DRR | 51075 | 1.9 | 10.2 | 157.3 |

## Config diff

The runs' effective Compose configs (secrets redacted) differ at: `services.dispatcher.environment.QUEUED_SCHEDULER`.

## Chart

![p99 queue wait per 10 s bucket, FIFO and DRR](blackfriday.svg)
