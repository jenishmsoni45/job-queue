# Black Friday: FIFO vs DRR

Written by `make bench` (design §17) on 2026-11-14T19:41:13Z. Every number here comes from this directory's `run.json` and `summary.json`.

**Overall: FAIL**

## Targets

| Check | Result | Detail |
|---|---|---|
| P5-6: the runs differ only in the scheduler | PASS | effective configs differ at [services.dispatcher.environment.QUEUED_SCHEDULER]; bench config sha256 47d2bb6a3424 vs 47d2bb6a3424 |
| NFR-9: Black Friday fairness (DRR run) | FAIL | 1 spike buckets over their bound; closest: shop-08.mockshop.test: worst bucket p99 253.5 ms against a bound of 191.9 ms |
| NFR-11: one effect per job (both runs) | PASS | FIFO: 274776 completed, 0 without exactly one mutation, 0 keys applied more than once, 0 mutations for no job; DRR: 253383 completed, 0 without exactly one mutation, 0 keys applied more than once, 0 mutations for no job |
| NFR-12: baseline keeps up (both runs) | PASS | limit 40 jobs; worst FIFO 10, worst DRR 10 |

## Validity

| Check | Result | Detail |
|---|---|---|
| FIFO: loadgen drained the spike's backlog | PASS | drained true, drain timed out false, interrupted false |
| FIFO: loadgen kept its schedule | PASS | 0 late ticks |
| FIFO: no enqueue failed | PASS | 0 jobs in failed batches |
| FIFO: every accepted job finished | PASS | 0 of 274776 jobs not completed or dead |
| DRR: loadgen drained the spike's backlog | PASS | drained true, drain timed out false, interrupted false |
| DRR: loadgen kept its schedule | PASS | 0 late ticks |
| DRR: no enqueue failed | PASS | 0 jobs in failed batches |
| DRR: every accepted job finished | PASS | 0 of 253383 jobs not completed or dead |
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
| shop-02.mockshop.test | 43.3 | 186.7 | 114.2 | PASS | 162037.9 |
| shop-03.mockshop.test | 42.8 | 185.5 | 85.6 | PASS | 162054.0 |
| shop-04.mockshop.test | 44.3 | 188.5 | 100.5 | PASS | 162037.3 |
| shop-05.mockshop.test | 43.6 | 187.2 | 153.9 | PASS | 162038.8 |
| shop-06.mockshop.test | 45.2 | 190.3 | 143.0 | PASS | 162015.3 |
| shop-07.mockshop.test | 44.1 | 188.2 | 148.8 | PASS | 162039.0 |
| shop-08.mockshop.test | 45.9 | 191.9 | 253.5 | FAIL | 161970.7 |
| shop-09.mockshop.test | 46.3 | 192.6 | 135.5 | PASS | 161973.9 |
| shop-10.mockshop.test | 45.4 | 190.9 | 127.5 | PASS | 162049.9 |

## NFR-12: most jobs in a shop's backlog during the warm-up

Limit: 40 jobs (2 s of arrivals).

| Shop | FIFO | DRR |
|---|---|---|
| shop-01.mockshop.test | 7 | 7 |
| shop-02.mockshop.test | 8 | 8 |
| shop-03.mockshop.test | 8 | 8 |
| shop-04.mockshop.test | 10 | 10 |
| shop-05.mockshop.test | 8 | 8 |
| shop-06.mockshop.test | 7 | 7 |
| shop-07.mockshop.test | 8 | 8 |
| shop-08.mockshop.test | 9 | 9 |
| shop-09.mockshop.test | 9 | 9 |
| shop-10.mockshop.test | 9 | 9 |

## NFR-11: one effect per job

| Run | Completed jobs | Without exactly one mutation | Keys applied more than once | Mutations for no job | Claims (runs) | Reruns | Mutations applied |
|---|---|---|---|---|---|---|---|
| FIFO | 274776 | 0 | 0 | 0 | 274776 | 0 | 274776 |
| DRR | 253383 | 0 | 0 | 0 | 253383 | 0 | 253383 |

## Runs

Published numbers with no target (requirements §3).

| Run | Jobs | Completed | Dead | Shed (429) | Duration (s) | Completed per s | Queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| FIFO | 274776 | 274776 | 0 | 438030 | 653.5 | 420.5 | 127703.1 / 161677.4 | 127733.8 / 161709.2 |
| DRR | 253383 | 253383 | 0 | 459265 | 587.2 | 431.5 | 51841.9 / 161003.9 | 51872.5 / 161034.9 |

### FIFO, per shop

| Shop | Jobs | Shed (429) | Queue wait p50 / p99 (ms) | Spike queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|
| shop-01.mockshop.test | 169948 | 438030 | 127680.2 / 161691.5 | 128730.2 / 161700.6 | 127711.5 / 161722.4 |
| shop-02.mockshop.test | 11592 | 0 | 125881.6 / 161657.2 | 149018.1 / 161789.3 | 125912.3 / 161688.1 |
| shop-03.mockshop.test | 11631 | 0 | 128653.1 / 161663.6 | 149497.6 / 161788.9 | 128684.2 / 161694.5 |
| shop-04.mockshop.test | 11826 | 0 | 126940.8 / 161666.4 | 149414.7 / 161788.5 | 126972.3 / 161698.0 |
| shop-05.mockshop.test | 11624 | 0 | 127914.2 / 161683.2 | 149447.5 / 161800.7 | 127945.0 / 161714.4 |
| shop-06.mockshop.test | 11507 | 0 | 128193.8 / 161651.7 | 149426.1 / 161767.8 | 128225.0 / 161682.8 |
| shop-07.mockshop.test | 11632 | 0 | 127971.8 / 161667.3 | 149343.1 / 161773.2 | 128002.4 / 161698.2 |
| shop-08.mockshop.test | 11738 | 0 | 128207.5 / 161597.5 | 149445.4 / 161749.3 | 128238.3 / 161628.6 |
| shop-09.mockshop.test | 11501 | 0 | 127998.7 / 161594.4 | 149330.7 / 161757.8 | 128029.8 / 161625.6 |
| shop-10.mockshop.test | 11777 | 0 | 127918.2 / 161623.7 | 149414.1 / 161757.8 | 127949.5 / 161655.3 |

### DRR, per shop

| Shop | Jobs | Shed (429) | Queue wait p50 / p99 (ms) | Spike queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|
| shop-01.mockshop.test | 148694 | 459265 | 159378.8 / 161075.6 | 159437.1 / 161077.6 | 159409.5 / 161106.4 |
| shop-02.mockshop.test | 11572 | 0 | 16.1 / 67.3 | 18.0 / 67.4 | 46.9 / 98.3 |
| shop-03.mockshop.test | 11615 | 0 | 17.3 / 67.7 | 19.7 / 67.6 | 48.1 / 98.7 |
| shop-04.mockshop.test | 11812 | 0 | 18.9 / 67.9 | 20.9 / 67.4 | 49.7 / 98.9 |
| shop-05.mockshop.test | 11607 | 0 | 18.6 / 68.1 | 21.0 / 69.0 | 49.4 / 99.0 |
| shop-06.mockshop.test | 11497 | 0 | 20.4 / 68.7 | 22.2 / 69.6 | 51.2 / 99.5 |
| shop-07.mockshop.test | 11613 | 0 | 22.0 / 68.4 | 24.4 / 68.5 | 52.8 / 99.3 |
| shop-08.mockshop.test | 11719 | 0 | 23.2 / 69.0 | 25.7 / 70.2 | 53.9 / 100.1 |
| shop-09.mockshop.test | 11492 | 0 | 24.9 / 69.1 | 27.6 / 69.3 | 55.7 / 100.0 |
| shop-10.mockshop.test | 11762 | 0 | 27.0 / 69.3 | 29.3 / 69.2 | 57.7 / 100.4 |

## Enqueue latency (loadgen's batch requests)

| Run | Batches | p50 (ms) | p99 (ms) | Max (ms) |
|---|---|---|---|---|
| FIFO | 50796 | 2.4 | 13.8 | 190.6 |
| DRR | 50720 | 1.8 | 10.4 | 69.9 |

## Config diff

The runs' effective Compose configs (secrets redacted) differ at: `services.dispatcher.environment.QUEUED_SCHEDULER`.

## Chart

![p99 queue wait per 10 s bucket, FIFO and DRR](blackfriday.svg)
