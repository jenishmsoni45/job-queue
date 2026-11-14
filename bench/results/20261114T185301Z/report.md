# Black Friday: FIFO vs DRR

Written by `make bench` (design §17) on 2026-11-14T19:14:22Z. Every number here comes from this directory's `run.json` and `summary.json`.

**Overall: PASS**

## Targets

| Check | Result | Detail |
|---|---|---|
| P5-6: the runs differ only in the scheduler | PASS | effective configs differ at [services.dispatcher.environment.QUEUED_SCHEDULER]; bench config sha256 47d2bb6a3424 vs 47d2bb6a3424 |
| NFR-9: Black Friday fairness (DRR run) | PASS | 0 spike buckets over their bound; closest: shop-08.mockshop.test: worst bucket p99 88.2 ms against a bound of 187.2 ms |
| NFR-11: one effect per job (both runs) | PASS | FIFO: 276675 completed, 0 without exactly one mutation, 0 keys applied more than once, 0 mutations for no job; DRR: 251753 completed, 0 without exactly one mutation, 0 keys applied more than once, 0 mutations for no job |
| NFR-12: baseline keeps up (both runs) | PASS | limit 40 jobs; worst FIFO 10, worst DRR 10 |

## Validity

| Check | Result | Detail |
|---|---|---|
| FIFO: loadgen drained the spike's backlog | PASS | drained true, drain timed out false, interrupted false |
| FIFO: loadgen kept its schedule | PASS | 0 late ticks |
| FIFO: no enqueue failed | PASS | 0 jobs in failed batches |
| FIFO: every accepted job finished | PASS | 0 of 276675 jobs not completed or dead |
| DRR: loadgen drained the spike's backlog | PASS | drained true, drain timed out false, interrupted false |
| DRR: loadgen kept its schedule | PASS | 0 late ticks |
| DRR: no enqueue failed | PASS | 0 jobs in failed batches |
| DRR: every accepted job finished | PASS | 0 of 251753 jobs not completed or dead |
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
| shop-02.mockshop.test | 43.1 | 186.3 | 71.1 | PASS | 170406.0 |
| shop-03.mockshop.test | 42.9 | 185.8 | 77.0 | PASS | 170393.1 |
| shop-04.mockshop.test | 43.1 | 186.1 | 74.3 | PASS | 170371.9 |
| shop-05.mockshop.test | 43.0 | 186.0 | 74.6 | PASS | 170398.7 |
| shop-06.mockshop.test | 43.0 | 186.0 | 81.7 | PASS | 170404.3 |
| shop-07.mockshop.test | 43.3 | 186.6 | 78.8 | PASS | 170373.2 |
| shop-08.mockshop.test | 43.6 | 187.2 | 88.2 | PASS | 170379.5 |
| shop-09.mockshop.test | 43.8 | 187.5 | 82.8 | PASS | 170397.7 |
| shop-10.mockshop.test | 43.7 | 187.4 | 75.7 | PASS | 170394.1 |

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
| FIFO | 276675 | 0 | 0 | 0 | 276675 | 0 | 276675 |
| DRR | 251753 | 0 | 0 | 0 | 251753 | 0 | 251753 |

## Runs

Published numbers with no target (requirements §3).

| Run | Jobs | Completed | Dead | Shed (429) | Duration (s) | Completed per s | Queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| FIFO | 276675 | 276675 | 0 | 438028 | 672.5 | 411.4 | 127634.0 / 169882.0 | 127665.9 / 169913.6 |
| DRR | 251753 | 251753 | 0 | 461912 | 592.4 | 425.0 | 48244.7 / 165393.1 | 48275.8 / 165424.0 |

### FIFO, per shop

| Shop | Jobs | Shed (429) | Queue wait p50 / p99 (ms) | Spike queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|
| shop-01.mockshop.test | 170146 | 438028 | 126016.4 / 169889.2 | 127075.1 / 169912.8 | 126047.2 / 169920.3 |
| shop-02.mockshop.test | 11760 | 0 | 128391.1 / 169879.1 | 151698.7 / 170086.9 | 128423.0 / 169910.4 |
| shop-03.mockshop.test | 11815 | 0 | 130969.5 / 169881.7 | 152708.3 / 170077.6 | 131000.2 / 169914.7 |
| shop-04.mockshop.test | 12001 | 0 | 129238.5 / 169881.7 | 152303.4 / 170075.3 | 129275.9 / 169913.6 |
| shop-05.mockshop.test | 11812 | 0 | 129935.0 / 169948.6 | 152565.2 / 170110.8 | 129966.6 / 169979.5 |
| shop-06.mockshop.test | 11723 | 0 | 130051.6 / 169850.0 | 152658.0 / 170107.9 | 130082.9 / 169881.6 |
| shop-07.mockshop.test | 11823 | 0 | 129752.0 / 169862.5 | 152211.5 / 170106.5 | 129783.1 / 169894.6 |
| shop-08.mockshop.test | 11919 | 0 | 130271.7 / 169804.0 | 152481.6 / 170041.2 | 130304.2 / 169835.0 |
| shop-09.mockshop.test | 11708 | 0 | 130112.8 / 169833.7 | 152149.5 / 170065.6 | 130145.2 / 169865.2 |
| shop-10.mockshop.test | 11968 | 0 | 129913.2 / 169873.6 | 152462.2 / 170079.0 | 129944.4 / 169904.5 |

### DRR, per shop

| Shop | Jobs | Shed (429) | Queue wait p50 / p99 (ms) | Spike queue wait p50 / p99 (ms) | End to end p50 / p99 (ms) |
|---|---|---|---|---|---|
| shop-01.mockshop.test | 146146 | 461912 | 164363.9 / 165463.1 | 164404.3 / 165466.7 | 164394.6 / 165493.6 |
| shop-02.mockshop.test | 11665 | 0 | 17.1 / 69.6 | 19.8 / 66.5 | 48.2 / 100.8 |
| shop-03.mockshop.test | 11717 | 0 | 17.6 / 70.6 | 20.6 / 69.6 | 48.6 / 101.6 |
| shop-04.mockshop.test | 11910 | 0 | 18.6 / 70.1 | 21.6 / 68.8 | 49.5 / 101.2 |
| shop-05.mockshop.test | 11699 | 0 | 19.1 / 69.8 | 22.2 / 68.6 | 49.9 / 100.9 |
| shop-06.mockshop.test | 11602 | 0 | 20.1 / 70.5 | 22.7 / 68.8 | 50.9 / 101.4 |
| shop-07.mockshop.test | 11727 | 0 | 21.0 / 71.0 | 23.8 / 69.9 | 51.7 / 102.1 |
| shop-08.mockshop.test | 11810 | 0 | 22.3 / 71.2 | 24.9 / 69.8 | 53.0 / 101.9 |
| shop-09.mockshop.test | 11595 | 0 | 23.9 / 71.3 | 26.4 / 69.8 | 54.5 / 102.4 |
| shop-10.mockshop.test | 11882 | 0 | 26.7 / 71.2 | 29.1 / 70.0 | 57.5 / 102.4 |

## Enqueue latency (loadgen's batch requests)

| Run | Batches | p50 (ms) | p99 (ms) | Max (ms) |
|---|---|---|---|---|
| FIFO | 51584 | 2.4 | 12.3 | 26.0 |
| DRR | 51148 | 1.8 | 9.6 | 42.5 |

## Config diff

The runs' effective Compose configs (secrets redacted) differ at: `services.dispatcher.environment.QUEUED_SCHEDULER`.

## Chart

![p99 queue wait per 10 s bucket, FIFO and DRR](blackfriday.svg)
