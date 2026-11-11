# bench

Measured results live in `bench/results/`, and those files are the only source of the measured numbers in `README.md` and `docs/` (D-005).

- `bench/blackfriday.json`: the Black Friday config (design §17, D-009). Every process of both runs reads it: the shops, the `order.tag` kind, `mockshop`'s buckets, and the `loadgen` section with the seed and the timeline (D-050). Caps, token buckets and `mockshop` buckets are set so they never bind.
- `bench/results/<timestamp>/`: one directory per `make bench` (D-052):
  - `machine.json`: the host, Docker Desktop's CPUs and memory, the git commit, and whether `make up` was running
  - `fifo/` and `drr/`, one per run:
    - `loadgen.json`: what `loadgen` sent, accepted and shed per shop, by phase, and its enqueue latency
    - `effective-config.json`: the run's effective Compose config, with passwords and secrets redacted; the report diffs the two runs' files
    - `run.json`: the run's summary: per-shop queue waits, the p99 queue wait per shop in 10 s buckets of arrival (D-051), and the NFR-9, NFR-11 and NFR-12 checks
    - `raw/`: the per-job export from Postgres and `mockshop`'s mutation log, gzipped and git-ignored
  - `summary.json`: the targets, the validity checks, and both runs' numbers without their series
  - `report.md`: PASS or FAIL for each target, and the tables behind them
  - `blackfriday.svg`: each shop's p99 queue wait per 10 s bucket, FIFO and DRR side by side
- `bench/results/chaos/`: one directory per `make chaos` or `make chaos-one`, written by `tests/chaos` (design §18). Each holds one JSON file per run (seed, fault schedule, duration and check results), `summary.json` and `summary.md`, and `recovery.json` from the crash-recovery scenario (NFR-10).
