# bench

Measured results live in `bench/results/`, and those files are the only source of the measured numbers in `README.md` and `docs/` (D-005).

- `bench/results/chaos/`: one directory per `make chaos` or `make chaos-one`, written by `tests/chaos` (design §18). Each holds one JSON file per run (seed, fault schedule, duration and check results), `summary.json` and `summary.md`, and `recovery.json` from the crash-recovery scenario (NFR-10).
- Experiment configs and the bench runner arrive in phase 5 (design §17).
