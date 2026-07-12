## Testing

- When making code changes, run the related unit test when one is available.

## Performance

- Build benchmarks with `CMT_BENCHMARKS=ON` and an optimized Release build.
- For performance changes, capture at least five before and five after runs on
  the same machine with identical compiler flags and workload parameters.
- Use `benchmarks/run-perf.sh` for the standard workloads and Linux `perf stat`
  hardware counters. Keep only changes with repeatable improvements and no
  relevant benchmark regressions.
