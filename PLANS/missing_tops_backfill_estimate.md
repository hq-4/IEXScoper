# Missing TOPS Backfill Estimate

## Scope

- Your Parquet set ends in April 2025.
- The live HIST index currently has TOPS through `2026-05-01`.
- Missing TOPS window used for this estimate: `2025-05-01` through `2026-05-01`.

## Remaining Workload

- Trading days: `252`
- TOPS files: `252`
- Total compressed input: `2,575,636,547,782` bytes
- Total compressed input size: about `2,398.75 GiB`
- Average compressed input per day: about `9.52 GiB`

## Benchmark Basis

- Parser used: `hq-4/IEXTools`
- Observed end-to-end benchmark rate across sampled days: about `3.12 GiB/hour`
- This rate includes parse, normalize, and Parquet write, not just parsing.

## Runtime Estimate

- `1` worker: about `768.6 hours`
- `1` worker: about `32.0 days` running `24/7`
- `2` workers: about `16.0 days`
- `3` workers: about `10.7 days`

## Planning Range

- `1` worker: plan for `30-36 days`
- `2` workers: plan for `15-18 days`

## Assumptions

- You use `IEXTools`, not `rob-blackbourn`
- TOPS only
- Similar end-to-end throughput to the benchmarked runs
- Reads are staged locally or otherwise not repeatedly bottlenecked by SMB
- No major reruns, failures, or additional validation passes

## Important Caveat

- The remaining days are larger than the benchmark sample on average, so this should be treated as a planning estimate, not an exact completion guarantee.
