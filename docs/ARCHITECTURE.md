# Architecture

## Benchmark Utilities

- `utils/benchmark_iex_parsers.py` orchestrates archived-day benchmarks across external parser repos.
- `utils/iex_parser_repo_runner.py` runs a single repo/day parse-normalize-write stage inside a measured subprocess.
- `utils/iex_benchmark_adapters.py` maps repo-native message shapes into the canonical two-file TOPS Parquet contract.
- `utils/iex_benchmark_core.py` holds schema, repo, path, compression, and environment helpers.

## Backfill Utilities

- `utils/backfill_tops_iextools.py` runs the bounded TOPS backfill workflow with one raw `.pcap.gz` per worker, local NVMe staging, NAS publish, and cleanup after verified publish.
- `utils/iextools_backfill_core.py` owns scratch paths, publish paths, and NAS transfer/verification helpers.
- `utils/iextools_backfill_reporting.py` classifies effective failures, aggregates unknown-type frequencies, derives retry-only and remaining-day lists, and computes the resume checkpoint.
- `utils/iextools_backfill_recovery.py` centralizes retryable corruption/desync signatures and runner-failure extraction so the backfill can decide when to discard scratch state and retry a day.
- `utils/parse_iex_hist_index.py` downloads and parses the live HIST index so workers can refresh expiring Google Cloud Storage URLs before each day starts.
- `utils/summarize_iextools_backfill.py` renders summary artifacts from the backfill results log plus the current NAS parquet state.

## Current Failure Mode

- `hq-4/IEXTools` is fast and matched parity on sampled benchmark days, but some 2025-2026 backfill days fail inside the upstream parser with `ProtocolException: Unknown message type: (...)`.
- The observed unknown bytes are not stable. Logged failures include `0`, `42`, `45`, `49`, `54`, `64`, `92`, `161`, `173`, `221`, and `244`.
- That spread is inconsistent with a single new official TOPS message and is more consistent with parser desynchronization after losing framing on the raw byte stream.

## Current Mitigation

- `utils/iex_parser_repo_runner.py` now quarantines unknown message types into a sidecar JSONL artifact rather than treating the first one as fatal.
- `utils/backfill_tops_iextools.py` now retries a day up to a bounded attempt count when the runner fails with corruption-style signatures such as:
  - gzip CRC failures
  - gzip decompression errors
  - negative parser message-length reads
- Each quarantine entry records parser context such as:
  - raw message type byte
  - body prefix hex
  - bytes read
  - stream offset
  - first sequence number
  - processed message count at the time of quarantine
- The runner now fails only after configurable total or consecutive unknown-message thresholds are exceeded.
- Backfill status can now be summarized independently of the worker run:
  - effective success/failure counts
  - failure reason by day
  - unknown message byte frequencies
  - average runtime and output sizes
  - retry-only failed days
  - unattempted missing days
  - last contiguous published day and suggested resume day

## Robust Replacement Direction

- The current `IEXTools` parser scans the decompressed byte stream for a hard-coded IEX-TP header and then trusts message lengths and type bytes.
- The more robust design is:
  - parse PCAP records explicitly
  - extract UDP payloads explicitly
  - parse IEX-TP segment headers and message blocks from validated payload boundaries
  - treat unknown-but-well-framed messages as quarantinable records
  - treat invalid lengths or broken segment structure as framing/corruption failures
