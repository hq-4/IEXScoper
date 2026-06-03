# Security

- Benchmark inputs are local archived files under `/media/tn/iex`; no live IEX HIST download is part of the benchmark path.
- The harness clones parser source repos only when the local benchmark cache is missing.
- Generated benchmark outputs are confined to the configured temp output root.
