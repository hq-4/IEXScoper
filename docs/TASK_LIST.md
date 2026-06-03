# Task List

- Standalone IEX parser parity benchmark harness landed under `utils/`. `[CA][PA]`
- Canonical TOPS normalization adapters landed for the benchmark path. `[CA][REH]`
- Focused benchmark, HIST index, and bounded backfill tests landed under `tests/`. `[REH]`
- Backfill utility landed for `hq-4/IEXTools` with bounded local staging and NAS publish. `[RM][PA]`
- Backfill is currently blocked by parser robustness on some 2025-2026 days. `[REH]`
- Runner hardening landed: isolated unknown message types are now quarantined and logged to a JSONL artifact instead of failing the day immediately. `[REH]`
- Threshold-based failure now exists for total and consecutive unknown messages so extreme framing loss still fails, but defaults now tolerate forward-compatible unknown-message bursts observed on `20250815`. `[REH][PA]`
- Added `utils/debug_iextools_day.py` to isolate IEXTools parse-only and normalize-only behavior without Parquet writes. `[CA][REH][PA]`
- `20250815` RCA: the early exit `139` did not reproduce in parse-only or normalize-only probes over the first 10M messages; the deterministic blocker is an unknown `0x28` message burst near 107M messages. The current IEX TOPS spec allows unknown future message types, so the runner now quarantines larger bursts before aborting. `[REH][KBT]`
- Backfill summary/report generation landed with retry-only failed-day lists, remaining missing-day lists, and resume checkpoint derivation from NAS state plus results logs. `[CDiP][REH]`
- Backfill recovery hardening landed: gzip corruption and negative-length parser failures now trigger fresh HIST URL refresh, scratch cleanup, redownload, and bounded per-day retry instead of immediate terminal failure. `[REH][RM][PA]`
- Remaining follow-up: replace brittle byte-stream header scanning with a transport-aware parser that distinguishes unknown-but-well-framed messages from framing loss. `[CA][REH][AS]`
