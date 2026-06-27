# Architecture

## Benchmark Utilities

- `utils/benchmark_iex_parsers.py` orchestrates archived-day benchmarks across external parser repos.
- `utils/iex_parser_repo_runner.py` runs a single repo/day parse-normalize-write stage inside a measured subprocess.
- `utils/iex_benchmark_adapters.py` maps repo-native message shapes into the canonical two-file TOPS Parquet contract.
- `utils/iex_benchmark_core.py` holds schema, repo, path, compression, and environment helpers.

## Backfill Utilities

- `utils/backfill_tops_iextools.py` runs the bounded TOPS backfill workflow with one raw `.pcap.gz` per worker, local NVMe staging, NAS publish, and cleanup after verified publish. Raw staged filenames preserve the HIST TOPS protocol version, while published Parquet filenames preserve the canonical existing `TOPS1.6` output contract.
- `utils/iextools_backfill_core.py` owns scratch paths, publish paths, and NAS transfer/verification helpers.
- `utils/iextools_backfill_reporting.py` classifies effective failures, aggregates unknown-type frequencies, derives retry-only and remaining-day lists, and computes the resume checkpoint.
- `utils/iextools_backfill_recovery.py` centralizes retryable corruption/desync signatures and runner-failure extraction so the backfill can decide when to discard scratch state and retry a day.
- `utils/iextools_price_repair.py` audits and repairs published hq-4 backfill Parquet files whose float price columns are null while lossless integer price columns are populated. It streams row groups, verifies schema/row-count/row-group invariants, and writes same-directory temp files before atomic replacement.
- `utils/repair_iextools_price_columns.py` is the CLI wrapper for price-column audit/repair. Audit is the default; `--apply` is required for NAS mutation.
- `utils/iex_transport_payloads.py` detects pcap-ng/classic pcap captures and extracts UDP payload bytes for parser paths that do not understand packet containers directly. It recognizes TOPS 1.5 and TOPS 1.6 IEX-TP protocol IDs.
- `utils/parse_iex_hist_index.py` downloads and parses the live HIST index so workers can refresh expiring Google Cloud Storage URLs before each day starts.
- `utils/summarize_iextools_backfill.py` renders summary artifacts from the backfill results log plus the current NAS parquet state.
- `utils/build_symbol_stability_audit.py` scans completed TOPS main Parquet files and builds ticker-era continuity artifacts. It intentionally does not assert issuer identity; CIK/FIGI/CUSIP enrichment remains a separate security-master layer.
- `utils/symbol_eras.py` splits ticker observations at major calendar gaps and writes `symbol_eras.{parquet,csv,jsonl}`. This is the point-in-time analysis key layer: downstream OHLC/statistics should join on `symbol_era_id` plus date rather than treating ticker text alone as issuer identity.
- `utils/build_daily_trade_bars.py` materializes confirmed `TradeReport` rows into daily OHLCV bars keyed by `symbol_era_id`. It reads only main TOPS Parquet files, writes day-partitioned derived Parquet files, and skips existing outputs by default for resumable long scans.
- `utils/build_stable_long_window_universe.py` joins `long_window_candidate` symbol eras to confirmed-trade daily bars and writes the stable ticker-era universe with trade-day coverage and liquidity tiers.
- `utils/build_stable_universe_quality_report.py` audits the stable long-window universe against daily confirmed-trade bars for OHLC consistency, nonpositive/near-zero prices, extreme raw close-to-close returns, and volume/notional outliers.
- `utils/build_stable_daily_panel.py` materializes the first research-ready table: stable confirmed-trade daily OHLCV joined to IEX entity evidence and daily quality-event flags.
- `utils/validate_stable_daily_panel.py` validates the stable daily panel contract: required columns, null counts, duplicate `(day, symbol_era_id)` keys, OHLC invariants, nonpositive trade metrics, timestamp order, quality-flag consistency, and quality-event source parity.
- `utils/enrich_symbol_stability_openfigi.py` enriches symbol-stability rows with OpenFIGI mapping metadata through a cache-first, rate-limited API workflow.
- `utils/openfigi_enrichment_core.py` owns OpenFIGI batching, cache lookup/write-through, response classification, and identity-risk flags.
- `utils/openfigi_enrichment_outputs.py` writes the CSV, JSONL, summary JSON, and Markdown enrichment report.
- `utils/diff_iex_entities_snapshots.py` diffs local daily IEX entity JSON snapshots and records net adds/removes, issuer/status changes, invalid snapshots, and product hints.
- `utils/build_iex_entity_enrichment.py` turns those snapshots into an entity lifecycle table and joins current IEX listing evidence onto `symbol_eras.parquet` and the stable long-window universe.

## Parquet Repair Mode

- Existing NAS Parquet outputs are immutable by default: normal backfill publish refuses to overwrite an existing main/quote pair.
- Explicit repair requires both `--days` and `--replace-existing`, which prevents accidental broad overwrites.
- Repair workers still stage one raw `.pcap.gz` per worker under the selected scratch root, regenerate the main and quote Parquet pair locally, verify the pair, copy to same-directory temporary files on the NAS, verify those temporary NAS files, and then atomically replace the final paths.
- `--min-scratch-free-gb` guards local NVMe headroom before each day starts; the repair pass used one worker and a `120 GB` minimum for the nine unreadable published main Parquet files found by the symbol-stability scan.

## Analysis Utility Flow

- Symbol continuity analysis is deliberately two-stage:
  - first, `build_symbol_stability_audit.py` classifies ticker-era continuity from local TOPS Parquet only
  - second, `enrich_symbol_stability_openfigi.py` maps those ticker eras to current OpenFIGI metadata for review triage
- `symbol_eras.parquet` is generated from the same scan as the symbol-stability report. A ticker with major observation gaps becomes multiple era rows, each with `symbol_era_id`, `first_day`, `last_day`, `recommended_use`, and `identity_status`.
- Confirmed-trade daily bars are generated from main TOPS files after `symbol_eras.parquet` exists. The derived output shape is one row per `day` and `symbol_era_id`, with OHLC, volume, trade count, notional, VWAP, and first/last trade timestamps. QuoteUpdate files are intentionally excluded until market-structure analysis is needed.
- The stable long-window universe is generated from `recommended_use == long_window_candidate` eras only. Its liquidity tiers are based on confirmed-trade median daily notional and trade-day coverage; they are screening labels, not issuer identity claims.
- Stable universe quality reports are a pre-backtest gate. Extreme raw returns are not automatically errors because raw TOPS prices are unadjusted for splits/corporate actions, but every flagged row should be reviewed before using raw returns in strategy research.
- OpenFIGI enrichment is not treated as a historical security master. It flags unresolved, multiple-match, ticker-mismatch, stable-match, and needs-review cases so downstream analysis can decide which tickers require licensed CUSIP/ISIN or exchange listing-history validation.
- The OpenFIGI cache is append-only JSONL under the selected report root, so repeated enrichment runs avoid duplicate API calls for the same ticker/exchange/market-sector request.
- IEX entity snapshot enrichment is a current/listing-evidence layer, not historical identity proof. The local snapshot window currently runs from `2026-02-22` to `2026-06-26`; enriched rows include `iex_entity_confidence` so downstream analysis can distinguish direct snapshot overlap, current-symbol-only matches, removed-before-latest matches, changed issuer/status rows, and unmatched ticker eras.
- The stable daily panel lives at `/media/tn/pq/derived/stable-daily-panel/stable_daily_panel.parquet`. It currently covers `2,874` stable ticker eras, `6,656,475` daily rows, and keeps quality flags in-row so analysis can filter out raw-price or volume/notional anomaly days without rescanning the quality-event parquet.
- Stable daily panel validation passed with zero hard failures: no duplicate keys, no critical nulls, no invalid OHLC rows, no nonpositive price/volume/trade-count/notional rows, no timestamp-order violations, and no mismatch between in-panel quality flags and `quality_events.parquet`. Sparse `thin` symbols can still have low observed panel-day coverage because the panel contains confirmed-trade days only.

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
- The current mitigation implements the first transport boundary for `hq-4/IEXTools`: pcap-ng and classic pcap inputs are converted to a worker-local concatenated UDP payload stream before the IEXTools parser scans for IEX-TP headers. This covers modern HIST files whose public name remains `IEXTP1_TOPS1.6.pcap.gz` but whose internal capture comments reference `TOPS620`, and older 2016-2017 TOPS 1.5 files whose IEX-TP protocol ID is `0x8002`.
- RCA for the 2017 failure batch: the backfill previously staged TOPS 1.5 downloads as `TOPS1.6` and skipped UDP extraction for classic pcap, so `IEXTools` scanned compressed pcap bytes for a TOPS 1.6 header until EOF and raised `IndexError` in `_get_session_id`. The owned runner now passes `--tops-version 1.5` for HIST 1.5 records and extracts classic pcap UDP payload streams before parsing.
- Parallel backfill workers refresh the expiring HIST URL index under a shared lock, and index downloads use a temp-file write followed by atomic replace. This prevents one worker from reading a partially written JSON index while another worker refreshes links.
- Published files created before the hq-4 slot-price adapter fix can be repaired without reparsing when `price_int`, `bid_price_int`, or `ask_price_int` are populated. The repair derives float prices with the canonical IEX scale (`integer / 10000`) and preserves existing non-null float values.
- Second 2017 RCA pass found two distinct remaining causes. Some November 2017 HIST days list both a tiny TOPS 1.5 placeholder file and a full TOPS 1.6 file; `choose_tops_record()` now selects the largest TOPS record so workers do not parse placeholder captures with zero IEX payloads. Earlier 2017 short-buffer failures were caused by hq-4's TOPS 1.5 `TradeBreak` decoder expecting a 37-byte body while the wire files contain 41-byte bodies (`<Bq8sLqqxxxx`); the runner now applies an idempotent hq-4 compatibility patch before opening TOPS 1.5 files.
