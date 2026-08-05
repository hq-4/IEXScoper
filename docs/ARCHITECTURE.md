# Architecture

## Dead Ticker Evidence-Delta V3

- `resolution_v2_schema.py` defines deterministic fact IDs, evidence fingerprints, canonical
  paths, and independent status dimensions.
- `resolution_v2_migration.py` normalizes V1 overrides, identity-only holds, ledger closures,
  lifecycle attempts, and the quarantined 25,622-era cohort without mutating legacy sources.
- `resolution_v2_store.py` owns atomic, duplicate-free JSONL fact projections.
- `resolution_v2_registry.py` is the shared SQLite request, negative-result, document-metadata,
  cumulative-metric, and resume registry. Filing bodies never enter it.
- `resolution_v2_events.py`, `resolution_v2_identity.py`, `resolution_v2_local.py`, and
  `resolution_v2_sec.py` implement evidence gates. Identity and event proof remain separate;
  symbol changes precede generic terminal language.
- `resolution_v2_lanes.py` orders known-identity event salvage before missing-identity work,
  resumes by evidence fingerprint, applies request/impact stopping policy, and persists facts
  as each row completes.
- `resolution_v2_outputs.py` emits dimension-specific queues, complete-population reconciliation,
  and the verified identity-plus-event legacy projection.
- `build_identity_verified_event_queue.py` joins independent V3 event gaps to the latest
  verified canonical identity fact and era metadata; missing or mismatched joins abort.
- `run_dead_ticker_resolution_program.py` is the only evidence-delta composition root. Dry run
  builds a deterministic stage keyed by cohort, migrated fact IDs, resolver version, and mode;
  `--apply` promotes only that exact stage with zero network calls.

Data flow:

`stable cohort → local migration/harvest → cached semantic rescoring → ordered public-primary lanes → staged facts → apply → projections/queues`

The primary invariant is that workflow closure, local observation, heuristic instrument type,
verified identity, and verified endpoint event are different facts. Eligibility requires the
applicable hard gates; absence of evidence is never converted into event proof. [CA][REH][PA]

## OpenFIGI Identity Pillar, Event Catalog & Era-Identity Enrichment

A second identity source layered on top of the V3 evidence-delta store — coverage-first
rather than SEC-grade proof-first, feeding V3 as confidence-tiered facts rather than
replacing its gates. Plan and phase-by-phase status: `docs/EVENT_CATALOG_RESOLUTION_PLAN.md`.

- `openfigi_identity_core.py` is the keyed `/v3/mapping` client (`includeUnlistedEquities=true`
  recall unlock, batch 100, backoff on 429/5xx, single-job fallback), with a resume-safe
  JSONL cache at `data/openfigi/identity_cache.jsonl` retaining ALL FIGI matches per symbol
  (unlike the older `openfigi_enrichment_core.py`, which keeps only `data[0]`).
- `build_openfigi_symbol_identities.py` runs the keyed enrichment over every symbol in
  `observation_facts.jsonl` and writes `reports/openfigi-identity/` (`symbol_figi_map.parquet`,
  `era_classes.parquet`, `summary.json`) with the authoritative instrument census.
- `openfigi_recall_experiment.py`, `openfigi_recall_full_pass.py`, and
  `openfigi_recall_metrics.py` measured the `includeUnlistedEquities` recall unlock against
  verified-dead ground truth before the full-universe pass was approved.
- `event_catalog_sources.py`, `event_catalog_fetch.py`, and `event_catalog_join.py` fetch and
  ticker-bind the SEC Form 25 corpus (display-name, issuer-name, and security-name binding)
  into a normalized event catalog under `data/event_catalog/cache/`.
- `probe_event_catalog_coverage.py` measures per-instrument-class era join yield before the
  catalog is trusted as an event source.
- `openfigi_era_binding.py` stages OpenFIGI identity candidates and Form 25 event candidates
  as V3 facts (`verification_state=candidate`), corroboration-tiered against Form 25 and SEC
  current names; nothing applies at this stage.
- `build_openfigi_identity_candidates.py` is the staging CLI; `apply_openfigi_identity_candidates.py`
  is the dry-run-first, idempotent apply step that writes confidence-tiered facts
  (`corroborated`, `openfigi_asserted` with `contested` conflict flags) into the canonical
  `identity_facts.jsonl`/`event_facts.jsonl` store. It never overwrites `verified` (SEC-grade)
  facts and skips eras that already have one.
- `build_era_identity_enriched.py` is the read-side product: it joins the best-tier identity
  (`verified` > `corroborated` > `openfigi_asserted`, `contested` excluded from the
  default-usable view) and best event fact onto every `symbol_era_id` in `symbol_eras.parquet`,
  deriving `era_span_days`, and writes `reports/era-identity/eras_identity_enriched.parquet`.
  It is the first single table joining identity/event assurance tiers onto the full era
  universe rather than requiring callers to join `data/resolution/*.jsonl` by hand.

Confidence-tier invariant: `verified` facts are SEC-grade and never rewritten by OpenFIGI
tooling; `corroborated` and `openfigi_asserted` are coverage aids with measured ~68-74%
ground-truth agreement, so any downstream join must choose its assurance tier explicitly
rather than treating all `identity_facts.jsonl` rows as equally trustworthy. [CA][IV][SFT][KBT]

## Sector/Industry (SIC) Classification — offline groundwork landed, live run pending

No sector/industry classification existed anywhere before this; OpenFIGI's `marketSector` is a
coarse asset-class bucket (`"Equity"`/`null`), not an industry field. SIC/`sicDescription` live
only in SEC's `data.sec.gov/submissions/CIK*.json` endpoint, already called elsewhere in this repo
but never read for those fields.

- `utils/sic_division_table.py` — the standard public 10-division SIC rollup (`2000-3999 →
  Manufacturing`, `7000-8999 → Services`, …), scalar and vectorized polars-expression lookups.
  Unused numeric gaps between divisions (e.g. `1800-1999`, `6800-6999`) correctly resolve to no
  division rather than being force-fit into a neighbor.
- `utils/sector_cik_reconcile.py` — reconciles the three previously-unreconciled CIK sources in
  this repo (`identity_facts.jsonl`'s `sec_date_scoped_display_names` facts, its
  `legacy_historical_override` facts via a recovered archive-URL CIK, and
  `symbol_eras_sec_enriched.parquet`'s current-ticker-match `sec_cik`) into one confidence-tiered
  best-CIK-per-era table. The current-ticker-match tier is **strictly scoped** to
  `stable_candidate`/`ipo_or_new_listing_candidate` and never applied to the four dead-ticker
  review classes — a current match on a historically dead ticker is very likely a *different*
  company that reused the symbol, so those eras get no CIK rather than a fuzzy guess.
- `utils/sec_sic_client.py` — fetches SIC/`sicDescription` for a resolved CIK by reusing
  `resolution_v2_network.CachedPrimaryClient` (not a new client) with the identical
  `source="sec_submissions"` / `request={"url": ..., "params": {}}` cache-key shape
  `resolution_v2_sec.py` already uses, so any CIK the live SEC-lane resolver has already fetched
  is a free cache hit.
- `canonical_identity_join.py` gained an additive `identity_source_url` column (the raw fact's
  `source` URL) so `sector_cik_reconcile.py` can recover CIKs from `legacy_historical_override`
  facts whose `entity_id` was migrated empty.

Verified against real production data (not just unit tests): reconciled Tier-C coverage matches
independently-confirmed ground truth almost exactly. Still pending: the fetch+join orchestration
tool (`eras_sector_enriched.parquet`), a no-CIK manual-research worklist, and a real coverage
report — deferred to a follow-up change since running it means several thousand live requests
against a government API. [CA][IV][REH][CDiP][KBT]

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
- `utils/build_stable_returns_table.py` derives close-to-close raw and log returns from the validated stable daily panel while preserving clean/dirty return flags and metadata needed for screening.
- `utils/enrich_symbol_stability_openfigi.py` enriches symbol-stability rows with OpenFIGI mapping metadata through a cache-first, rate-limited API workflow.
- `utils/openfigi_enrichment_core.py` owns OpenFIGI batching, cache lookup/write-through, response classification, and identity-risk flags.
- `utils/openfigi_enrichment_outputs.py` writes the CSV, JSONL, summary JSON, and Markdown enrichment report.
- `utils/diff_iex_entities_snapshots.py` diffs local daily IEX entity JSON snapshots and records net adds/removes, issuer/status changes, invalid snapshots, and product hints.
- `utils/build_iex_entity_enrichment.py` turns those snapshots into an entity lifecycle table and joins current IEX listing evidence onto `symbol_eras.parquet` and the stable long-window universe.
- `utils/enrich_symbol_eras_sec.py` joins SEC current ticker/CIK metadata from `company_tickers_exchange.json` onto every `symbol_era_id`. It is a no-key enrichment path for operating-company CIK triage, not a historical ticker master.
- `utils/build_dead_ticker_review_queue.py` combines SEC and IEX current evidence, manual historical identity overrides, and ticker-era continuity to produce the review queue for dead, intermittent, and partial-window symbols that still need historical identity evidence.
- `utils/instrument_classifier.py` centralizes the first-pass instrument heuristic used by the review queue. It preserves the legacy `instrument_hint` bucket and adds `instrument_type` plus `instrument_reason` for preferreds, warrants, units, rights, share classes, funds/trusts, operating companies, and ambiguous patterns.
- `utils/instrument_research_routing.py` maps instrument types to `research_route`, `recommended_evidence`, and `routing_reason` columns so manual templates and later SEC resolvers can choose the right evidence path before making network calls.
- `utils/dead_ticker_review_schema.py` centralizes dead-ticker review defaults, target review classes, and selected output columns so the builder stays below the local script complexity threshold.
- `utils/build_dead_ticker_priority_queue.py` derives the first manual-review worklist from the dead ticker review queue. It filters `historical_identity_unresolved` rows, ranks `probable_operating_company` eras ahead of non-common instruments when `instrument_type` is present, ranks delisted/acquired candidates ahead of other unresolved classes, and then sorts by `trade_rows` descending.
- `utils/build_dead_ticker_resolution_template.py` turns the priority queue into a fillable manual research CSV with proposed override/source columns.
- `utils/import_dead_ticker_manual_overrides.py` validates completed research-template rows and appends only `research_status=verified` rows into the manual override CSV, rejecting missing evidence and duplicate `symbol_era_id` values.

**The rest of this list is the archived narrative-first SEC lane** (`utils/legacy/`, ~46
files: three iteration runners plus their EDGAR/SEC evidence, workplan, and text-scoring
stage modules). It is what months of dead-ticker RCA/retry work in `docs/TASK_LIST.md`
actually built, it produced all `818` SEC-grade `verified` identity facts and the terminal
workflow ledger, and it still runs — but it plateaued at ~1% yield per era attempted (see
`docs/EVENT_CATALOG_RESOLUTION_PLAN.md`) and is no longer the primary resolution path; the
OpenFIGI pillar above superseded it as the default coverage strategy. See
`utils/legacy/README.md` for the full inventory and why each piece moved. [CDiP][KBT]

- `utils/legacy/lookup_edgar_tickers.py` performs a bounded EDGAR lead lookup with an explicit custom SEC User-Agent. It can map template symbols through the current SEC ticker directory and optionally fetch recent `data.sec.gov/submissions` metadata for current CIK matches.
- `utils/legacy/search_edgar_full_text.py` performs a broader SEC EFTS lead search over the manual-resolution template when current ticker lookup misses dead symbols. Its companion modules keep endpoint constants, output writing, and config types separate.
- `utils/legacy/run_sec_high_impact_identity_resolution_iterations.py` is the identity-first SEC composition root. It snapshots and hashes its input, harvests local EFTS payloads, resumes row state, anchors a unique date-scoped CIK, queries that CIK's recent and overlapping historical submissions shards, scores bounded event snippets, and invokes the existing override importer once. Transport, parsing/scoring, state, outputs, and runtime side effects live in separate `sec_*` modules under the 300-SLOC gate.
- Historical identity and event resolution remain separate facts. Exact filer-ticker/date/CIK evidence admits identity; only anchored-CIK actual terminal or symbol-change evidence admits import. Active/data-gap, identity-only, collision, no-evidence, and fetch-error rows remain non-importable.
- `utils/legacy/dead_ticker_workplan_automation.py` joins resumable workflow state into workplan reports. `automation_exhausted` suppresses misleading repeated automation while preserving `historical_identity_unresolved` until real evidence is imported.
- `utils/derivative_identity_resolution.py` enforces instrument-specific derivative gates. Share classes require exact child ticker, same CIK, and a near parent action; warrants, units, rights, and preferreds require explicit child/class action language and a near date. Parent-root syntax by itself is never a disposition.

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
- SEC ticker/CIK enrichment is current-biased. It hydrated `10,262` symbol eras with a single current CIK match and found `3` multiple current matches; the remaining `27,163` eras are unmatched in the current SEC ticker directory. Intermittent eras remain keyed by `symbol_era_id` because current CIK evidence does not prove historical ticker identity.
- The dead ticker review queue lives at `reports/dead-ticker-review/dead_ticker_review_queue.parquet`. After the quarantined-era remap it contains `25,622` non-stable ticker eras: `17,677` remain `historical_identity_unresolved`, `364` are `manual_verified_historical_identity`, and `5,246` carry terminal workflow dispositions. Manual overrides remain keyed by `symbol_era_id` so reused symbols cannot inherit stale issuer identity. Instrument labels are routing hints, not identity evidence.
- The unresolved priority queue lives at `reports/dead-ticker-review/unresolved_priority_queue.parquet`. It currently contains `12,431` needs-resolution eras; the top 2,500 are all probable operating-company SEC/event targets and cover `97.98%` of identity-unresolved trade volume. Resolution lanes and the impact-weighted workplan remain workflow products rather than issuer/event proof.
- The manual dead-ticker resolution workflow is documented in `docs/DEAD_TICKER_RESOLUTION.md`. EDGAR lookups are treated as current-biased leads; verified overrides still require issuer/event evidence before being added to `data/manual_overrides/historical_ticker_identities.csv`.
- The stable daily panel lives at `/media/tn/pq/derived/stable-daily-panel/stable_daily_panel.parquet`. It currently covers `2,874` stable ticker eras, `6,656,475` daily rows, and keeps quality flags in-row so analysis can filter out raw-price or volume/notional anomaly days without rescanning the quality-event parquet.
- Stable daily panel validation passed with zero hard failures: no duplicate keys, no critical nulls, no invalid OHLC rows, no nonpositive price/volume/trade-count/notional rows, no timestamp-order violations, and no mismatch between in-panel quality flags and `quality_events.parquet`. Sparse `thin` symbols can still have low observed panel-day coverage because the panel contains confirmed-trade days only.
- The stable returns table lives at `/media/tn/pq/derived/stable-returns/stable_returns.parquet`. It keeps all `6,656,475` panel rows, has `6,653,601` non-null close-to-close return observations, and marks `6,561,194` observations as clean when neither the current nor previous day has a quality event. Returns remain raw and unadjusted for splits/dividends.
- The stable returns table also includes `potential_corporate_action`, a conservative flag for large raw close-to-close jumps (`abs(return) >= 0.45`). It is a triage flag only; rows are retained so analysts can inspect them before deciding whether to exclude or adjust them.

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
