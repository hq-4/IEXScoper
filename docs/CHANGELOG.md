# Changelog

## Unreleased

- feat: backfill `identity_issuer` from IEX's own entity snapshots and detect ticker
  renames/still-active-under-original-symbol via SEC's current ticker list — closes a real gap a
  worklist spot-check surfaced (`GPS`→`GAP`, `BK`→`BNY`, `PSTG`→`P` were misclassified as
  delisted; `CORZ`'s end date was a stale vendor-window artifact, not a real event). Root cause:
  OpenFIGI's ticker-keyed lookup is current-listing-biased (same blind spot as Tier C, one layer
  upstream) — a renamed-away ticker string returns zero FIGI matches, so `identity_issuer` never
  gets populated. `utils/sector_enrichment_inputs.load_iex_fallback_names`/
  `apply_iex_fallback_issuer` backfill it from `iex_latest_issuer` (already-ingested local data,
  zero network cost) without ever overwriting a real assertion. `utils/ticker_continuity.py` reads
  `tickers`/`exchanges` from the same SEC submissions payload already fetched for SIC (zero
  additional network cost) and derives `continuity_status`
  (`terminal`/`still_active_same_symbol`/`renamed_or_successor`) per era. Real run (17 network
  requests, mostly cache hits): `sec_name_matched` 1,948 -> 2,015; manual-research worklist 14,559
  -> 14,491 eras, 389M -> 378M trade rows; `has_googleable_name` rows 5,155 -> 6,513. Across the
  whole universe, `continuity_status` classified 740 eras `renamed_or_successor` and 7,915
  `still_active_same_symbol` — a signal that was structurally invisible before. Diagnosed but not
  yet fixed: `CFLT`/`CORZ`/`PXD` still fail to resolve due to a separate Tier E search-recall gap
  (EDGAR's literal prefix-match search rejects the raw OpenFIGI/IEX name; a shorter truncated
  query finds the right CIK) `[CA][IV][REH][CDiP][KBT]`

- feat: add EDGAR company-name-search CIK resolution (`sector_cik_reconcile.py` Tier E) for
  issuers no longer in SEC's current listings at all — genuinely deregistered/merged/dissolved
  companies Tier D structurally can't reach. `utils/sec_company_search_client.py` calls EDGAR's
  classic `browse-edgar` company search (confirmed live to return historical/inactive
  registrants) through `CachedPrimaryClient.get_json()`'s new backward-compatible
  `parse_response`/`is_negative` hooks for non-JSON responses; `utils/edgar_company_search_match.py`
  accepts a match only when exactly one candidate's actual registrant name validates against the
  query name (rejected a real single-candidate false lead — `180 Life Sciences Corp` → a
  wrong-company hit — on live data); `utils/build_edgar_company_search_matches.py` batches it over
  every unique unresolved issuer name, rate-limited, and now survives a transient SEC 5xx on one
  name (`fetch_error` status, batch continues) after the first live run hit a real `503` ~10
  minutes in and lost all in-progress results before this fix. Real run: 969/4,453 unique names
  matched (21.8%); manual-research worklist dropped 15,882 -> 14,559 eras (492M -> 389M trade
  rows) `[CA][IV][REH][CDiP][KBT]`

- feat: add a descriptor-stripping fallback to `utils.sec_name_cik_lookup.match_by_name` — strips
  trailing Bloomberg/OpenFIGI security-descriptor suffixes (`-CW23`, `-ADR`, `W/I`, `-CLASS A`, …)
  before a second exact-match attempt against SEC's current company-name list; still strict exact
  matching, no fuzziness added. Recovers 299 more era rows (35.8M trade rows). A token-subset/fuzzy
  matcher was evaluated and rejected after it produced a real wrong-company match on live data.
  Manual-research worklist: 16,181 -> 15,882 eras, 528M -> 492M trade rows `[CA][IV][REH][CDiP][KBT]`

- feat: shrink the sector manual-research worklist by automating what didn't need to be manual —
  `utils/build_openfigi_stable_universe.py` extends OpenFIGI classification to the
  `stable_candidate`/`ipo_or_new_listing_candidate` universe (11,244 eras, 43.3% funds/ETFs), and
  `utils/sec_name_cik_lookup.py` adds a new confidence tier (`sector_cik_reconcile.py` Tier D)
  matching an era's OpenFIGI-asserted issuer name against SEC's already-fetched company-name list
  with zero new network calls, rejecting ambiguous matches rather than guessing. Live re-run (6,562
  distinct CIKs, zero errors): eras with a real SIC+sector rose 6,836 -> 8,417; the manual-research
  worklist dropped 29,597 eras/1.12B trade rows -> 16,181 eras/528M trade rows (45% reduction), with
  11,767 eras now correctly excluded as funds rather than treated as research targets
  `[CA][IV][REH][CDiP][KBT]`

- feat: land SIC/sector classification's live fetch orchestration (`utils/build_era_sector_enriched.py`)
  and the no-CIK manual-research worklist (`utils/build_sector_manual_research_worklist.py`), and run
  both for real — 6,087 distinct CIKs fetched from SEC's submissions endpoint at ~3.3 req/sec with
  zero errors in ~39 minutes, yielding 6,836 eras (18.5% of the 36,866-era universe) with a real SIC
  and sector at a 93.9% fill rate on resolved CIKs; the 29,597-era no-CIK remainder is now a ranked
  manual-research worklist, 15,762 rows of which already carry a googleable OpenFIGI-asserted issuer
  name `[CA][IV][REH][CDiP][KBT]`

- feat: add the offline groundwork for SIC/sector classification — `utils/sic_division_table.py`
  (the standard public 10-division SIC rollup), `utils/sector_cik_reconcile.py` (reconciles the
  three previously-unreconciled CIK sources into one confidence-tiered best-CIK-per-era table,
  strictly scoped so a current-listing ticker match is never applied to a dead-ticker review
  class), and `utils/sec_sic_client.py` (a thin SIC/`sicDescription` fetcher reusing
  `resolution_v2_network.CachedPrimaryClient`'s existing cache/retry/rate-limit machinery with
  the identical cache-key shape `resolution_v2_sec.py` already uses, so any CIK the live SEC-lane
  resolver already fetched is a free cache hit). `canonical_identity_join.py` gained an additive
  `identity_source_url` column to recover CIKs embedded in `legacy_historical_override` facts'
  archive URLs. Verified against real production data: reconciled Tier-C coverage matches the
  independently-confirmed ground truth almost exactly (stable_candidate 2,330/2,872, `ipo_or_new_
  listing_candidate` 4,124/8,372 exactly); ~6,087 distinct CIKs identified for the fetch step.
  No live SEC fetch/join orchestration yet — that, the manual-research worklist, and docs with
  real (not estimated) coverage numbers land in a follow-up PR. `[CA][IV][REH][CDiP][KBT]`
- feat: wire the canonical confidence-tiered identity/event store into `dead_ticker_review_queue.parquet` and `unresolved_priority_queue.parquet` via a shared `utils/canonical_identity_join.py` helper, fixing both reports' ~10-day drift from the OpenFIGI pillar (legacy columns still showed the pre-OpenFIGI 364 manually-verified / 1,686-era regex fund count while the canonical store already held 818/1,580/14,179 tiered facts and a 7,395-era authoritative fund census); the priority queue now excludes 10,277 eras a usable canonical fact already covers, dropping its true unresolved count from a legacy-only 12,431 to 4,929 `[CA][REH][CDiP][KBT]`
- feat: add `utils/build_truly_missing_eras_by_year.py`, a year-by-year (by `first_day`) breakdown of the 10,368 ticker eras with no usable canonical identity fact (244.1M trade rows), with a top-10-by-volume sample per year and an explicit left-censoring caveat for the TOPS capture floor (2016-12-12) `[CA][CDiP][KBT]`
- chore: archive the narrative-first SEC resolution lane (46 files: three iteration runners plus EDGAR/SEC evidence, workplan, and text-scoring stage modules) to `utils/legacy/`, decided by parsing actual import/subprocess references rather than guessing — it plateaued at ~1% yield per `docs/EVENT_CATALOG_RESOLUTION_PLAN.md` but still produced all 818 SEC-grade `verified` facts and remains fully runnable at its new path; one real cross-boundary dependency (`derivative_identity_resolution.py` needed two generic helpers from the archived `sec_terminal_text_evidence.py`) was resolved by moving those helpers into the shared `sec_identity_evidence.py` module instead of keeping the whole lane at the top level `[CA][CSD][CDiP][KBT]`
- feat: add era×identity enriched product joining tiered identity/event facts onto all 36,866 symbol eras with a default-usable view (verified+corroborated+non-contested asserted: 15,254 eras / 790.2M trade rows) and derived era spans `[CA][KBT]`

- feat: apply OpenFIGI era-binding facts into the canonical store as confidence tiers (`corroborated` 1,580, `openfigi_asserted` 14,179 with 1,323 `contested` quarantines) plus 363 Form 25 event candidates; dry-run-first idempotent apply tool, SEC-verified facts untouched `[CA][IV][REH][SFT][KBT]`

- feat: stage OpenFIGI era-binding identity candidates (15,759 eras, 89% of the unresolved baseline, corroboration-tiered) plus 363 Form 25 event candidates as review-only V3 facts; nothing auto-applies `[CA][IV][SFT][KBT]`
- feat: add keyed OpenFIGI full-universe identity enrichment (`includeUnlistedEquities` recall unlock, 67.5% symbol match) with authoritative instrument census — fund_etf is 29.35% of eras, ~4.5× the regex estimate `[CA][IV][KBT]`
- feat: add SEC Form 25 event-catalog probe (9,691 filings, display-name/issuer/security-name ticker binding) with per-class era coverage reporting `[CA][IV][KBT]`

- docs: persist the revised entity-resolution methodology (OpenFIGI keyed identity pillar + evidence-first event catalog) in `docs/EVENT_CATALOG_RESOLUTION_PLAN.md`, with pointers from the dead-ticker workflow doc; OpenFIGI full-universe enrichment approved as Phase 1 `[CDiP][KBT][AS]`

- feat: add a canonical identity-verified/event-unproven queue with deterministic volume ranking, identity fact/date joins, and mismatch gates; current queue is 676 eras / 267.6M trade rows and its top 200 covers 229.2M `[CA][IV][PA][CDiP]`
- fix: include migrated fact IDs in V3 stage identity so changed workplans/evidence cannot silently reuse a completed stale stage `[REH][RM][KBT]`
- fix: harden V3 endpoint-event gates after a rejected 2,000-request dry run: separate terminal and symbol-change vocabulary, reject prospective/generic-effective clauses, require same-clause old/new ticker and date proof, and block unconfirmed symbol changes from falling through as delistings `[IV][REH][SFT][KBT]`
- docs: reconcile the quarantined-era Pareto queue, V3 local audit, rejected 26-event experiment, and 11 canonical-only V2 event records requiring review before apply `[CDiP][KBT][PA]`
- feat: remigrate the V2 resolution fact store onto the quarantined era build via a
  derived old→new `symbol_era_id` remap artifact (tiered exact/first-day/last-day
  matching, ambiguity-abort); legacy overrides, ledger, holds, and workplan attempts are
  translated at read time, rows on vanished eras drop with counts, and overrides on
  vanished eras abort. Cohort 26,184→25,622 eras; 818 verified identities, 127 verified
  events, 237 candidates, 454 holds preserved with zero loss; 448 weekend micro-era
  ledger closures retired; V1 store archived to `data/resolution-v1-archive/` `[CA][IV][REH][KBT]`
- fix: drop uncovered legacy era ids instead of passing them through on remap — a
  vanished old id can collide with an unrelated same-symbol era in the new build and
  misattach ledger closures (432 collisions avoided) `[REH][KBT]`
- feat: add review-only symbol-change (rename) candidate lane pairing era boundaries by
  mutual-heaviest volume recapture; recovers all 8 seed renames (FB→META … COG→CTRA) in
  the top 47 of 334 candidates from 4.6M raw pairs; IEX/SEC enrichments regenerated
  against the quarantined era build `[CA][IV][KBT]`
- fix: replace the dead `CANCEL`/`CORRECTION` sale-condition regex (unmatchable against
  parser-emitted conditions) with documented default odd-lot exclusion, real-condition test
  fixtures, and `--include-odd-lots`; `FILTER_VERSION` bumped to `v2`, existing per-second
  Parquet treated as unreconciled `[REH][IV][KBT]`
- fix: apply Trade Break messages in daily bars via `trade_id` anti-join with a
  `trade_break_row_count` summary metric; corpus measurement found 160 breaks across 6.6B
  trades (0.000002%), so historical bars impact is negligible `[REH][IV][PA]`
- fix: count `--limit-days` in trading days, tolerate corrupt days without aborting
  multi-month runs, and exit non-zero when zero days process or any day fails `[REH][KBT]`
- fix: dedupe per-second trades on `trade_id`+`symbol` only and log collisions as a
  data-quality metric, closing the retransmit/perturbed-timestamp duplicate path `[REH][IV]`
- fix: label out-of-hours timestamps as `unknown` session instead of the catch-all
  `after`/`pre` buckets `[REH][IV]`
- feat: add session-validity quarantine for weekend IEX test captures with manifest-driven
  exclusion in the stability audit and daily bars builders `[REH][IV][PA][CDiP]`
- fix: set the session trade-share floor to 5% after the initial 50% floor flagged 44 real
  2021-10..2022-02 weekdays; quarantined era rebuild removes 562 weekend micro-eras and the
  `20170826` end-date spike `[REH][PA][KBT]`
- feat: add the evidence-delta dead ticker resolution V2 program with canonical identity,
  event, observation, attempt, and research-decision facts; shared persistent cache/resume;
  hard-gated public-primary resolvers; independent gap queues; stable cohort staging; and
  zero-network apply `[CA][IV][REH][RM][SFT][PA][KBT][CDiP]`
- feat: migrate 364 identities, 127 verified events, 237 event candidates, 454 identity-only
  holds, 5,694 workflow closures, and 5,659 lifecycle attempts without rewriting legacy
  override inputs `[CA][REH][KBT]`
- fix: decode HTML/XML entities before tag removal and rank expanded date formats in the
  terminal-action sentence, recovering legacy `160`-corrupted date evidence `[REH][CSD]`
- fix: enforce subject/filer separation for Form 25, regulatory-rule date provenance,
  postponement blocking, stratified filing quotas, and two-source symbol-change confirmation
  `[REH][SFT][KBT]`
- fix: harden the global dual-sink logger with exact handler enforcement, rotating JSONL,
  required fields, bounded values, Rich tracebacks, millisecond local timestamps, and symbols
  `[REH][SFT][RM]`
- fix: bound V2 semantic date sentence scans for large SEC filings, replace applied
  research-decision projections instead of merging stale decision fact IDs, and reconcile
  recorded request totals to the actual source-request counter `[REH][PA][CA]`
- docs: record the live V2 SEC run using the approved User-Agent, including 32 applied facts,
  transport circuit-breaker stop, and unchanged rerun idempotency `[CDiP][KBT][RM]`
- feat: add resumable identity-first SEC resolution for high-impact ticker eras with conservative terminal and symbol-change import gates `[CA][IV][REH][RM][SFT][PA][CDiP]`
- feat: read overlapping historical SEC submissions shards with accession deduplication, cache reuse, and bounded retry backoff `[REH][RM][PA]`
- feat: add workplan automation-exhaustion reporting and filing-backed derivative security gates `[CA][REH][KBT][CDiP]`
- fix: emit structured progress events from the identity-first SEC runner so the Rich console and JSONL sink show active work before completion `[REH][KBT][CDiP]`
- feat: add standalone IEX parser parity benchmark harness and report generator `[CA][PA][CDiP]`
- feat: add HIST index parser and bounded `IEXTools` TOPS backfill workflow `[CA][RM][PA][CDiP]`
- feat: quarantine unknown parser message types with threshold-based runner failure `[REH][PA][CDiP]`
- feat: add IEXTools day-level parse/normalize debug probe for missing-day RCA `[CA][REH][PA]`
- feat: add backfill failure classification, summary generation, and resume/retry artifacts `[REH][CDiP][PA]`
- feat: retry corruption-style backfill day failures after scratch cleanup and fresh HIST link refresh `[REH][RM][PA][CDiP]`
- feat: add TOPS symbol stability audit for ticker-era continuity classification `[CA][KBT][PA][CDiP]`
- feat: add symbol-era outputs that split ticker observations at major gaps for point-in-time analysis keys `[CA][KBT][PA][CDiP]`
- feat: add resumable daily confirmed-trade OHLCV bars keyed by symbol era `[CA][PA][RM][CDiP]`
- feat: add stable long-window ticker-era universe report from confirmed daily bars `[CA][PA][KBT][CDiP]`
- feat: add stable universe daily-bar quality report before backtest panel construction `[REH][PA][KBT][CDiP]`
- feat: add cached OpenFIGI enrichment for symbol-stability review triage `[CA][IV][REH][SFT][PA][CDiP]`
- feat: add local IEX entity snapshot diff and enrichment layer for ticker-era listing evidence `[CA][KBT][PA][CDiP]`
- feat: add no-key SEC ticker/CIK enrichment for symbol eras `[CA][REH][PA][KBT][CDiP]`
- feat: add dead ticker review queue for unresolved historical identity work `[CA][REH][PA][KBT][CDiP]`
- feat: add manual historical identity overrides for verified dead ticker eras `[CA][REH][KBT][CDiP]`
- feat: add prioritized unresolved dead ticker review worklist `[CA][PA][KBT][CDiP]`
- feat: add dead ticker resolution template and EDGAR lead lookup helper `[CA][IV][REH][SFT][CDiP]`
- feat: add verified dead ticker manual override importer `[CA][IV][REH][SFT][CDiP]`
- feat: add EDGAR full-text fallback search for dead ticker resolution leads `[CA][IV][REH][KBT][CDiP]`
- feat: add dead ticker research routing fields for instrument-specific evidence paths `[CA][CSD][KBT][CDiP]`
- feat: add impact-weighted dead ticker resolution workplan with dry-run low-materiality ledger candidates `[CA][REH][PA][KBT][CDiP]`
- feat: add stable confirmed-trade daily panel with entity metadata and quality flags `[CA][PA][KBT][CDiP]`
- feat: add stable daily panel validation report for structural contract checks `[REH][PA][CDiP]`
- feat: add stable raw returns table with clean/dirty return flags `[CA][PA][KBT][CDiP]`
- feat: flag potential corporate-action-like jumps in the stable returns table `[REH][PA][CDiP]`
- feat: add streaming IEXTools price-column repair utility for pre-fix Parquet outputs `[REH][RM][PA][CDiP]`
- docs: record complete HIST TOPS day coverage through 2026-06-22 and note unreadable parquet follow-up `[CDiP][KBT]`
- fix: raise unknown-message quarantine thresholds for TOPS forward-compatibility and enable faulthandler in the runner `[REH][PA]`
- fix: align backfill unknown-message defaults with the hardened runner and add explicit failed-gz cleanup/retention controls `[REH][RM][PA][CDiP]`
- fix: extract UDP payload streams from pcap-ng inputs before IEXTools parsing and fail zero-message parses explicitly `[CA][REH][PA][CDiP]`
- fix: support TOPS 1.5 backfill inputs by extracting classic pcap UDP payloads and passing the HIST protocol version into IEXTools `[CA][REH][PA][CDiP]`
- fix: serialize parallel backfill HIST refreshes and write HIST index downloads atomically `[REH][RM][PA][CDiP]`
- fix: preserve hq-4 computed slot price fields during normalization so float price columns are populated with lossless values `[REH][PA][CDiP]`
- fix: prefer full-size duplicate TOPS HIST records and patch hq-4 TOPS 1.5 TradeBreak decoding `[REH][KBT][PA][CDiP]`
- fix: skip unreadable Parquet days during symbol-stability reporting and list them in outputs `[REH][KBT][CDiP]`
- fix: add explicit TOPS Parquet replacement repair mode with scratch headroom checks and atomically repair nine unreadable published main Parquet days `[REH][RM][CA][CDiP]`
- fix: regenerate seven latent corrupt main Parquet days blocking confirmed-trade daily bars `[REH][PA][KBT]`
- fix: harden EDGAR full-text search against SEC EFTS form-filter and transport failures with a no-forms fallback and lower-noise retry logging `[REH][PA][KBT][CDiP]`
- fix: downgrade exhausted EDGAR request failures to warning-level per-symbol `search_error` rows while preserving error-level tracebacks for script bugs `[REH][KBT][CDiP]`
- fix: default EDGAR full-text requests to omit brittle SEC EFTS form filters while keeping form filtering available via `--use-form-filter` `[REH][PA][KBT][CDiP]`
- fix: reset EDGAR full-text search logs by default and add `--append-log` for intentional cumulative logs `[REH][KBT][CDiP]`
- fix: add EDGAR full-text request variants for SEC 500s on date-bounded `entityName` searches `[REH][PA][KBT][CDiP]`
- fix: suppress normal-console EDGAR retry noise and add `SYMBOL AND query` fallback for SEC query-parser edge cases `[REH][PA][KBT][CDiP]`
- fix: add EDGAR full-text issuer alias fallback for historical ticker no-hit cases such as `XLNX` and `ZNGA` `[REH][KBT][PA][CDiP]`
- fix: preserve runner exception details and classify parser short-buffer failures `[REH][CDiP]`
- docs: record backfill parser RCA and recommend transport-aware parser replacement path `[REH][AS][CDiP]`
- fix: repair four corrupt NAS TOPS days (`20201027`, `20220628`, `20240405`,
  `20240515`) from IEX HIST PCAPs and force-rebuild their stale pre-quarantine daily
  bars; bars coverage is now 2,393/2,393 days with zero failures `[REH][RM][KBT]`
