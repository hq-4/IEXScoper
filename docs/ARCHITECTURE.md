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

## Sector/Industry (SIC) Classification

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
  is a free cache hit. Rate-limited to ~3.3 req/sec by default (comfortably under SEC's 10 req/sec
  guidance).
- `canonical_identity_join.py` gained an additive `identity_source_url` column (the raw fact's
  `source` URL) so `sector_cik_reconcile.py` can recover CIKs from `legacy_historical_override`
  facts whose `entity_id` was migrated empty.
- `utils/build_era_sector_enriched.py` is the orchestration CLI: reconciles CIKs, fetches SIC for
  the distinct resolved CIKs, joins the division rollup, and writes
  `reports/era-identity/eras_sector_enriched.parquet` (a strict superset of
  `eras_identity_enriched.parquet`) plus `cik_sic_lookup.parquet` and a coverage report. No
  dry-run/apply gate — it only reads external SEC data and writes regenerable `reports/` output,
  nothing is applied to the tracked canonical store.
- `utils/build_sector_manual_research_worklist.py` ranks every era with no resolved CIK by trade
  volume for manual per-ticker research, flagging rows that at least carry a googleable
  OpenFIGI-asserted issuer name, and excluding funds/ETFs entirely (see below — they aren't
  research targets).

**Two follow-on automation passes shrank the manual-research pool by 45%** (measured, not
estimated), after the first live run showed the top of the worklist was dominated by huge ETFs
that were never OpenFIGI-classified, and by dead-ticker eras that already had a googleable issuer
name but no automatic path to a CIK:

- `utils/build_openfigi_stable_universe.py` derives an OpenFIGI input for
  `stable_candidate`/`ipo_or_new_listing_candidate` — the ~11,244-era slice that never went
  through OpenFIGI keyed enrichment (that pass was scoped to the dead-ticker review cohort only
  when it was built). Run for real: 92.5% matched, **43.3% are `fund_etf`** — exactly the ETFs
  that had been sitting unclassified at the top of the worklist.
- `utils/sec_name_cik_lookup.py` matches an era's OpenFIGI-asserted `identity_issuer` name
  against SEC's current company-name list (`sec_company_tickers_exchange.parquet`, already
  fetched — **zero new network calls**), normalizing and stripping legal-entity suffixes.
  Ambiguous names (two distinct CIKs normalizing identically) are dropped rather than guessed.
  This is `utils/sector_cik_reconcile.py`'s new **Tier D**: unlike Tier C (current-ticker-match),
  a name match applies to *any* class including dead-ticker ones, since a company keeps roughly
  the same name even after its old ticker gets reused by someone else. `match_by_name` also tries
  a second exact match after stripping trailing Bloomberg/OpenFIGI security-descriptor suffixes
  (`-CW23`, `-ADR`, `W/I`, `-CLASS A`, …, folded into OpenFIGI's `name` field but not part of the
  real legal name) — still a strict exact match against the same unambiguous index, not fuzzy. A
  broader token-subset/fuzzy matcher was evaluated and **rejected**: on real data it matched
  "1895 Bancorp of Wisconsin" to an unrelated company simply named "Bancorp" (a single generic
  token satisfying a naive subset check) — exactly the wrong-company risk this module exists to
  avoid, so it was not built.
- `utils/sector_enrichment_inputs.py` wires both into `build_era_sector_enriched.py`: the stable
  OpenFIGI classification feeds a new `instrument_class` column (COALESCE of `identity_instrument`
  and the stable-universe class) and a new `sic_coverage_status=fund_no_sic_needed` value, and Tier
  D adds to CIK reconciliation. Both inputs degrade gracefully (skipped, not an error) if their
  source file doesn't exist yet.

**Live run results, before -> after all automation passes** (zero errors, any run):

| | Before | After |
|---|---:|---:|
| Distinct CIKs resolved | 6,087 | 6,605 |
| Eras with real SIC + sector | 6,836 (18.5%) | 8,716 (23.6%) |
| Eras correctly excluded as funds/ETFs | 0 | 11,767 |
| **Manual-research worklist size** | 29,597 eras / 1.12B trade rows | **15,882 eras / 492M trade rows** |
| Worklist top-500 volume concentration | 64.7% | 72.5% |

`stable_candidate`'s `no_cik` count alone dropped from 542 to 13 — virtually every stable ticker
now either has a real SIC/sector or is correctly identified as a fund. [CA][IV][REH][CDiP][KBT]

**A third automation pass added Tier E**, extending the same "match a name, validate before
trusting it" idea Tier D uses to companies that are no longer in SEC's *current* listings at
all — genuinely deregistered, merged, or dissolved issuers, which Tier D structurally cannot
reach since it only searches the current-company file:

- `utils/sec_company_search_client.py` calls EDGAR's classic
  `cgi-bin/browse-edgar?action=getcompany` company-name browse search (not the current-listings
  JSON file), which — confirmed on live data before building the parser — **does** return
  historical/inactive registrants (e.g. a real, bankrupt Circuit City Stores CIK). It parses only
  the `<cik>` tags from the atom response; the endpoint's `title` field is a known SEC-side
  rendering bug (`title="ARRAY(0x...)"`) and is never read. Routed through
  `CachedPrimaryClient.get_json()`'s new optional `parse_response`/`is_negative` hooks (fully
  backward-compatible — every existing JSON caller is unaffected) so this non-JSON endpoint gets
  the same cache/retry/rate-limit machinery as everything else.
- `utils/edgar_company_search_match.py` accepts a match **only** when the search returns exactly
  one candidate CIK *and* that candidate's actual registrant name (fetched via the same
  `sec_sic_client.fetch_sic` call already used for SIC — often a free cache hit) matches the query
  name after normalization, with the same descriptor-stripping fallback Tier D uses. A bare
  "only one search hit" is never trusted on its own — live testing surfaced a real single-candidate
  false lead (`180 Life Sciences Corp` → a single, wrong-company search hit) that the name
  validation step correctly rejected.
- `utils/build_edgar_company_search_matches.py` batches this over every unique unresolved issuer
  name (deduped once up front — the same name repeating across an issuer's several eras is one
  request, not several), rate-limited at the same ~3.3 req/sec. A transient SEC 5xx on one name
  degrades to a `fetch_error` status and the batch continues rather than aborting and losing every
  result already collected (this exact failure happened once on the real run — see below).
- `sector_cik_reconcile.py`'s new **Tier E** fires only when Tiers A-D didn't already resolve a
  CIK, joined by issuer name (one row per unique name, like Tier D) rather than by era.

**Real run, 4,453 unique unresolved issuer names searched**: `969` matched (21.8%), `245`
ambiguous (>1 candidate, correctly left unresolved), `463` name-mismatch (single candidate but
the actual registrant name didn't match — rejected rather than guessed), `2,767` no-candidates,
`9` transient fetch errors. The run hit one real SEC `503` about 10 minutes in; before the
`fetch_error` handling above existed, that aborted the entire batch and discarded everything
collected so far — the fix (catch `PrimarySourceError` per name, mirroring the pattern
`sec_sic_client.fetch_sic` already established) let the rerun continue past it, replaying the
already-searched names as free cache hits.

| | Before Tier E | After Tier E |
|---|---:|---:|
| Distinct CIKs resolved | 6,605 | 7,529 |
| Eras with real SIC + sector | 8,716 (23.6%) | 10,002 (27.1%) |
| **Manual-research worklist size** | 15,882 eras / 492M trade rows | **14,559 eras / 389M trade rows** |
| Worklist top-500 volume concentration | 72.5% | 78.7% |

[CA][IV][REH][CDiP][KBT]

**A user spot-check of the worklist's own top rows surfaced a real, general blind spot**: `GPS`,
`BK`, and `PSTG` are not delisted at all — they're ticker renames (`GPS`→`GAP`, `BK`→`BNY`,
`PSTG`→`P`, all confirmed live against SEC's own submissions data), and `CORZ`'s `20260622` end
date is a stale vendor-window artifact, not a real corporate event (Core Scientific is still
actively trading). Root-caused to two distinct, general problems rather than four one-off ticker
fixes:

- **OpenFIGI's ticker-keyed lookup is current-listing-biased**, the same structural blind spot
  Tier C already has, just one layer upstream: querying a renamed-away ticker string (`GPS`, `BK`,
  `PSTG`) returns zero FIGI matches, so `identity_issuer` never gets populated and no downstream
  tier has a name to search with. (`CFLT`'s ticker separately collides with an unrelated foreign
  security in OpenFIGI's namespace — a different failure mode, same symptom.)
- **Nothing checked whether a resolved CIK's current SEC ticker matched the era's own symbol** —
  the pipeline had no way to distinguish "genuinely delisted" from "renamed" from "still trading,
  stale end-date" once *any* CIK was resolved.

Two fixes landed for these, both zero-new-network-cost (both reuse data already fetched/ingested
elsewhere) and general — they apply to the whole universe, not just the four tickers found by
inspection:

- `utils/sector_enrichment_inputs.py`'s `load_iex_fallback_names`/`apply_iex_fallback_issuer`
  backfill `identity_issuer` from `iex_latest_issuer` (`utils/build_iex_entity_enrichment.py`'s
  output — already-ingested local snapshot data, zero network cost) whenever the identity pillar
  left it null, without ever overwriting a real OpenFIGI/SEC-asserted name. A new
  `identity_issuer_from_iex_fallback` boolean keeps the backfilled rows distinguishable in the
  output.
- `utils/ticker_continuity.py` reads `tickers`/`exchanges` from the same SEC submissions payload
  `sec_sic_client.fetch_sic` already fetches for SIC — two more fields sitting unused in that same
  response, at zero additional network cost whenever the SIC pass already covered the CIK. Derives
  `continuity_status` per era: `terminal` (current tickers empty — genuinely delisted),
  `still_active_same_symbol` (era's own symbol is still in the current list — the end date is a
  stale artifact, not a real event), or `renamed_or_successor` (current tickers non-empty but
  doesn't include the era's own symbol).

Real run (17 new network requests — a mostly-free-cache-hit rerun since most CIKs were already
fetched by the prior pass): `sec_name_matched` CIKs rose 1,948 → 2,015 (+67, IEX-fallback names
Tier D could now search); manual-research worklist 14,559 → **14,491 eras**, 389M → **378M trade
rows**; `has_googleable_name` rows in the worklist rose 5,155 → **6,513** (+1,358 — IEX-fallback
names now visible for the eras that still didn't get an automatic CIK, e.g. `CFLT`/`CORZ`/`FTCH`/
`PSTG`/`PXD`, all of which need the separate Tier E search-recall fix below, not this one).
Across the whole universe, **`continuity_status` classified 740 eras `renamed_or_successor` and
7,915 `still_active_same_symbol`** — signal that was structurally invisible before, not just for
the four tickers that prompted the investigation. `GPS` itself stays a genuine residual: no name
anywhere in the pipeline (not OpenFIGI, not IEX), so no tier has anything to search with; would
need a heavier mechanism (EDGAR full-text search over old filings, deliberately deprioritized
earlier in this program for ~1% yield) or manual lookup.

**A fourth pass fixed the diagnosed Tier E recall gap** (`CFLT`/`CORZ`/`PXD`/and, on inspection, a
wider set including `ATVI`/`X`): EDGAR's `browse-edgar` company search does literal prefix
matching against the exact registered name string, and the raw OpenFIGI/IEX name frequently
doesn't literally prefix-match a real registrant's exact string. `utils/edgar_company_search_match.py`
now tries the full (descriptor-stripped) name first, then progressively drops trailing words down
to a 2-word floor, stopping at the first query that yields a validated match — and validates
*every* candidate a query returns (reusing `fetch_sic`, often a free cache hit) instead of
discarding any multi-candidate result as ambiguous outright. A separate, compounding bug in
`utils.sec_name_cik_lookup.normalize_name` meant SEC's trailing `/XX` jurisdiction tag (`"Core
Scientific, Inc./tx"`) blocked the legal-suffix-stripping loop from ever reaching `"INC"` — fixed
by stripping the tag first; this also fixed `CORZ` directly through **Tier D**, since the current-
listings match Tier D already does had the same normalization bug.

**A live smoke test before committing to the full run caught a real false-positive risk in the fix
itself**: searching for the real Confluent, Inc. (CIK 1699838, SIC 7372, the Kafka company) also
turned up an unrelated same-named shell, `"CONFLUENT INC"` (CIK 1171179, blank SIC) — a genuine SEC
name collision that plain normalized-name matching can't tell apart. Every confirmed-correct match
found while building this had a real SIC on record; the collision didn't, so a candidate with no
SIC is now never accepted as a match, even alone with no competing candidate — free (the SIC is
already fetched during validation) and directly targeted at the failure actually observed, not a
hypothetical one.

**A second bug, unrelated to search quality, was found and fixed mid-run**: rerunning
`utils/build_edgar_company_search_matches.py` against only the still-unresolved residual pool and
overwriting its output file silently *dropped* the previous run's 969 matches, since
`reconcile_cik` rebuilds `cik_source` fresh from that file on every run — `distinct_ciks_resolved`
measurably went *down* after the first rerun before this was caught. Fixed by re-including any
name Tier E itself resolved on a prior run in the search population, so every run recomputes a
complete, self-consistent Tier E table from scratch rather than an eroding partial one; cached
responses make the redundant-looking rerun over already-matched names cheap.

Real run (full recomputation, 4,627 unique names, mostly cache hits): EDGAR matches rose
969/4,453 (21.8%) → **1,680/4,627 matched (36.3%)**. Reconciled: distinct CIKs resolved 7,546 →
**8,085**; eras with real SIC + sector 10,070 (27.3%) → **11,150 (30.2%)**; `continuity_status`
`terminal` rose 1,953 → 2,956 (the newly-resolved CIKs are disproportionately genuinely-delisted
companies, e.g. `ATVI`/`X`/`FTCH`/`PXD`, correctly flagged rather than left unresolved). Of the
original top-10-by-volume worklist rows spot-checked, **7 of the 9 that had a real name anywhere
in the pipeline now resolve correctly** (`ATVI`, `X`, `BK`, `FTCH`, `CORZ`×2, `PXD`); only `PSTG`
(a bare `-A` share-class suffix the descriptor patterns don't cover) and `HOLX` (a genuine EDGAR
search miss) remain, plus `GPS` itself (no name anywhere in the pipeline, unreachable by any of
this). Manual-research worklist 14,491 → **13,448 eras**, 378M → **292M trade rows**; top-500
volume concentration 78.7% → **83.0%**.

**A fifth pass closed the `PSTG`-shaped gap directly visible at the top of the refreshed
worklist**: after the Tier E recall fix, the worklist's own top rows showed `EVERPURE INC-A`,
`C3.AI INC-A`, `ROYALTY PHARMA PLC- CL A`, and `MOBILEYE GLOBAL INC-A` — bare trailing `-A`/`-B`
share-class letters and a `"- CL A"` spacing variant `DESCRIPTOR_PATTERNS` didn't cover.
Quantified before building anything: 599 rows (29.9M trade rows) with a bare trailing letter, 23
rows (4.1M trade rows) with the spacing variant — comparable in size to the Phase 4 descriptor fix
that already recovered 299 rows earlier in this program. Added both patterns to
`utils.sec_name_cik_lookup.DESCRIPTOR_PATTERNS`, with the bare-letter pattern requiring exactly
one trailing character so it can't eat a genuine two-letter word ending like `-CO`. This improves
**both** Tier D and Tier E, since both share `strip_security_descriptors` — `PSTG` resolved
immediately through Tier D alone (its real current name, "Everpure, Inc.", is already in SEC's
current-listings file; the `-A` suffix was the only thing blocking the match), no live search
needed.

Real run: Tier D directly resolved 24 more CIKs / 106 more eras with zero network calls; a fresh
Tier E pass (4,584 names, mostly cache hits) then rose 1,680/4,627 (36.3%) → **1,844/4,584 matched
(40.2%)**. Reconciled: distinct CIKs resolved 8,085 → **8,201**; eras with real SIC + sector
11,150 (30.2%) → **11,466 (31.1%)**; manual-research worklist 13,448 → **13,131 eras**, 292M →
**264M trade rows**; top-500 volume concentration 83.0% → **83.4%**.

**Correction** (a prior version of this entry undercounted the remaining unresolved names — caught
when the user spot-checked the actual worklist CSV against the summary rather than trusting it):
of the original top-10, **3 remain unresolved, not 2** — `GPS`, `HOLX`, **and `CFLT`**. `CFLT`'s
case is worth being explicit about, since it isn't a gap at all: the SIC guard from the Tier E
recall fix correctly refuses it, because the only single-candidate match EDGAR's search can reach
for `"CONFLUENT INC-CLASS A"` within the 2-word truncation floor is the unrelated blank-SIC shell
(CIK 1171179), not the real Confluent, Inc. (CIK 1699838) — whose actual registered name, `"Confluent,
Inc."`, only becomes reachable by truncating to the single word `"CONFLUENT"`, below the floor this
design deliberately holds to avoid over-broad, high-collision-risk queries. `HOLX` is a genuine
EDGAR search miss (the real registrant's exact name/comma placement isn't reachable by any
variant tried) and `GPS` has no name anywhere in the pipeline — both, like `CFLT`, correctly
unresolved rather than silently missing. [CA][IV][REH][CDiP][KBT]

**Phase 9 (Tier D prefix-match fallback).** User: "what else is next." Cross-checked the refreshed
worklist's still-unresolved googleable-issuer names directly against SEC's current-listing file by
ticker and found 253 names whose ticker *is* currently SEC-listed under a name Tier D's exact match
was too strict to see — two generalizable, non-fuzzy patterns: (1) OpenFIGI truncates its `name`
field to a hard 28-character ceiling, sometimes mid-word (`"ALPHA METALLURGICAL RESOURCE"` for
`"...Resources, Inc."`); (2) un-expanded Bloomberg abbreviations survive `normalize_name` because
they aren't legal suffixes it knows to drop (`HLDGS` vs `HOLDINGS`, `INTL` vs `INTERNATIONAL`).
Quantified before building: 93 names/9.2M rows exactly-28-char-truncated, 127 names/11.4M rows
abbreviation-shaped, 220 total/~21M rows — comparable to prior descriptor-fix phases — against 50
names correctly left alone as genuine ticker reuse (`IAC`→People Inc, `USEG`→Big Sky Industrial),
confirming the pattern doesn't paper over real different-company cases.

Added a third fallback pass to `utils.sec_name_cik_lookup.match_by_name`: a word-boundary prefix
match on the same unambiguous index, requiring the shorter side to clear `MIN_PREFIX_TOKENS` (2)
tokens so a single generic word can't over-match (the exact risk that sank the Phase 4 fuzzy
matcher), and requiring exactly one distinct CIK across every candidate satisfying the relation —
ambiguous still means no match. Building the mid-word-truncation case (`_is_prefix_relation`
allowing a same-position final-token partial past `MIN_PARTIAL_TOKEN_CHARS`) surfaced a live
false-positive risk before shipping: SPAC sequel numbering (`"...Acquisition Corp II"` vs
`"...Corp III"`) is a genuine different company, not a truncation of each other, yet `"II"` is a
literal string prefix of `"III"` — caught by testing the real `TMTSW`/`TVACU` worklist rows during
implementation, not discovered live. Guarded by rejecting any partial match whose leftover
characters are composed entirely of Roman-numeral letters. 5 new tests (truncation, abbreviation,
the 2-token floor, the Roman-numeral guard, cross-CIK ambiguity); 480 tests pass (was 475); ruff
and bandit clean (bandit's one finding is pre-existing and unrelated, in `sec_identity_sources.py`).

Real run (`--skip-fetch`, zero network calls — Tier D only needs the already-cached SEC listing
file): distinct CIKs resolved 8,201 → **8,276**; manual-research worklist 13,131 → **12,691 eras**,
264M → **245.8M trade rows** (more than the 220-name estimate, since several recovered names repeat
across multiple eras). Spot-checked several newly-resolved matches against known real companies
(`HTZ`→Hertz Global Holdings CIK 1657853, `AMR`→Alpha Metallurgical Resources CIK 1704715,
`CCC`→CCC Intelligent Solutions CIK 1818201, `JHX`→James Hardie Industries CIK 1159152) — all
correct. `SG`/`ALIT` (a `" - CLASS A"` spacing variant with a space *before* the hyphen, distinct
from the descriptor patterns already handled) remain unresolved, out of this phase's scope. SIC/
sector coverage for the newly-resolved CIKs awaits a follow-up live SIC-fetch pass, same as every
prior Tier-D-only phase. [CA][IV][REH][CDiP][KBT]

**Phase 10 (Tier E `formerNames` validation).** User: "what else is next." The freshly-shrunk
worklist's new top rows (`COG`, `ETRN`, `CPE`, `FRC`, `HZNP`, `PE`, `ABC`, `CDAY`, `RAD`, ...) all
had EDGAR search finding the correct single-candidate CIK, but `edgar_company_search_match`
rejected every one, because `_names_match` only ever checked a candidate's *current* SEC
registrant name — and every one of these companies renamed or merged since the era's ticker was
active (`"CABOT OIL & GAS CORP"` finds CIK 858470 immediately; that CIK is now `"Coterra Energy
Inc."` post-2021-merger). SEC's submissions payload — already fetched for every validated
candidate via `sec_sic_client.fetch_sic` — carries a `formerNames` array (exact historical name +
date range) sitting completely unused next to `sic`/`name`.

Quantified entirely from the existing SQLite request cache before writing any code (zero network
calls): 515 unique names / 724 eras / 46.0M trade rows recoverable, with 9 names correctly staying
ambiguous (genuine historical-name collisions between two real registrants) — confirming
`formerNames` doesn't just loosen the match, it still separates real matches from real ambiguity.
Spot-checked the case most likely to repeat this module's own documented false-lead warning
("180 Life Sciences Corp" — see Phase 5): checked that CIK's `formerNames` directly and confirmed
it genuinely *was* 180 Life Sciences 2019-2025 before two later unrelated renames, so today's fix
doesn't reintroduce that risk. Also hand-verified `RITE AID CORP` -> CIK 84129, whose `formerNames`
shows it held that exact name 1994-2024 before its post-bankruptcy reorg to `NEW RITE AID, LLC`
(SIC 5912, Retail-Drug Stores) — a real, correct match that a 41-candidate ambiguous 2-word
fallback query was previously masking.

Added `former_names` to `sec_sic_client.fetch_sic`'s result dict (harmless to every existing
caller — polars' schema-constrained `DataFrame` construction in `build_era_sector_enriched.py`
drops unknown dict keys). `edgar_company_search_match._validate_candidates` now checks a
candidate's `formerNames` whenever its current name doesn't match, gated by the same
SIC-must-exist guard and the same 2-validated-matches-is-ambiguous rule as before — no new trust
assumption, just more of the already-fetched authoritative payload. 5 new tests; 485 pass (was
480); ruff/bandit clean.

Real run (full recomputation, 4,342 names): only **21 new network requests** (8,330 cache hits) —
Tier E matched 1,844/4,584 (40.2%) -> **2,287/4,342 (52.7%)**. Reconciled: distinct CIKs resolved
8,276 (Phase 9) -> **8,351**; manual-research worklist 12,691 -> **12,007 eras**, 245.8M ->
**210.3M trade rows**. Spot-checked several newly-resolved matches against known real corporate
history (`COG`->Coterra Energy SIC 1311, `ETRN`->SIC 4922 Natural Gas Transmission, `ABC`->SIC 5122
Wholesale-Drugs, `RAD`->SIC 5912 Retail-Drug Stores) — all correct. `HZNP`/`CPE`/`FRC` remain
unresolved: candidates exist but none — current or former name — validate, a different, smaller
gap than this phase targeted. Combined with Phase 9 in the same session: worklist
13,131 -> **12,007 eras** (-8.6%), 264.1M -> **210.3M trade rows** (-20.3%). [CA][IV][REH][CDiP][KBT]

**Phase 11 (Tier E 1-word query floor).** User: "what else is next," then approved running the
full live quantification and building it after being shown the trade-off against this project's
own prior `CFLT` decision (Phase 5/8: the query floor was deliberately held at 2 words "to limit
over-broad-query collision risk"). Root cause: a name that's already exactly 2 words after
descriptor-stripping (`"HOLOGIC INC"`, `"ZENDESK INC"`, `"ANAPLAN INC"`) could never truncate
further under that floor — if the full 2-word query didn't literally prefix-match the registrant's
real punctuation (EDGAR's `browse-edgar` search is a literal prefix match), the name got zero
candidates at *every* query tried, with no shorter fallback possible. This was now the single
largest remaining worklist bucket: 527 names / 1,800 eras / 58.7M trade rows.

Before touching the floor, live-verified the specific risk it was guarding against no longer
applies: querying `HOLOGIC`/`ZENDESK`/`ANAPLAN` (1 word) each found a real single candidate.
`ZENDESK` and `ANAPLAN` validated correctly against their real SIC/name. `HOLOGIC` surfaced an
unrelated blank-SIC limited partnership — and the existing SIC-must-exist guard *correctly
rejected* it rather than accepting it, proving the guard (not the word count) was always what made
a broad query trustworthy, exactly as the module's own docstring already argued for the 2-word
case. Re-checked the actual `CONFLUENT` precedent live too: it now returns 9 candidates, over
`MAX_CANDIDATES_TO_VALIDATE`, so `CFLT` still correctly stays unresolved via the existing
candidate-count guard — the 1-word floor doesn't reopen that specific case, it just changes which
guard catches it.

`MIN_QUERY_WORDS` dropped from 2 to 1 in `edgar_company_search_match.py`; no other logic changed
— the safety property was already carried by `_validate_candidates`, not the truncation floor. 3
new tests (2-word-to-1-word truncation landing a real match, the blank-SIC guard still holding at
the new floor, the over-candidate-cap guard still holding); 1 existing test's call-count
assertion updated to reflect the now-correct extra query attempt for a 2-word name. 488 tests pass
(was 485); ruff/bandit clean.

Real run (full recomputation, 4,342 names): genuinely rate-limited this time, since most of the
new 1-word queries were never cached — 54m20s wall-clock, matching SEC's guidance throughout.
`no_candidates` 527 -> **54**; `no_validated_match` 1,265 -> **501**; `ambiguous_candidates` 263 ->
**1,348** (the anticipated trade-off, documented up front: a 1-word query is more permissive, so
many previously-zero-candidate names landed in genuine multi-candidate ambiguity — correctly not
guessed between — rather than a clean match); `matched` 2,287 -> **2,438**. Reconciled: distinct
CIKs resolved 8,351 -> **8,455**; manual-research worklist 12,007 -> **11,817 eras**, 210.3M ->
**187.7M trade rows**. Spot-checked newly-resolved matches (`ZEN`->Zendesk CIK 1463172 SIC 7374,
`PLAN`->Anaplan CIK 1540755 SIC 7372) — correct; confirmed `HOLX` (Hologic) stayed unresolved
exactly as predicted by the blank-SIC guard rather than silently regressing. Combined across all
three phases landed in this session: worklist 13,131 -> **11,817 eras** (-10.0%), 264.1M ->
**187.7M trade rows** (-28.9%). [CA][IV][REH][CDiP][KBT]

**Phase 12 (share-class spacing gaps).** User: "what else is next." Phases 9-11 above had been
built and verified in a prior session but left uncommitted directly on `main`; committed first
(branch, PR #13) per the user's explicit choice, then moved on to new work.

Traced why several real, still-findable companies (`SEE`/Sealed Air, `CPE`/Callon Petroleum,
`HZNP`/Horizon Therapeutics, `IAC`, `CLR`/Continental Resources) were landing in
`ambiguous_candidates`/`no_validated_match` by replaying the actual `match_issuer_name` function
against cached responses rather than eyeballing candidate lists by hand (a manual trace first
mis-diagnosed `HZNP` as a `PLC` suffix gap; the real function showed its 2-word query never
name-matches a candidate at all and falls through to the 1-word query's candidate-count cap
instead). That trace also surfaced a separate, real bug — `normalize_name`'s legal-suffix strip
loop treats "GROUP" as a droppable suffix, so `"Continental Resources Group, Inc."` (an unrelated
junior mining shell, SIC 1000) collapses to the same normalized name as the real, Harold-Hamm
`"CONTINENTAL RESOURCES, INC"` (ticker `CLR`, SIC 1311) and both validate, causing a false
ambiguity — deliberately left unfixed pending a ticker-based disambiguator and two-directional
verification against the whole existing suffix-stripped match population, since loosening or
tightening `LEGAL_SUFFIXES` risks both directions (new matches and new false collisions) at once.

What shipped: two narrow regex spacing gaps, found the same way as Phase 9's `-CL A` fix.
`DESCRIPTOR_PATTERNS`'s `-CLASS [A-Z]$` required the hyphen directly against "CLASS" with no
space, so `"SWEETGREEN INC - CLASS A"` and `"FIRST DATA CORP- CLASS A"` never got stripped even
though the equivalent abbreviated `-CL A` pattern already tolerated that spacing — now mirrors it,
and both patterns also consume a space *before* the hyphen (`"UCP INC - CL A"`), closing a
trailing-space artifact the original Phase 9 fix left behind. Separately, `JURISDICTION_SUFFIX`
only matched a tight `/XX`, but SEC's own submissions payload returns a spaced `"Alight Inc. /
DE"` variant the tight `Core Scientific, Inc./tx` precedent from Phase 7 never covered.

Quantified entirely by replaying the full still-unresolved Tier E population (1,904 names) against
cached search/validation responses with both fixes applied — zero new network calls: 15 names flip
to a validated match. 6 new tests; 494 pass (was 488); ruff/bandit clean.

Real run (`build_edgar_company_search_matches.py` + `build_era_sector_enriched.py` +
`build_sector_manual_research_worklist.py`, ~40s wall-clock, essentially all cache hits): Tier E
matched 2,438/4,342 -> **2,453/4,342**; distinct CIKs resolved 8,455 -> **8,470**; manual-research
worklist 11,817 -> **11,791 eras**, 187.7M -> **180.8M trade rows**. Spot-checked all 6 largest
newly-resolved matches against known real companies (`SG`->Sweetgreen SIC 5812, `ALIT`->Alight Inc
SIC 7374, `AYX`->Alteryx SIC 7372, `FDC`->First Data Corp SIC 7389, `MCFE`->McAfee Corp SIC 7372,
`VEI`->Vine Energy SIC 1311) — all correct; `SG` resolved via Tier D (`sec_name_matched`) rather
than Tier E, confirming the shared `strip_security_descriptors` fix benefits both tiers. Open for a
follow-up phase: a ticker-based disambiguator for the `ambiguous_candidates` bucket (1,340 names,
now the largest remaining) using SEC's already-fetched `tickers` field, and the `GROUP`-suffix
over-normalization found but not touched here. [CA][IV][REH][CDiP][KBT]

**Phase 14 (Tier E filing-activity tie-break).** A ticker-based attempt at the `ambiguous_candidates`
disambiguator Phase 12 flagged as the next lead was tried and reverted (see `docs/TASK_LIST.md`'s
Phase 13 entry): SEC's `tickers` field reflects only *current* listing state, and this population
is by construction delisted companies, so it yielded 0/74 real matches. The real signal was sitting
in the same already-fetched submissions payload the whole time: `filings.recent` (and, for older
history, `filings.files` shards) — a candidate's actual SEC filing dates. For the real Continental
Resources, Inc. (CIK 732834, ticker `CLR`, SIC 1311), filings span 2009-12-28 through 2026-07-31
continuously (plus a shard back to 1998) — covering this repo's whole IEX TOPS data era
(2016-12-12 onward). Its name-collision partner, an unrelated junior-mining shell "Continental
Resources Group, Inc." (CIK 1430975, SIC 1000 — a real, non-blank SIC, so the existing blank-SIC
guard alone can't catch it): last filing a `15-12G` voluntary deregistration on 2013-03-05,
permanently dark since — structurally impossible to be the trading entity for any era after ~2013.

`sec_sic_client.fetch_filing_activity` reads `filings.recent`'s dates plus whether `filings.files`
holds unfetched older history, via the identical cache key `fetch_sic` already populated (zero new
network cost in practice). `edgar_company_search_match._filing_activity_verdict` has three
outcomes, never a guess: `plausible` (a filing lands inside the era window), `disjoint` (provably
could not have been active — newest filing predates the era, or every known filing postdates it
with no older shard left uncertain), or `unknown` (can't tell — including a candidate whose
history merely *brackets* the era without a filing actually landing inside it, deliberately not a
rejection, since a real filer can legitimately be quiet across a short window). A tie only resolves
when every candidate gets a definite verdict and exactly one is `plausible`; a single `unknown`
anywhere blocks acceptance. `_validate_candidates` no longer stops at 2 validated candidates —
needs to see every tied candidate, not just the first 2, to disambiguate correctly.
`build_edgar_company_search_matches.unresolved_issuer_era_spans` supplies each name's
`(min(first_day), max(last_day))` union across every unresolved era sharing that name (name-deduped
granularity, same as the module already runs at — a wider union only ever makes the guard more
permissive, never a false accept).

Quantified via a throwaway script (zero network calls, reading the cache directly) before writing
the real integration: of 1,340 `ambiguous_candidates` names, 74 reach a genuine 2-way validated
tie; ~38 resolve under the strict accept rule, confirmed correct for `CLR`. Scope confirmed with
the user: tie-break only. The same rule applied to the 2,453 already-`matched` names would
separately flag 81 as disjoint (e.g. `AETNA INC -> 1013761`, likely a genuine pre-existing false
positive — the operating Aetna for 2016-2018 eras is `1122304`) and 42 more inconclusive — a
larger, riskier audit explicitly deferred to a future phase. 16 new tests; 510 pass (was 494);
ruff/bandit clean.

Real run: **43 names** resolved via `filing_activity_tiebreak` (above the cache-only floor of ~38,
as expected — some ties had an uncached 3rd+ candidate the live run could finally see). Tier E
matched 2,453/4,342 -> **2,495/4,339**; distinct CIKs resolved 8,470 -> **8,500**; manual-research
worklist 11,791 -> **11,707 eras**, 180.8M -> **169.7M trade rows**. Spot-checked all 43
newly-resolved matches against known real companies (Raytheon Company, CIT Group Inc, Mead Johnson
Nutrition, Nuance Communications, Sealed Air Corp, Callon Petroleum, IAC Inc, Rexnord Corp, Vivint
Smart Home, GCI Liberty, MB Financial Inc, Renewable Energy Group, Welbilt Inc, among others) — all
correct. Open for follow-up: the deferred 81-name existing-match audit; the 1,266-name
`>MAX_CANDIDATES_TO_VALIDATE` bucket (filing activity is a plausible enabler for raising that cap,
a separate larger phase); `filings.files` shard walking (measured to unblock exactly 1 name today,
not built). [CA][IV][REH][CDiP][KBT]

**Phase 15 (raise the candidate-validation cap).** Took up Phase 14's flagged follow-up: with the
filing-activity guard proven safe at scale, `MAX_CANDIDATES_TO_VALIDATE` raised 8 -> 20 in
`edgar_company_search_match.py`. The risk the original cap guarded against was never "validating
more candidates is dangerous" (validation is exact-match; a wrong candidate simply fails to
validate) — it was "validating an implausibly generic query's candidates wastes requests." Unlike
every other phase, this one had no free quantification path: names with 9-20 candidates had never
been validated at all under the old cap (the count guard returns before fetching any of them), so
nothing sat in the cache to replay (verified: 972 cache misses across that population on the first
attempt). Shipped as a genuine ~19-minute live SEC run instead, at the same per-candidate request
budget as always.

Real run: matched 2,495/4,339 -> **2,515/4,339** (+20: 18 ordinary single-candidate validations, 2
more via the filing-activity tie-break); `ambiguous_candidates` 1,296 -> **1,066**;
`no_validated_match` 494 -> **704** (+210, a valuable reclassification, not a regression — names
previously sitting in "ambiguous, never actually checked" turned out, once validated, to contain
zero real matching candidates, correctly reclassified rather than left in a falsely-alarming
bucket). Distinct CIKs resolved 8,500 -> **8,517**; manual-research worklist 11,707 -> **11,683
eras**, 169.7M -> **162.8M trade rows**. Notably resolved the two running examples this module's
own docstring has cited since Phase 5/7/11 as "correctly staying unresolved" — `CFLT` (the real
Confluent, Inc., CIK 1699838) and `MYLAN NV` (CIK 69499) — both purely because their candidate
counts (9 and 12) had simply never fit under the cap before, not any new logic. Spot-checked
additional new matches (`IAA INC` -> IAA, Inc., real, spun off from KAR Auction Services 2019;
`NCI INC-A` -> NCI, Inc., a real government IT services company) — correct. 2 tests updated to
derive their over-cap candidate count from the real constant rather than a hardcoded `9`, so they
stay correct regardless of its value; 510 tests still pass, ruff/bandit clean. Open for follow-up:
the still-deferred 81-name existing-match audit; whether to raise the cap further — 504 of the
remaining 1,066 ambiguous names sit at the 100-candidate EDGAR API page cap, genuinely too generic
to be worth validating at any reasonable cap. [CA][IV][REH][CDiP][KBT]

**Phase 16 (81-name existing-match filing-activity audit).** Took up Phase 15's other flagged
follow-up: the filing-activity guard (Phase 14) only ever ran when 2+ candidates validated by
name — a *single* validated candidate was accepted on name+SIC alone, its filing history never
checked against `era_span` at all, even when flatly disjoint. Quantified cache-only first
(`EvidenceRegistry.get(...)` read directly, zero network calls): of the 2,515 then-`matched`
names, 82 provably disjoint (e.g. `AETNA INC` -> CIK 1013761, filings 1996-2015 for an era
starting 2016-12-12 — the real operating Aetna for those eras is a different CIK, 1122304), 42
more `ACTIVITY_UNKNOWN` (correctly left alone, same "quiet filer" reasoning as the tie-break).

Added `_provably_disjoint`, gating the single-candidate accept the same way
`_disambiguate_by_filing_activity` already gates a multi-candidate one: `continue` to a shorter
query instead of returning a confidently-wrong match, so a name whose only reachable candidate is
disjoint gets a chance to resolve against a different candidate at a broader query. Fails open
exactly like the tie-break guard: no `era_span` or a failed filing-activity fetch never rejects.
This intentionally reverses a Phase 14 regression test that had locked the opposite behavior in --
that test existed to keep the single-candidate path unchanged *until this exact audit ran*, not
as a permanent guarantee. 5 new tests; 514 tests pass (was 510), ruff/bandit clean.

Real run (two passes — the second cleared 8 transient SEC fetch errors from the first, nothing
cached on `fetch_error` so they retried cleanly): all 82 cleared out of `single_validated_candidate`
-- 8 resolved to a *different, correct* CIK via the tie-break (`EXTENDED STAY AMERICA INC`
1002579 -> 1581164, the post-2013-bankruptcy-reorg entity; `OSIRIS THERAPEUTICS INC` 912815 ->
1360886; six more), 45 honestly `no_validated_match`, 29 honestly `ambiguous_candidates`, zero
left wrongly matched. Tier E matched 2,515/4,339 -> **2,441/4,339** (-74, expected — correctness
over coverage, not a regression). Reconciled: distinct CIKs resolved 8,517 -> **8,448**;
manual-research worklist 11,683 -> **11,783 eras**, 162.8M -> **166.4M trade rows** (all three
moved in the expected direction). Open for follow-up: `GROUP`-suffix over-normalization (Phase
12/13, still untouched); whether the 74 names this phase net-unmatched are recoverable some other
way (not investigated — out of this phase's scope). [CA][IV][REH][CDiP][KBT]

**Phase 17 attempt (`GROUP`-suffix over-normalization) — negative result, not shipped** (see
`docs/TASK_LIST.md`'s Phase 17 entry for full detail). Quantified removing `"GROUP"` from
`sec_name_cik_lookup.LEGAL_SUFFIXES` against both tiers that share it, entirely from local
data/cache: Tier D would lose 54 currently-correct matches for only 9 gained (the losses are real
same-company matches depending on `GROUP`-stripping to bridge SEC's own short-name vs. market-name
convention gap, or to prefix-match financial products issued under a parent holding company's
CIK); Tier E gained 1 of 1,095 currently-ambiguous names. The `CLR`/Continental Resources
collision this thread originally targeted was already solved by Phase 14's filing-activity
tie-break, an independent-evidence mechanism that doesn't touch `normalize_name` at all — so the
one real case motivating this fix no longer needs it. `GROUP` stays in `LEGAL_SUFFIXES`,
unchanged; no code shipped. [CA][IV][REH][CDiP][KBT]

**Phase 18 (`identity_disproven` worklist flag).** A fresh pass over the worklist's top rows
found `UTX` (919K trade rows, the real United Technologies Corp) carrying an OpenFIGI-asserted
`identity_issuer` of `"ULTRATREX INC-A"` — a real but unrelated shell. Root cause traced one
layer upstream of anything Tier A-E touches: `openfigi_identity_core.py` queries OpenFIGI's
`/v3/mapping` by bare ticker with no date awareness and takes the first FIGI unconditionally;
OpenFIGI's own index now returns only Ultratrex for `"UTX"` since United Technologies' ticker was
vacated by its 2020 merger into Raytheon — the same current-listing bias this project has hit at
every other layer, one step further upstream. No available data source (OpenFIGI, IEX's own
snapshot fallback) can recover the correct name here; this stays a genuine manual-research case.
What *is* fixable: Phase 16's filing-activity guard already proves `"ULTRATREX INC-A"` can't be
the operating entity for `UTX`'s era, but that proof was a silent internal rejection, never
surfaced. Added `identity_disproven` (`edgar_company_search_match.match_issuer_name`, purely
additive metadata, no matching-logic change), threaded through
`build_edgar_company_search_matches.py`, a new `sector_enrichment_inputs.apply_identity_disproven`
side-channel that never touches `resolved_cik`/`cik_source`, and surfaced in the manual-research
worklist as a column, a summary count, and a struck-through marker in the top-rows table. 8 new
tests; 522 pass (was 514), ruff/bandit clean. Real run confirmed byte-identical match outcomes
(distinct CIKs resolved, worklist eras/trade-rows all unchanged) except the new field: 82 names /
100 worklist eras flagged. See `docs/TASK_LIST.md`'s Phase 18 entry for full detail.
[CA][IV][REH][CDiP][KBT]

**Phase 19 (`blank_sic_lead` research-lead flag).** A fresh worklist pass found `FIRST
REPUBLIC BANK/CA` (1.86M trade rows) rejected purely for a blank SIC on its exact-name-match
candidate (CIK 1132979, 42 real filings 2004-2024 covering its era). Built as auto-acceptance
first — the same shape as Phase 16's disjoint-rejection but in reverse — then, spot-checking
the 34-name quantified population before shipping (this project's standing practice), found
roughly half had zero substantive filings ever (`entityType="other"`, only ownership-disclosure
forms any unrelated party can file), and tightening the rule would have excluded the flagship
`FIRST REPUBLIC BANK` case too (plausibly a Section 12(i) bank filing its real 10-Ks with its
regulator, not SEC). No reliable signal in this repo's data distinguishes the two cases, so
auto-acceptance was abandoned; redesigned as informational-only, mirroring Phase 18's already-
safe pattern — `blank_sic_lead_cik`/`_name`/`_high_confidence` surfaced as metadata, never
changing `match_status`/`matched_cik`. New `sec_sic_client.SUBSTANTIVE_FORMS` distinguishes
real operating disclosure from ownership-disclosure forms for the confidence flag. 12 new
tests; 535 pass (was 531), ruff/bandit clean. The real run caught a second bug before shipping:
a lead found at a narrower query level was dropped when a later, broader query hit the
candidate-count cap and returned early without carrying it forward — losing the `FIRST
REPUBLIC BANK` lead itself. Fixed, regression test added, rerun: 34 leads (12 high-confidence),
match outcomes and CIK-resolution counts fully unchanged. See `docs/TASK_LIST.md`'s Phase 19
entry for full detail. [CA][IV][REH][CDiP][KBT]

**Phase 20 (filing-window-containment tie-break).** A fresh worklist pass found two small
2-candidate ties the original disjoint-based tie-break can't resolve: `LAREDO PETROLEUM INC`
(a genuine mid-era holdco-reorg succession — an original CIK whose filings stop in 2019, and
its successor, `formerNames`-linked, later renamed `Vital Energy, Inc.`, filing continuously
2016-2025) and `LIFE STORAGE INC` (a REIT parent and its operating partnership, both filing
continuously through and past the era — two legitimately co-existing entities, not a
succession). Both candidates read `ACTIVITY_PLAUSIBLE` in each case, so "exactly one
plausible, others disjoint" can't separate them. New `_fully_contains_era` accepts the one
plausible candidate (among several) whose own filing window fully spans the era, when exactly
one does — strictly stricter than the "any filing lands inside" bar already trusted
elsewhere, so it only resolves a narrower case, never loosens acceptance. Labeled with a
distinct `match_basis` (`filing_window_containment_tiebreak`). Correctly leaves `LIFE
STORAGE`'s shape (10 of 34 quantified names) unresolved — both candidates' windows fully
contain the era there too, needing a different signal (SIC specificity) not built here. 2 new
tests; 537 pass (was 535), ruff/bandit clean. Real run: Tier E matched 2,441/4,339 ->
2,459/4,339 (+18, exactly matching cache-only quantification); all 18 spot-checked against
known real companies via resolved SIC — correct. Distinct CIKs resolved 8,448 -> 8,463;
manual-research worklist 11,783 -> 11,748 eras, 166.4M -> 161.1M trade rows. Flagged (not
acted on): `edgar_company_search_match.py` is now 669 lines, over the 300-line CSD review
threshold — almost entirely this file's own deliberately-kept phase-by-phase narrative
docstring, not code complexity. See `docs/TASK_LIST.md`'s Phase 20 entry for full detail.
[CA][IV][REH][CDiP][KBT]

**Phase 21 attempt (SIC-specificity tie-break) — negative result, not built.** Investigated
Phase 20's own speculated next step for the 10-name `LIFE STORAGE`-shaped population (multiple
filing-plausible, window-containing candidates a containment check can't separate). Found a
direct real counter-example before writing any code: `DUPONT FABROS TECHNOLOGY, INC.` — the
actual publicly-traded REIT (ticker `DFT`) — carries the *generic* SIC `6500`, while its
private operating partnership carries the *more specific* REIT code `6798`, the exact
opposite of the `LIFE STORAGE` case that motivated the idea. No consistent SIC pattern
distinguishes a REIT parent from its operating-partnership sibling across real EDGAR data.
The remaining 8 names are each their own shape (two real LP entities with no Inc./LP split;
candidates matching via current name on both sides; genuinely complex multi-entity corporate
history) — no single safe, generalizable signal covers this population. No code shipped; see
`docs/TASK_LIST.md`'s Phase 21 entry for full detail. [CA][IV][REH][CDiP][KBT]

**Phase 22 (Tier E OpenFIGI-truncation-tolerant name matching — largest single-phase yield
this session).** Found `INTERCEPT PHARMACEUTICALS IN`: EDGAR's search finds the one real
candidate, but OpenFIGI's 28-character field ceiling cut `"...INC"` to `"...IN"`, and exact
normalized-equality validation rejected it anyway — the same gap Phase 9 already closed for
Tier D (`sec_name_cik_lookup._is_prefix_relation`), never carried to Tier E. Built the obvious
fix (reuse `_is_prefix_relation` directly, 562 matches quantified cache-only), then caught two
real, confirmed false positives via random-sampling before shipping: `"TPG Pace Holdings
Corp."` normalizes down to just `"TPG PACE"` (both `"Holdings"` and `"Corp."` are legal
suffixes) and spuriously prefixed the unrelated sibling SPAC `"TPG Pace Beneficial Finance
Corp."`; `"Prime Number Holding Ltd"` similarly collapsed to `"PRIME NUMBER"` and spuriously
prefixed `"Prime Number Acquisition..."` — both real instances of the 2020-2022 SPAC boom's
pattern of one sponsor launching many similarly-named vehicles from a shared short prefix.
That branch is only safe in Tier D because it additionally requires uniqueness across the
*entire* SEC index; Tier E validates one already-searched candidate at a time with no
equivalent check. New `_is_safe_final_token_truncation` keeps only the narrower "same token
count, exact match on every token but the last" sub-case, deliberately not recovering the
fix's own original motivating example (no structural way to tell truncation noise apart from
a real distinguishing word). Re-quantified: 345 names under the safe rule (871 under the
rejected broader rule); 80 fresh random samples found zero remaining false positives. 5 new
tests; 542 pass (was 537), ruff/bandit clean. Real run: Tier E matched 2,459/4,339 ->
2,804/4,339 (+345, exactly matching quantification); a fresh spot-check of 25 from the full
matched population all correct. Distinct CIKs resolved 8,463 -> 8,572; manual-research
worklist 11,748 -> 11,300 eras, 161.1M -> 154.6M trade rows. Flagged again: the module file is
now 767 lines, continuing to grow past the CSD review threshold. See `docs/TASK_LIST.md`'s
Phase 22 entry for full detail. [CA][IV][REH][CDiP][KBT]

**Phase 23 (housekeeping — module docstring extraction).** Addressed the file-size flag
raised in Phase 20 and Phase 22. `edgar_company_search_match.py`'s ~265-line module
docstring (the full Phase 5-22 narrative, already duplicated more compactly in this file
and in `docs/TASK_LIST.md`) moved verbatim into a new
`docs/EDGAR_COMPANY_SEARCH_MATCH_DESIGN.md`, replaced with a concise ~55-line technical
summary of the current mechanisms. File 767 -> 559 lines. Docstring-only change — 542
tests still pass, ruff clean, no behavior change. Still over the 300-line CSD threshold
(the remainder is real code plus necessary per-function "why" documentation, not
narrative bulk); full modularization would be a separately-justified refactor, not
undertaken here. [CA][CDiP]

**Phase 24 (ADR descriptor-pattern gaps).** Re-checked the Phase 20/21 small-tie shape
against the post-Phase-22 state first (no new lead, just two more members of the
already-ruled-unsafe `LIFE STORAGE` bucket). Found a real gap instead tracing the
worklist's top ADR-shaped rows: `SONY CORP-SPONSORED ADR`, `SIBANYE GOLD LTD-SPONS ADR`,
and `BRASKEM SA-CLASS A- ADR` don't match any existing `DESCRIPTOR_PATTERNS` entry — the
existing `-SPON ADR$`/`-ADR$` patterns require an exact abbreviation and no space before
`ADR`. Same abbreviation/spacing-variant category as Phase 8/9/12. Added three pattern
entries to `sec_name_cik_lookup.DESCRIPTOR_PATTERNS` (shared by both tiers): a
`"SPONS ADR"` sibling, spelled-out `"SPONSORED ADR"`, and a space-tolerant `[-\s]+ADR$`
(ordered before the `CLASS` patterns so a compound suffix strips correctly). Quantified
cache-only first: 11 resolve immediately, 13 more blocked by cache misses (a live run
genuinely needed to learn the true yield). 3 new tests; 545 pass (was 542), ruff/bandit
clean. Real run: Tier E matched 2,804/4,339 -> 2,823/4,339 (+19). `SONY`/`SIBANYE`
correctly resolved; `BRASKEM` correctly stayed unresolved (no real SIC on any
candidate). All 76 ADR-shaped matched names spot-checked — no false positives. Distinct
CIKs resolved 8,572 -> 8,589; manual-research worklist 11,300 -> 11,278 eras, 154.6M ->
152.5M trade rows. [CA][IV][REH][CDiP][KBT]

**Phase 25 (widen `JURISDICTION_SUFFIX` beyond 2-letter codes).** `BITFARMS LTD/CANADA`
traced the gap: `JURISDICTION_SUFFIX` only matched a trailing `/XX` of exactly 2 letters.
Scanning the full unresolved population found 22 names carrying a trailing `/WORD`, every
one either SEC's `"/THE"` sorting artifact, a `"/NEW"` successor tag, or a full state/
country name — never part of a registrant's real name. Widened the pattern from
`/\s*[A-Z]{2}$` to `/\s*[A-Z]+$`. Since this touches `normalize_name` itself (shared by
Tier D's bulk index, not just Tier E), replayed the entire SEC current-listings index
under both patterns: zero new ambiguous-name collisions. Quantified Tier E yield
cache-only: 28 names. 4 new tests; 549 pass (was 545), ruff/bandit clean. Real run: Tier E
matched 2,823/4,339 -> 2,848/4,334 (+25); 11 newly-matched names spot-checked, all
correct. Distinct CIKs resolved 8,589 -> 8,607; manual-research worklist 11,278 ->
11,213 eras, 152.5M -> 150.7M trade rows. [CA][IV][REH][CDiP][KBT]

**Phase 26 (`normalize_name` "AND"/"&" joiner-word asymmetry).** `PETCO HEALTH AND
WELLNESS CO` traced the gap: `normalize_name` already drops a literal `"&"` to nothing
(via `NON_ALNUM`), but left the spelled-out `"AND"` as a real token — so it never
token-matched SEC's own `"Petco Health & Wellness Company, Inc."`. Added `JOINER_WORDS =
frozenset({"AND"})`, dropped anywhere in the token stream (mid-name, not just trailing).
Shared by Tier D and Tier E; replayed the entire SEC current-listings index: zero new
ambiguous-name collisions. Cache-only Tier E quantification: 4 of 16 `"AND"`-containing
names newly resolve; 33 `"&"`-containing names: zero new matches. 4 new tests; 553 pass
(was 549), ruff/bandit clean. Real run (shared infra, so Tier D gained matches too):
resolved-CIK era rows 14,107 -> 14,130 (+23); distinct CIKs resolved 8,607 -> 8,612;
manual-research worklist 11,213 -> 11,196 eras, 150.7M -> 149.2M trade rows.
[CA][IV][REH][CDiP][KBT]

**Phase 27 (Tier E query-level abbreviation expansion).** `MICHAELS COS INC/THE` traced
the gap: EDGAR's search is a literal string-prefix match against the real registered
name, and `"COS"` isn't a character-prefix of `"COMPANIES"` (unlike `"CORP"`/`"INC"`/
`"CO"`, which already are prefixes of their expansions) — so the search itself found
nothing at any truncation level. Same shape for `"HLDGS"`/`"HOLDINGS"` and
`"INTL"`/`"INTERNATIONAL"`. Added `QUERY_ABBREVIATION_EXPANSIONS` +
`_expand_query_abbreviations`, wired into `_search_query_variants`. Caught mid-build that
expanding the search alone wasn't enough — `_names_match` needed the identical
substitution too, since `"COS"` isn't a string-prefix truncation of `"COMPANIES"` either;
a regression test against the real Michaels case caught it before shipping. Live-verified
17 of 22 sampled abbreviation-shaped names (new query strings, no prior cache — same
"can't quantify for free" situation as Phase 15/24). 4 new/changed tests; 557 pass (was
553), ruff/bandit clean. Real run: Tier E matched 2,843/4,322 -> 2,856/4,321 (+13; 4 of
the 17 sampled turned out to be separate, narrower gaps — SPAC roman-numeral sequel
truncation, a `"/DE/"` trailing-slash jurisdiction-tag shape `JURISDICTION_SUFFIX`
doesn't cover, a compound-suffix truncation mismatch — each a future lead, not this
fix's shape). Resolved-CIK era rows 14,130 -> 14,160 (+30); distinct CIKs resolved
8,612 -> 8,625; manual-research worklist 11,196 -> 11,166 eras, 149.2M -> 146.9M trade
rows. [CA][IV][REH][CDiP][KBT]

**Phase 28 (widen `JURISDICTION_SUFFIX` for a second trailing slash).** Tracing Phase
27's `TRC COS INC` gap found the real registrant is `"TRC COMPANIES INC /DE/"` — a
*second* trailing slash after the state code, a shape `JURISDICTION_SUFFIX`'s
end-anchored pattern didn't cover. 253 names in the current-listings index carry this
shape. Widened `/\s*[A-Z]+$` to `/\s*[A-Z]+\s*/?$`. Collision check: 3 new ambiguous
groups (`CITIZENS`, `FIRST BANCORP`, `INDEPENDENT BANK`) — each genuinely different real
companies sharing an identical base name post-strip, none reachable under the old
pattern either, so nothing regresses (documented transparently rather than claiming
"zero collisions"). Cache-only quantification: 24 names newly resolve, zero network
calls, zero cache misses, SIC codes spot-checked correct. 4 new tests; 561 pass (was
557), ruff/bandit clean. Real run: Tier E matched 2,856/4,321 -> 2,878/4,321 (+22); all
24 quantified names confirmed resolved. Resolved-CIK era rows 14,160 -> 14,207 (+47);
distinct CIKs resolved 8,625 -> 8,645; manual-research worklist 11,166 -> 11,119 eras,
146.9M -> 144.5M trade rows. [CA][IV][REH][CDiP][KBT]

**Phase 29 (add `"PUBLIC"` to `LEGAL_SUFFIXES`).** `DECOMA INTL INC` (Phase 27's
undiagnosed leftover) traced to a genuine negative result: its only valid EDGAR
candidate filed exclusively 2002-2005, completely disjoint from the 2022-2025 era —
`_provably_disjoint` correctly blocking a wrong match, not a bug. Moving to fresh
worklist rows, `HORIZON THERAPEUTICS PLC` traced a `LEGAL_SUFFIXES` gap: OpenFIGI's
`"PLC"` pops as a suffix, but SEC's own name spells it `"...Public Ltd Co"` — `"LTD"`/
`"CO"` pop, `"PUBLIC"` didn't, stopping one token short. Checked first: every
current-listings name ending in bare `"PUBLIC"` is part of this exact `"PUBLIC LTD CO"`
pattern (5 real names), never a genuine distinguishing word. Added `"PUBLIC"` to
`LEGAL_SUFFIXES`. Collision check: zero new ambiguous groups (16 either way). Cache-only
quantification: 2 names newly resolve (`HORIZON THERAPEUTICS PLC`, `KALERA PLC`), zero
network calls, zero cache misses, both spot-checked correct. 4 new tests; 565 pass (was
561), ruff/bandit clean. Real run: both confirmed resolved and gone from the worklist.
Resolved-CIK era rows 14,207 -> 14,219 (+12); distinct CIKs resolved 8,645 -> 8,648;
manual-research worklist 11,119 -> 11,107 eras, 144.5M -> 142.6M trade rows.
[CA][IV][REH][CDiP][KBT]

**Phase 30 (Tier E ticker-lookup fallback — new capability).** `HOLOGIC INC`/`SEAWORLD
ENTERTAINMENT INC`: real CIKs, name search finds only irrelevant subsidiaries or
nothing — one deregistered out of `browse-edgar`'s name index after going private, the
other renamed ("United Parks & Resorts Inc." in 2024). `action=getcompany&CIK=<ticker>`
resolves a ticker straight to a CIK via SEC's persistent ticker registry, independent
of name search. Added `lookup_cik_by_ticker` (`sec_company_search_client.py`) and
`_try_ticker_fallback` (`edgar_company_search_match.py`), tried once name search
doesn't land on a match; that single candidate goes through the identical acceptance
gate as every other source, so a stale/reused ticker is rejected on name mismatch, not
trusted outright. `unresolved_issuer_tickers` (`build_edgar_company_search_matches.py`)
maps each name to its one symbol, skipping names shared by 2+ symbols (~2%).

Two real design gaps found and fixed via full pipeline runs, not just unit tests: (1)
the fallback never fired on an early `ambiguous_candidates` return from inside the
name-search loop — the highest-value shape — fixed by extracting `_match_by_name` and
trying the fallback whenever it doesn't return `STATUS_MATCHED`, regardless of which
non-matched status; (2) several ticker-resolved candidates still failed
`_names_match`'s narrower truncation rule (Phase 22's deliberate limitation) — fixed
with `_names_match_broad`, reusing Tier D's `_is_prefix_relation` "extra trailing
tokens" branch, safe here (unlike Phase 22's broad-search context) because the ticker
already narrows the field to one candidate before any name check runs. Live-reverified:
Phase 22's two real false positives (`TPG PACE BENEFICIAL II`, `PRIME NUMBER
ACQUISITION`) now resolve to their *correct* entity via ticker lookup.

7 new tests; 578 pass (was 565), ruff/bandit clean. Extensively spot-checked the full
253-match result (majority are SPAC shells, SIC consistently Blank Checks; 4 non-SPAC
CIKs live-verified directly against `data.sec.gov`) — zero suspicious entries. Real run
(3 iterations as fixes landed): Tier E matched 2,904/4,314 -> 3,130/4,314. Resolved-CIK
era rows 14,219 -> 14,536 (+317); distinct CIKs resolved 8,648 -> 8,750; manual-research
worklist 11,107 -> 10,790 eras, 142.6M -> 127.4M trade rows — the largest single-phase
drop in this cycle. [CA][IV][REH][CDiP][KBT]

**Phase 31 (whitespace-tolerant bare share-class-letter descriptor).** Directly traced
Phase 30's one residual case, `TUSIMPLE HOLDINGS INC - A`: still failed even after the
ticker fallback found its real CIK, because the bare trailing letter pattern (`-A`
shape) requires the hyphen directly against the letter — `" - A"` (with spaces) survived
un-stripped, blocking the legal-suffix pop loop from ever reaching `"INC"`/`"HOLDINGS"`.
Widened to tolerate whitespace on both sides, same precedent as the `-CL A`/`-CLASS A`
fix (Phase 12/24). Cache-only quantification: 17 of 23 similarly-shaped names newly
resolve, zero network calls, zero cache misses, all correct. No collision-risk replay
needed (query-side-only pattern). 3 new tests; 581 pass (was 578), ruff/bandit clean.
Real run: Tier E matched 3,130/4,314 -> 3,153/4,314 (+23); all 17 confirmed resolved,
including `TUSIMPLE HOLDINGS INC - A` itself — Phase 30's residual case fully closed.
Resolved-CIK era rows 14,536 -> 14,565 (+29); distinct CIKs resolved 8,750 -> 8,766;
manual-research worklist 10,790 -> 10,761 eras, 127.4M -> 123.9M trade rows.
[CA][IV][REH][CDiP][KBT]

**Phase 32 (fuse dotted abbreviations — with a real safety correction).**
`US SILICA HOLDINGS INC`'s real EDGAR candidate is `"U.S. Silica Holdings, Inc."` —
`normalize_name`'s punctuation-stripping turned `"U.S."` into stray tokens `"U"`/`"S"`
instead of fusing to `"US"`, also silently blocking `LEGAL_SUFFIXES`' whole-token check
for `"SA"`/`"NV"`/`"LP"` (182 current-listings names carry this shape). Added
`DOTTED_ABBREVIATION`, collapsing 2+ consecutive single-letter-plus-period groups.

Collision-risk check surfaced something more consequential than usual: 6 new ambiguous
groups, each a real "Corp + sibling LP" pair. Tracing why revealed a genuine
**pre-existing** correctness bug this fix retroactively exposed: Tier D's
`_prefix_match_name` had been silently resolving 4 distinct real Navios Maritime
registrants (Holdings, Midstream Partners, Containers, Acquisition Corp —
live-confirmed as 4 different CIKs) all to the *same* CIK, because only Holdings'
period-free name stripped cleanly enough to look like the sole unique candidate. Fixing
the periods bug correctly exposed the ambiguity; a second full pipeline pass (the
standard "regenerate consistent state" pattern) let Tier E's more careful per-candidate
validation resolve 2 of the 5 affected symbols to their own distinct correct CIKs and
honestly leave the other 3 unresolved — net: 0 previously-correct matches lost.

Cache-only quantification (main yield): 30 names newly resolve, zero network calls,
zero cache misses, spot-checked correct. 8 new tests; 587 pass (was 581), ruff/bandit
clean. Real run: Tier E matched 3,153/4,314 -> 3,180/4,314; resolved-CIK era rows
14,565 -> 14,592 (+27, net of both the new matches and the Navios/Cheniere
corrections); distinct CIKs resolved 8,766 -> 8,786; manual-research worklist 10,761
-> 10,734 eras, 123.9M -> 122.6M trade rows. [CA][IV][REH][CDiP][KBT]

**Phase 33 (accept a backslash jurisdiction tag).** Re-scanning the fresh worklist:
`MYLAN NV` traced to a genuinely hard, structurally ambiguous tie between two real
sequential Mylan entities (pre-2015 `Mylan Inc.` and the 2015 Netherlands `Mylan
N.V.`/`Mylan II B.V.` restructuring), both plausible for the era — correctly left
unresolved rather than guessed, a legitimate negative result. Several other
renamed-successor names (`EVERNORTH HEALTH INC`, `VANTOR PARENT INC`, `BORGWARNER
JERSEY LTD`, others) have no SEC ticker-registry entry at all, same dead-end class as
`HTA`/`AGN`. `AGILITI INC` did resolve a CIK via ticker fallback but didn't validate:
SEC's own submissions data uses a *backslash* instead of forward slash for the
jurisdiction tag (`"AGILITI, INC. \DE"`, confirmed against `data.sec.gov`). Widened
`JURISDICTION_SUFFIX` to accept either slash direction. Zero occurrences in the
current-listings index (ticker-fallback-only names), collision check unchanged (22
groups either way). Cache-only quantification: 6 rows / 4 distinct companies newly
resolve (`AGILITI INC`; `DIAMOND EAGLE ACQUISITION CO` -> its real 2020 SPAC merger
into DraftKings Holdings; `LANDEC CORP` -> renamed Lifecore Biomedical; `PROTAGENIC
THERAPEUTIC`), zero network calls, zero cache misses, all correct. 4 new tests; 590
pass (was 587), ruff/bandit clean. Real run: Tier E matched 3,180/4,314 ->
3,186/4,314; all 6 rows confirmed resolved. Resolved-CIK era rows 14,592 -> 14,598
(+6); distinct CIKs resolved 8,786 -> 8,788; manual-research worklist 10,734 ->
10,728 eras, 122.6M -> 122.2M trade rows. [CA][IV][REH][CDiP][KBT]

**Phase 34 (fuse a possessive-contraction apostrophe).** Investigated the
`no_candidates` bucket (52 names) for a new shape — mostly ETFs (a different problem).
`PCTEL INC`'s real registrant is `"PC TEL INC"`, a genuine base-name word-split
deliberately not pursued (riskier than any punctuation fix so far). `CONN'S INC`,
`FLANIGAN'S ENTERPRISES INC`, `ART'S-WAY MANUFACTURING CO` all trace to the same real
gap: SEC drops a possessive apostrophe entirely (`"CONNS INC"`), while
`normalize_name` converts it to a token-splitting space instead, leaving a stray
one-letter `"S"` token. Added `POSSESSIVE_APOSTROPHE`, deleting an apostrophe only
when immediately followed by a bare `"S"` — narrow by construction, elsewhere (e.g.
`"O'Brien"`) it still becomes a space. Zero new collisions (22 either way). Cache-only
quantification: 4 names newly resolve, zero network calls, zero cache misses, all
correct. 6 new tests; 596 pass (was 590), ruff/bandit clean. Real run: Tier E matched
3,186/4,314 -> 3,190/4,314; all 4 confirmed resolved (two via Tier D's shared index).
Resolved-CIK era rows 14,598 -> 14,616 (+18); distinct CIKs resolved 8,788 -> 8,792;
manual-research worklist 10,728 -> 10,710 eras, 122.2M -> 121.4M trade rows. Also
traced `MYLAN NV` (structurally hard tie, correctly left ambiguous) as a negative
result. [CA][IV][REH][CDiP][KBT]

**Phase 35 (verified BDC-election auto-accept).** Revisits Phase 19's deliberate
decision to never auto-accept a blank-SIC lead, with genuinely new evidence. Phase
19's own docstring found even the strengthened bar (`entityType="operating"` +
substantive filing in era) insufficient, since a real match (`FIRST REPUBLIC BANK`)
also fails it (files its 10-Ks with its banking regulator, not SEC). Found that all 13
of the current `blank_sic_lead_high_confidence` population are real, well-known
Business Development Companies — a regulated structure under the Investment Company
Act of 1940 that legitimately carries no conventional SIC. BDCs file Form N-54A, a
formal, one-time, self-filed legal election that (unlike every prior signal) can never
be filed by an unrelated third party about a CIK — closing the exact "coincidental
secondary filer" gap Phase 19 named. Live-confirmed all 13 have exactly one N-54A
filing, zero archived-shard risk. `sec_sic_client.fetch_filing_activity` gained
`bdc_election_filed`; `edgar_company_search_match._find_verified_bdc_match` promotes a
lead to `STATUS_MATCHED` (`BASIS_VERIFIED_BDC_ELECTION`, SIC left honestly blank) only
when it also clears the existing high-confidence bar. Every other blank-SIC lead stays
exactly as informational as before. Downstream needed zero changes:
`build_era_sector_enriched.py` already has a `cik_no_sic` coverage status for exactly
this shape. 6 new tests (including an explicit no-N-54A regression case); 600 pass
(was 596), ruff/bandit clean. Real run: 14 BDCs promoted (13 from the sample plus `MVC
CAPITAL INC`, live-spot-checked correct); `blank_sic_lead_high_confidence_count`
dropped 13 -> 0. Resolved-CIK era rows 14,616 -> 14,635 (+19); distinct CIKs resolved
8,792 -> 8,799; `cik_no_sic` coverage now 590 era rows; manual-research worklist
10,710 -> 10,691 eras, 121.4M -> 120.8M trade rows. [CA][IV][REH][CDiP][KBT]

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
