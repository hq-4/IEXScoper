# Entity Resolution v2: OpenFIGI-Keyed Identity + Evidence-First Event Catalog

Approved methodology for the next resolution stage, persisted so anyone opening the
repo mid-process understands what is happening and why. This revision supersedes the
catalog-only version of this doc: the user obtained an OpenFIGI API key (`.env`,
`OPENFIGI_API_KEY`), adding a strong identity/classification pillar. Keep this doc
updated as phases complete. [CDiP]

## Status

- **Step 0 — persist plan in `docs/`: done** (this file, `docs/TASK_LIST.md`,
  `docs/DEAD_TICKER_RESOLUTION.md`, `docs/CHANGELOG.md`).
- **Phase 1 — OpenFIGI keyed full-universe enrichment: done** (2026-08-03).
  `includeUnlistedEquities=true` was the unlock (recall experiment:
  `reports/openfigi-identity/recall_experiment.json`): match rate **8,948 / 13,256
  symbols (67.5%)**, single-FIGI 8,501, multi-FIGI (reuse) 447, unmatched 4,308,
  zero errors, no 429s. Fund census: OpenFIGI `fund_etf` = **7,521 eras / 29.35%**
  vs regex 1,686 / 6.58% — the fund cohort is ~4.5× the heuristic estimate; the ETF
  gap is largely real, not bias. Outputs: `reports/openfigi-identity/`
  (`symbol_figi_map.parquet` with `query_variant` provenance, `era_classes.parquet`,
  `summary.json`); caches under `data/openfigi/` (resume-safe).
- **Phase 2 — event-catalog probe: done** (2026-08-04). Full Form 25 corpus 2016–2026
  (9,691 filings) fetched and parsed: catalog 2,285 rows (2,090 display-name tickers,
  56 issuer-name binds, 139 security-name binds — old-format exchange filings name the
  fund, e.g. "The Restaurant ETF"). Yield: **356 / 19,205 unresolved eras (1.9%)**,
  fund_etf 144/5,432 (2.65%). Honest conclusion: high precision, structurally small —
  most unresolved eras are intermittent/thin symbols, not delistings, so event catalogs
  only address the true-terminal subset. Form 25 parser work stops here (diminishing
  returns); N-8F optional later for fund trust binding. Outputs:
  `reports/event-catalog-probe/`, cache `data/event_catalog/cache/`.
- **Phase 3 — era binding + staging: done** (2026-08-04, stage
  `data/resolution/staged/7a35d98a404e95c5/`). **15,759 identity candidates**
  (`evidence_method=openfigi_symbol_identity`, `verification_state=candidate`) covering
  **89.15% of the 17,677-era unresolved baseline**, incl. 7,395 fund_etf eras; 363 Form
  25 event candidates. Multi-FIGI: 11 bound via Form 25 name disambiguation, 568 held.
  Corroboration tiers flag precision risk (single-FIGI binds can misassign reused
  tickers, e.g. AAAP): form25_agree 97, sec_name_agree 1,483 (high-confidence tier
  1,580), form25_conflict 235, sec_name_conflict 1,088, uncorroborated 12,856.
  **Nothing applied** — review starts with the 1,580-fact high-confidence tier.
  Builder: `utils/build_openfigi_identity_candidates.py`; summary:
  `reports/openfigi-identity/phase3_binding_summary.json`.
- **Tiered apply: done** (2026-08-04, zero manual review). Rationale: ground-truth
  measurement vs the 818 SEC-verified identities showed 68% exact / 74% high-similarity
  entity agreement, ~8-12% hard errors concentrated in reused tickers — too good to
  leave staged, too noisy to auto-verify. So the canonical store is now confidence-
  tiered: `verified` (818, SEC-grade, unchanged), `corroborated` (1,580: OpenFIGI
  agrees with Form 25 or SEC current name), `openfigi_asserted` (14,179; the 1,323
  conflicted facts carry a `contested` flag for exclusion from default joins). 363
  Form 25 `event_candidate` facts also applied. Apply tooling:
  `utils/apply_openfigi_identity_candidates.py` (dry-run default, idempotent, skips
  eras with verified identity; `--apply` to write). Downstream queries choose their
  assurance tier; nothing was overwritten or deleted.

## Assessment

The V3 evidence-delta architecture (`data/resolution/` fact stores, staged dry-runs,
era-keyed overrides, strict anti-false-positive gates) is sound engineering; the
methodology is what caps yield. Every high-yield lane does narrative-first SEC search:
take a ticker, search EFTS full text, hope a filing contains terminal language inside
a bounded window anchored on the same CIK. Recorded yields show the plateau:

- Terminal lane: `7,500` attempted → `37` verified (~0.5%).
- Lifecycle lane: `2,500` → `25`, then `861` → `9` (~1%).
- High-impact identity: `1,992` rows → `552` verified, `1,440` `no_identity_found`.
- `17,677` eras / `560.6M` trade rows still identity-unresolved.

The gates got stricter (correctly) after the rejected 26-proposal dry run, which pushed
yield further down. More iterations of the same lanes will keep returning ~1%. This is
a methodology plateau, not a tuning problem. [KBT]

### The ETF gap

Funds/trusts are ~6.8% of eras but only 3.2% of volume by the regex classifier, so
volume-weighted the gap is small. But the gap is structurally real for ETF-ecosystem
questions: fund closures almost never produce the terminal language the gates require.
A small issuer liquidating an ETF issues a press release and a supplement, files N-8F
to deregister the series months later, and the exchange files Form 25. No 8-K says
"ceased trading" from the filer CIK the identity anchor expects — the filer is the
trust, and the identity lane cannot anchor a series to the trust CIK. The generic
lanes miss this cohort ~100% of the time by construction; individual wins (FLIO, BIKR,
HJEN) only landed where the pattern accidentally matched. Phase 1's OpenFIGI census
replaces the regex estimate with an authoritative fund/ETF count.

## What the OpenFIGI key changes (verified capabilities)

- FIGIs are permanent and **delisted/inactive instruments remain queryable** via
  `/v3/mapping` — dead tickers, including dead ETFs, are mappable.
- A key raises throughput from ~10 jobs/request, 25 req/min to roughly 100
  jobs/request / ~25k jobs/min — the full ~13k-symbol universe is a minutes-scale run.
- Mapping returns per-FIGI `name`, `securityType`, `securityType2` (ETP, Common Stock,
  Preferred, Unit, Right, Warrant, …), `marketSector`, `exchCode`, composite FIGI:
  **authoritative instrument classification** and **ticker-reuse detection** (multiple
  FIGIs per ticker = multiple instruments across time; the existing
  `utils/openfigi_enrichment_core.py` keeps only `data[0]` — the new core retains ALL
  matches, because multi-match is signal for era work).
- Honest limits: OpenFIGI has **no validity dates, no CIK/CUSIP, no events**. It says
  *what* a ticker was, not *when*. Era binding still needs date evidence — exactly
  what the event catalog (Phase 2+) provides. The pillars compose.

## Phase 1 — OpenFIGI keyed full-universe enrichment (in progress)

- **Key plumbing**: new CLI calls `load_dotenv()` before reading `OPENFIGI_API_KEY`
  (the existing `utils/enrich_symbol_stability_openfigi.py` does not load `.env`;
  only `src/framework/config.py` does). Key is never logged (only `has_api_key`).
  `.env.example` gains an `OPENFIGI_API_KEY=` placeholder. [SFT]
- **New code** (existing enrichment untouched):
  - `utils/openfigi_identity_core.py` — keyed client (batch 100, backoff on 429/5xx,
    single-job fallback for poisoned batches), resume-safe JSONL cache at
    `data/openfigi/identity_cache.jsonl`, ALL matches retained per symbol.
  - `utils/build_openfigi_symbol_identities.py` — CLI; universe = unique symbols in
    `data/resolution/observation_facts.jsonl` (~13k).
- **Outputs** under `reports/openfigi-identity/`:
  - `symbol_figi_map.parquet` — one row per (symbol, FIGI): figi, composite_figi,
    name, security_type, security_type2, market_sector, exch_code, match_status
    (unmatched/single/multi), figi_count.
  - `era_classes.parquet` — eras joined to the symbol map with an authoritative class
    (`etp_fund / equity_common / adr / preferred / unit / right / warrant / other`)
    from securityType2; raw securityType2 kept for audit.
  - `summary.json` — match rate, multi-FIGI (reuse) rate, class distribution, and the
    **ETF census**: OpenFIGI `etp_fund` era share vs the regex `fund_or_trust`
    estimate (~6.8% / ~1,686 eras). This answers "is the ETF gap bias or real" with
    data.
- Tests in `tests/test_openfigi_identity.py` with recorded responses (no live
  network): multi-match retention, cache resume, classification, era join.

## Phases 2+ — evidence-first event catalog (pending Phase 1 review gate)

Rationale unchanged: delistings/deregistrations/fund closures are structured facts
published by authoritative sources; enumerate catalogs and join to eras on
ticker+date instead of crawling filings per ticker.

1. **SEC Form 25 / 25-NSE** (EFTS, 2016–2026): exchange-filed delisting notice;
   ticker + effective date. Tier: authoritative. **Probe status: running.**
2. **SEC N-8F / N-8F-NTC**: fund deregistration; trust CIK + series names. The
   ETF-tail killer. Tier: authoritative. **Added after Form 25 probe numbers land.**
3. ~~NasdaqTrader `nasdaqdelisted.txt`~~ — **discontinued** (verified 2026-08-04:
   302→404; FTP mirror dead). Only current-listing files survive; dropped as a source.
4. ~~Wikipedia defunct-ETF list~~ — **does not exist** (verified 2026-08-04; was an
   error in the original plan). Dropped.
5. **`company_tickers_mf.json`**: current fund ticker→CIK→series. Tier: lead only.

Planned sequence after the gate: coverage probe (join yield per source, split by the
Phase 1 authoritative classes, ETP hit rate called out) → normalized catalog store →
era binding (single-FIGI symbols → staged identity candidates; multi-FIGI → bind only
when catalog dates corroborate era boundaries; fund eras → trust-CIK candidates) → V3
staging (`data/resolution/staged/<stage_id>/`, dry-run first, `--apply` only after
human review).

## Invariants (what this does NOT change)

- Existing V3 gates, fact stores, and review queues stay authoritative; OpenFIGI and
  catalog output lands as *candidates* unless it passes review. Conflicts are flagged,
  never auto-resolved.
- No auto-apply anywhere; dry-run first.
- No paid data, no CUSIP layer, no preferreds/derivatives deep-dive.
- Lead-tier sources never land in canonical facts without human review.
- Full SEC filing text is never persisted; only metadata and bounded evidence.

## Expected outcome (honest)

Phase 1 alone should attach a named, typed identity candidate to a large share of the
~13k symbols (dead FIGIs remain queryable) and produce the first authoritative
fund/ETF census. Phases 2+ convert eras whose terminal dates corroborate catalog
events into reviewable facts. Series-to-ticker *era windows* for the ETF tail improve
substantially but not completely — date-free identity is OpenFIGI's hard limit, and
that residual is what only a licensed security master closes.
[CA][REH][IV][SFT][KBT][AS]
