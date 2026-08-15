# Task List

- 2026-08-15: SIC/sector classification, Phase 19 (`blank_sic_lead` research-lead flag —
  built as auto-acceptance, redesigned to informational after due diligence). User: "what
  else is next, i just merged" (after PR #22, Phase 18). Continued the same "fresh pass
  over the worklist's top rows" method Phase 18 used.
  Found `FIRST REPUBLIC BANK/CA` (priority rank 3, 1.86M trade rows): EDGAR's `"FIRST
  REPUBLIC"` search surfaces CIK 1132979, current name the *exact* string `"FIRST
  REPUBLIC BANK"`, but `_validate_candidates` rejects it outright for having a blank SIC
  — the name falls through to a hopelessly generic 1-word `"FIRST"` query (100+
  candidates, ambiguous). That CIK's own filing history has 42 filings spanning
  2004-2024, squarely covering FRC's 2016-2023 era — looked like overwhelming evidence to
  *auto-accept* the candidate, the same way `_provably_disjoint` (Phase 16) auto-*rejects*
  one. Built exactly that: `_blank_sic_admissible` admitting a blank-SIC candidate when
  its filing history is `ACTIVITY_PLAUSIBLE` for the era, a new `BASIS_BLANK_SIC_FILING_ACTIVITY`
  match_basis, quantified cache-only against the 1,844 still-unresolved names: 34 hits.
  **Before shipping**, spot-checked the resulting matches by hand (this project's
  standing practice) and found a real, serious flaw: fetching the *full* filing-type
  breakdown (not just dates) for all 34 candidates showed roughly half had
  `entityType="other"` and **zero substantive filings ever** — their entire history was
  ownership-disclosure forms (SC 13G, Form 3/4/5, 13F-NT) that any unrelated third party
  can file *about* a CIK regardless of whether it was ever the real operating entity
  (`BLACK KNIGHT INC`, `FARMER BROS CO`, `SHELL MIDSTREAM PARTNERS LP`, `STEEL PARTNERS
  HOLDINGS LP`, `WELLESLEY BANK` among them — all real companies, but these specific
  candidate CIKs are very likely the wrong entity). Worse: tightening the rule to require
  a genuine substantive filing (10-K/10-Q/8-K/etc.) landing in the era — the obvious fix —
  would have excluded `FIRST REPUBLIC BANK` itself too, since its entire 42-filing
  history is also ownership-disclosure-only (plausibly explained by Section 12(i): some
  banks file their real 10-Ks with their banking regulator instead of SEC, so SEC's own
  EDGAR never sees them). No reliable way to distinguish "genuinely the right company,
  files elsewhere" from "coincidental secondary filer" exists anywhere in this repo's
  data — auto-acceptance was abandoned.
  Redesigned as **informational-only**, mirroring Phase 18's already-proven-safe pattern:
  `_find_blank_sic_lead` surfaces the same candidate as `blank_sic_lead_cik`/
  `blank_sic_lead_name`/`blank_sic_lead_high_confidence` result fields, never changing
  `match_status`/`matched_cik`. `blank_sic_lead_high_confidence` (from the new
  `_is_high_confidence_lead`) requires both `entityType="operating"` *and* a substantive
  filing (`sec_sic_client.SUBSTANTIVE_FORMS` — a new constant distinguishing real
  operating/registration disclosure from ownership-disclosure forms) landing in the era —
  so a researcher sees every lead but knows which ones are worth checking first, rather
  than silently hiding the uncertain half or silently trusting them as fact.
  Threaded through the same three-layer pipeline as Phase 18 (`build_edgar_company_search_matches.py`'s
  schema/summary; a new `sector_enrichment_inputs.apply_blank_sic_lead` side-channel;
  the manual-research worklist as new columns, a summary count split by confidence, and a
  `[lead: CIK name ✓/?]` marker in the top-rows table). 12 new tests (auto-accept path
  fully replaced with informational-path tests: lead surfaced without matching, disjoint/
  unknown activity never leads, no era_span never leads, the Confluent shell regression
  guard, high-vs-low-confidence flagging, `SUBSTANTIVE_FORMS`/`entity_type` extraction in
  `sec_sic_client`); 535 tests pass (was 531 going into this phase); ruff/bandit clean.
  Real run (three passes total): first pass confirmed match outcomes byte-identical to
  pre-Phase-19 (`matched_count` 2,441 unchanged) but only found 9 leads — far short of the
  34 quantified. Diagnosed live: a real bug in the loop's cap-carryforward — a lead found
  at a narrower query level (`"FIRST REPUBLIC"`, 12 candidates) was silently discarded
  when a later, broader query (`"FIRST"`, 100+ candidates) hit the `MAX_CANDIDATES_TO_VALIDATE`
  cap and returned immediately without carrying `blank_sic_lead` forward — losing the
  real `FIRST REPUBLIC BANK` lead itself, the phase's own motivating case. Fixed (one
  missing `blank_sic_lead=blank_sic_lead` kwarg), added a regression test reproducing the
  exact scenario, reran: **34 leads found (12 high-confidence)**, `FRC` correctly carries
  `blank_sic_lead_cik=1132979`. Reconciled: `distinct_ciks_resolved` unchanged at 8,448;
  manual-research worklist unchanged at 11,783 eras / 166.4M trade rows (fully expected —
  informational-only, no CIK resolution changed); worklist-level lead count 53 across
  eras (34 unique names, some spanning 2+ eras), 17 high-confidence. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-15: SIC/sector classification, Phase 18 (`identity_disproven` worklist flag). User:
  "what else is next, i just merged" (after Phase 17's negative-result docs PR). Phase 17's own
  conclusion was that no further quantified backlog item remained — the next lead would need a
  fresh pass over the worklist's current top rows, the same method every prior phase used. Did
  exactly that: checked the freshly-regenerated worklist's top rows and found `UTX` (priority
  rank 29, 919,517 trade rows, era 2016-12-12..2020-04-03) carrying `identity_issuer =
  "ULTRATREX INC-A"` — a real but obviously-unrelated shell. `UTX` is the historic ticker for
  United Technologies Corp, a NYSE mega-cap that merged into Raytheon Technologies on 2020-04-03
  — the exact `last_day` this era ends on.
  Traced the root cause to the identity-assertion layer itself, one step upstream of anything
  this project's Tier A-E work touches: `utils/openfigi_identity_core.py` queries OpenFIGI's
  `/v3/mapping` by bare ticker string with no date awareness (`build_openfigi_job`), and
  `build_era_rows` takes the *first* returned FIGI unconditionally. Confirmed directly against
  the raw cached response (`data/openfigi/identity_cache.jsonl`): OpenFIGI's own index returns
  exactly *one* FIGI for `"UTX"` today — `BBG01X4WM088`, `"ULTRATREX INC-A"` — not United
  Technologies, because OpenFIGI's ticker index reflects current/latest ticker ownership and
  `UTX` has since been reused. The same current-listing-bias root cause this project has hit at
  every other layer (Tier C, `ticker_continuity`, the reverted Phase 13 ticker tie-break), now
  found one layer further upstream, at the identity assertion itself rather than at CIK
  resolution. Checked whether Phase 6's IEX-snapshot fallback (`apply_iex_fallback_issuer`)
  could recover the real name instead: no — IEX's own local snapshot data has zero coverage for
  `UTX` (`iex_entity_confidence: iex_snapshot_unmatched`), and the fallback only backfills a
  *null* `identity_issuer` anyway, never overrides a wrong-but-present one. No available data
  source in this repo can recover the correct name for this specific case — this is genuinely a
  "no automatic answer, needs a human" case, exactly what the worklist exists for.
  What *is* fixable: Phase 16's own filing-activity evidence already proves `"ULTRATREX INC-A"`
  can't be the operating entity for `UTX`'s era (Ultratrex's real SEC filings are 2025-2026,
  entirely outside the 2016-2020 window) — that proof existed internally as a silent rejection
  reason but was never surfaced to the worklist, where a human researcher would see the name and
  waste time googling the wrong company. Quantified the population before building: 74 names
  still carry this exact proof after Phase 16 (82 originally flagged, 8 recovered via the
  tie-break), touching 100 worklist eras and 3.66M trade rows — several in the top 200 priority
  rows (`UTX` #29, `FIT`/Fitness Fanatics Ltd #52, `AET`/Aetna Inc #96, `JAG`/Job Aire Group Inc
  #107).
  Built `identity_disproven`: `edgar_company_search_match.match_issuer_name` now tracks whether
  `_provably_disjoint` (Phase 16) rejected any single-candidate match at any truncation level,
  surfaced as a new boolean field in the result dict regardless of final status — purely
  additive metadata, no matching-logic change. Threaded through
  `build_edgar_company_search_matches.py`'s `RESULT_SCHEMA` and summary,
  `sector_enrichment_inputs.py`'s new `apply_identity_disproven` (backfills by `identity_issuer`
  onto every era, `False` when a name was never searched — never assumed fine), wired into
  `build_era_sector_enriched.py`'s orchestrator as a side-channel that never touches
  `resolved_cik`/`cik_source` (informational only, can't change what gets a CIK), and surfaced in
  the manual-research worklist (`build_sector_manual_research_worklist.py`) as a new column, a
  summary count, and a struck-through `~~name~~ (disproven)` marker in the top-rows markdown
  table. 8 new tests across the touched modules (the field's presence/defaults, tie-break
  interaction, graceful degradation reading an older matches file built before this phase, and a
  full orchestrator end-to-end proof using the real `UTX` shape); 522 tests pass (was 514);
  ruff/bandit clean.
  Real run (`build_edgar_company_search_matches.py` + `build_era_sector_enriched.py` +
  `build_sector_manual_research_worklist.py`, ~90s wall-clock total, entirely cache hits since
  nothing about matching itself changed): confirmed byte-identical match outcomes to before this
  phase (`distinct_ciks_resolved` 8,448, worklist 11,783 eras / 166.4M trade rows, both
  unchanged) except the new field — `identity_disproven_count: 82` in the EDGAR-search summary,
  **100 worklist eras** flagged. Verified `UTX`'s worklist row now carries
  `identity_disproven: True` and renders as `~~ULTRATREX INC-A~~ (disproven)` in the report.
  Next candidates, not started: whether a similar "current-listing-bias, no available fallback"
  pattern affects other still-unresolved names beyond this specific 74-name filing-activity-proof
  population (not investigated — this phase only surfaced evidence already computed, it didn't go
  looking for new instances); `GROUP`-suffix over-normalization remains closed (Phase 17,
  negative result). `[CA][IV][REH][CDiP][KBT]`

- 2026-08-15: SIC/sector classification, Phase 17 attempt (`GROUP`-suffix over-normalization) —
  investigated, quantified, and **not shipped**: negative result, same shape as Phase 13's
  ticker-based tie-break attempt. Picked up as the last open thread from this session's earlier
  "what else is next" choice (the alternative to Phase 16's audit).
  Root hypothesis (Phase 12): `sec_name_cik_lookup.normalize_name`'s legal-suffix-strip loop
  treats `"GROUP"` as droppable, so unrelated companies whose names differ only by that word
  (`"Continental Resources, Inc."` vs `"Continental Resources Group, Inc."`) collapse to the same
  normalized string and cause false collisions.
  Quantified both tiers that share `normalize_name`, entirely from local data/cache (zero network
  calls):
  - **Tier D** (SEC current-listing name index, `sec_name_cik_lookup.build_name_cik_index` +
    `match_by_name`): rebuilt the index and replayed every era with `GROUP` removed from
    `LEGAL_SUFFIXES`. Result: **54 currently-correct matches would be LOST**, only **9 gained** —
    a clear net negative, not a wash. The losses aren't bugs being fixed; they're genuine
    same-company matches that depend on `GROUP`-stripping to bridge a real naming-convention gap
    — SEC's own `sec_name` field is sometimes shorter than the market name (`"BRP Inc."` for the
    real "BRP Group, Inc.", CIK 1748797; `"FG Holdings Ltd"` for "FG Group Holdings Inc") — or to
    correctly prefix-match a family of SEC-registered financial products issued under a parent
    holding company's own CIK (13 of the 54 are Goldman Sachs Group-issued structured
    notes/ETFs correctly resolving to CIK 886982; without `GROUP` stripped, `"GOLDMAN SACHS
    GROUP"` (3 tokens) no longer satisfies the prefix-match's exact-trailing-token rule against
    `"GOLDMAN SACHS ACCESS ..."`, so the match is lost). Separately checked how often `GROUP`
    itself is even the cause of a raw SEC-universe name collision (the theorized mechanism): only
    **4 of 13** total ambiguous-normalized-name collisions in the whole SEC current-listing table
    involve `GROUP` at all (`QVC`, `UBS`, `BRC`, `TARGET`) — and all 4 are already safely excluded
    from the Tier D index today by the existing "2+ CIKs collapse -> drop, never guess" design, so
    they were never a false-match risk to begin with, only a lost-match one.
  - **Tier E** (`edgar_company_search_match._names_match`, live EDGAR search + validate): replayed
    all 1,095 currently-`ambiguous_candidates` names against the existing SQLite request cache
    (network calls forcibly blocked via a monkeypatched `requests.get` that raises, to prove
    nothing live was needed) with `GROUP` removed from `LEGAL_SUFFIXES`. Result: **1 of 1,095**
    newly resolved (`MOXIAN INC` -> CIK 1516805, via the existing filing-activity tie-break).
    Essentially zero yield — the specific case this thread was originally about, `CLR`/Continental
    Resources, was already solved by Phase 14's filing-activity tie-break (an independent-evidence
    disambiguator that doesn't touch `normalize_name` at all), so the collision-avoidance benefit
    this fix would have provided for Tier E was already captured by a safer mechanism before this
    phase even started.
  Conclusion: shipping this would cost real, verified-correct matches (mostly in Tier D) for
  essentially no yield in either tier. Not built — no code changed, only throwaway quantification
  scripts (not committed), same "quantify before touching shared logic" discipline the module's
  own history already demands of any change to `normalize_name`. `GROUP` stays in
  `LEGAL_SUFFIXES`, unchanged. With this and Phase 16 both closed, every specific lead this session
  identified is now either shipped or ruled out with evidence — no further concrete, quantified
  candidate remains queued; the next step is a fresh pass over the current worklist's top rows
  (the same method every prior "what else is next" phase used to find its lead in the first
  place), not a known backlog item. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-14: SIC/sector classification, Phase 16 (81-name existing-match filing-activity
  audit). User: "what else is next," offered a scoped choice between two threads Phase 14/15
  left open — the deferred audit of already-`matched` names for filing-activity-disjoint false
  positives, or fixing `normalize_name`'s `GROUP`-suffix over-normalization (Phase 12/13, still
  untouched) — and picked the audit as lower-risk (bounded, cache-only to quantify) and
  higher-value (hunting real errors already shipped in the resolved output).
  Root cause: `_disambiguate_by_filing_activity` (Phase 14) only ever ran when 2+ candidates
  validated by name — a *single* validated candidate was accepted on name+SIC alone, its filing
  history never checked against `era_span` at all, even when flatly disjoint. Quantified first,
  cache-only via a throwaway script (`EvidenceRegistry.get(...)` read directly, zero network
  calls, mirroring `_filing_activity_verdict` exactly): of the 2,515 then-`matched` names, 82
  provably disjoint (e.g. `AETNA INC` -> CIK 1013761, filings 1996-2015 for an era starting
  2016-12-12 — the real operating Aetna for those eras is CIK 1122304; several modern-SPAC name
  collisions like `ALBATROSS ACQUISITION CORP`, matched to a CIK whose entire filing history
  postdates the era by years), 42 more `ACTIVITY_UNKNOWN` (bracketing history, no filing landing
  inside a narrow era window — correctly left alone, same "quiet filer" reasoning as the
  tie-break guard). Spot-checked a sample by hand before building — all genuinely wrong, not
  edge cases (multi-year gaps between the matched CIK's filing history and the era, not narrow
  misses).
  Built `_provably_disjoint`, gating the single-candidate accept the same way
  `_disambiguate_by_filing_activity` already gates a multi-candidate one: `continue` to a shorter
  query instead of returning a confidently-wrong match — a name whose only reachable candidate is
  disjoint gets a chance to resolve against a different candidate at a broader query rather than
  simply losing its match outright. Fails open exactly like the tie-break guard: no `era_span` or
  a failed filing-activity fetch never rejects. This intentionally reverses a Phase 14 regression
  test that had locked in the opposite behavior — that test existed to keep the single-candidate
  path unchanged *until this exact audit ran*, not as a permanent guarantee, per its own Phase 14
  narrative explicitly deferring this audit. 5 new tests (rejection, no-era-span backward-compat,
  quiet-filer-stays-matched, falls-through-to-a-broader-query-and-resolves-correctly, fetch-error
  fails open); the old regression test rewritten to assert the new (correct) behavior. One
  downstream integration test's submissions fixture needed a real filing date added (previously
  missing `filings` entirely, an unrealistic shape no real SEC payload has) to stay clear of the
  new guard, since it wasn't testing filing-activity behavior. 514 tests pass (was 510);
  ruff/bandit clean.
  Real run (`build_edgar_company_search_matches.py` x2 — second pass cleared 8 transient SEC
  fetch errors from the first, nothing cached on `fetch_error` so they retried cleanly — then
  `build_era_sector_enriched.py` + `build_sector_manual_research_worklist.py`; ~17 minutes
  wall-clock total, genuinely rate-limited since the shorter-query retries for 82 names were
  mostly uncached): all 82 cleared out of `single_validated_candidate` — **8 resolved to a
  different, correct CIK** via the tie-break (`EXTENDED STAY AMERICA INC` 1002579 -> 1581164, the
  post-2013-bankruptcy-reorg entity; `MERIDIAN BANCORP INC` -> 1600125; `NIELSEN HOLDINGS LTD` ->
  1492633; `OSIRIS THERAPEUTICS INC` 912815 -> 1360886; `PEAK RESOURCES LP-A` -> 1393548;
  `PINNACLE ENTERTAINMENT INC` -> 1656239; `SURGICAL CARE AFFILIATES INC` -> 1411574; `VIEWRAY
  INC` -> 1597313), **45 honestly `no_validated_match`**, **29 honestly `ambiguous_candidates`**,
  zero left wrongly matched. Tier E matched 2,515/4,339 -> **2,441/4,339** (-74, expected —
  correctness over coverage, not a regression). Reconciled: distinct CIKs resolved 8,517 ->
  **8,448**; manual-research worklist 11,683 -> **11,783 eras**, 162.8M -> **166.4M trade rows**
  (all three moved in the expected direction — fewer resolved, more back on the research
  worklist, since 74 fewer names are now confidently matched). Next candidates, not started:
  `GROUP`-suffix over-normalization (Phase 12/13, still untouched — the other open thread from
  this "what else is next" choice); whether the 71 still-`ambiguous`/`no_validated_match` names
  from this audit's 82 are recoverable some other way (not investigated — this phase's scope was
  the rejection guard itself, not chasing down replacements for every name it correctly
  unmatched). `[CA][IV][REH][CDiP][KBT]`

- 2026-08-14: SIC/sector classification, Phase 15 (raise Tier E's candidate-validation
  cap). User: "what else is next," offered a scoped choice after checking the freshly
  Phase-14-shrunk worklist's top rows and finding most of them (`GPS`, `CFLT`, `HOLX`,
  `FRC`, `HZNP`, `CDEV`, ...) were hitting the `>MAX_CANDIDATES_TO_VALIDATE` search-count
  guard (9-100 candidates), explicitly flagged as out of scope in Phase 14's own docs.
  Unlike every prior phase this session, this one genuinely couldn't be quantified for
  free first: names with 9-20 candidates had *never* been validated at all under the old
  cap of 8 (the count guard returns before fetching any of them), so nothing sat in the
  cache to replay — verified this directly (972 cache misses across every candidate in
  that population on a first quantification attempt). Presented the real trade-off
  honestly (a genuine ~30-40 minute live SEC run needed just to learn the yield, no way to
  know in advance); user: do the speculative live run.
  `MAX_CANDIDATES_TO_VALIDATE` raised 8 -> 20 in `edgar_company_search_match.py` — one
  constant, no other logic changed; the risk the original 8-cap guarded against was never
  "validating more candidates is dangerous" (validation is exact-match, a wrong candidate
  simply fails to validate), it was "validating an implausibly generic query's candidates
  wastes requests," and Phase 14's filing-activity guard already proved candidates can be
  validated safely at scale. 2 tests hardcoding a literal candidate count of 9 for the
  over-cap case were parameterized off the real constant instead of a magic number so
  they stay correct regardless of its value. 510 tests still pass; ruff/bandit clean.
  Real run (~19 minutes wall-clock, genuinely rate-limited since this candidate
  population was entirely uncached): matched 2,495/4,339 -> **2,515/4,339** (+20: 18
  ordinary single-candidate validations, 2 more via the filing-activity tie-break);
  `ambiguous_candidates` 1,296 -> **1,066**; `no_validated_match` 494 -> **704** (+210) —
  a valuable reclassification, not a regression: names previously sitting in "ambiguous,
  never actually checked" turned out, once validated, to contain zero real matching
  candidates at all, correctly reclassified rather than left in a falsely-alarming
  bucket. Reconciled: distinct CIKs resolved 8,500 -> **8,517**; manual-research worklist
  11,707 -> **11,683 eras**, 169.7M -> **162.8M trade rows**. Notably resolved the two
  running examples this module's docstring has cited since Phase 5/7/11 as "correctly
  staying unresolved": `CFLT` (`CONFLUENT INC-CLASS A` -> the real Confluent, Inc., CIK
  1699838, the blank-SIC shell correctly rejected same as always) and `MYLAN NV` (-> CIK
  69499) — both purely because their candidate counts (9 and 12) had simply never fit
  under the cap before, not because of any new logic. Spot-checked additional new matches
  (`IAA INC` -> IAA, Inc. CIK 1745041, SIC 5500 Retail-Auto Dealers — real, spun off from
  KAR Auction Services 2019; `NCI INC-A` -> NCI, Inc. CIK 1334478, SIC 7373 — real
  government IT services company) — correct. Updated the module's own docstring, since
  three prior phases' narrated examples (the exact `MYLAN`/`CONFLUENT` candidate counts)
  were tied to the old cap value and would otherwise read as still-true. Next candidates,
  not started: the deferred 81-name existing-match audit from Phase 14; whether to raise
  the cap further (504 of the remaining 1,066 ambiguous names sit at the 100-candidate API
  page cap — genuinely too generic, unlikely to be worth it at any cap). `[CA][IV][REH][CDiP][KBT]`

- 2026-08-14: SIC/sector classification, Phase 14 (Tier E filing-activity tie-break).
  User: "look at all of these failed methods and think of better ways to resolve this
  then execute." An Explore agent surveyed the existing SEC resolution tooling for a
  reusable "was this CIK active on date X" signal and found `filings.recent` (and
  `filings.files` for older shard history) sitting in the exact same submissions payload
  `fetch_sic` already fetches for every candidate — completely unread until now. For the
  real Continental Resources, Inc. (CIK 732834, ticker `CLR`, SIC 1311): filings span
  2009-12-28 through 2026-07-31 continuously, plus a shard back to 1998, covering the
  entire IEX TOPS data era (2016-12-12 onward). Its Phase-13 collision partner, an
  unrelated junior-mining shell "Continental Resources Group, Inc." (CIK 1430975, SIC
  1000 — a *real*, non-blank SIC, so the existing blank-SIC guard doesn't apply): last
  filing a `15-12G` voluntary deregistration on 2013-03-05, permanently dark since —
  structurally impossible to be the trading entity for any era after ~2013.
  A Plan agent then ran a read-only cached-data probe (reading
  `data/resolution/evidence_registry.sqlite` directly via `EvidenceRegistry.get(...)`,
  never constructing a `CachedPrimaryClient`, so zero possibility of a network call) and
  found real yield this time, unlike Phase 13's ticker attempt: of 1,340
  `ambiguous_candidates` names, 74 enter via a genuine 2-way validated name tie; ~38 of
  those resolve under a strict accept rule, confirmed correct for `CLR`. Scope confirmed
  with the user: tie-break only, touching only the already-ambiguous names — the same
  rule, applied to the 2,453 already-`matched` names, would separately flag 81 as having
  disjoint filing history (e.g. `AETNA INC -> 1013761`, likely a genuine pre-existing
  false positive — the operating Aetna for 2016-2018 eras is `1122304`) and 42 more as
  inconclusive; that audit is explicitly deferred to a future phase, not touched here.
  Built `sec_sic_client.fetch_filing_activity` (reuses the identical cache key `fetch_sic`
  already populated — zero new network cost in practice), `edgar_company_search_match`'s
  `_filing_activity_verdict` (three outcomes — plausible / provably disjoint / can't-tell
  — a candidate whose history merely brackets the era without a filing landing inside it
  is deliberately `unknown`, never rejected, since a real filer can be quiet across a
  short window) and `_disambiguate_by_filing_activity` (accepts only when every tied
  candidate gets a definite verdict and exactly one is plausible), wired into
  `match_issuer_name` via a new optional `era_span` parameter mirroring the (reverted)
  Phase 13 `symbol` shape. `_validate_candidates` no longer stops at 2 validated
  candidates — checked live: 0 names currently sit at 3-8 validated candidates, proof the
  break was binding; removing it cost 3 quantification-time accepts (41->38) that were
  only "unambiguous" because a real 3rd candidate was never looked at, correctly traded
  for full visibility into every genuine tie.
  `build_edgar_company_search_matches.unresolved_issuer_era_spans` supplies the per-name
  `(min(first_day), max(last_day))` union across every unresolved era sharing that name
  (name-deduped, same granularity the module already runs at — a wider union only ever
  makes the guard more permissive, never causes a false accept). 16 new tests (5
  filing-activity extraction incl. a "reuses cached payload, zero new requests" proof, 8
  tie-break scenarios incl. the CLR end-to-end case and the critical single-candidate
  regression guard, 3 era-span plumbing); 510 pass (was 494); ruff/bandit clean.
  Quantified via a throwaway script (not committed) against cached data before writing
  the real integration — zero network calls, positive control (`CONTINENTAL RESOURCES
  INC/OK -> 732834`, shell rejected) confirmed passing.
  Real run (`build_edgar_company_search_matches.py` + `build_era_sector_enriched.py` +
  `build_sector_manual_research_worklist.py`, ~45s wall-clock, cache-heavy): **43 names**
  resolved via `filing_activity_tiebreak` (slightly above the cache-only floor of ~38, as
  expected — some ties had an uncached 3rd+ candidate the live run could finally see).
  Tier E matched 2,453/4,342 -> **2,495/4,339**; distinct CIKs resolved 8,470 -> **8,500**;
  manual-research worklist 11,791 -> **11,707 eras**, 180.8M -> **169.7M trade rows**.
  Spot-checked all 43 newly-resolved matches by eye against known real companies (Raytheon
  Company SIC 3812, CIT Group Inc SIC 6021, Mead Johnson Nutrition SIC 2000, Nuance
  Communications SIC 7372, Sealed Air Corp SIC 2820, Callon Petroleum SIC 1311, IAC Inc
  SIC 7370, Rexnord Corp SIC 3560, Vivint Smart Home SIC 7381, GCI Liberty SIC 4813, MB
  Financial Inc SIC 6021, Renewable Energy Group SIC 2860, Welbilt Inc SIC 3580, among
  others) — all correct. Next candidates, not started: the deferred 81-name existing-match
  audit; the 1,266-name `>MAX_CANDIDATES_TO_VALIDATE` bucket (filing activity is a
  plausible enabler for raising that cap, a separate larger phase); `filings.files` shard
  walking (measured to unblock exactly 1 name today, not built). `[CA][IV][REH][CDiP][KBT]`

- 2026-08-13: SIC/sector classification, Phase 13 attempt (ticker-based ambiguity
  tie-break) — built, tested, then reverted after live quantification showed ~0 real
  yield. User: "what else is next," offered a scoped choice between the
  `ambiguous_candidates` bucket's low-risk 2-way-tie subset (74 names) and the riskier
  `GROUP`-suffix fix; picked the low-risk tie-break. Design: when exactly 2 candidates
  both validate by name/formerName (e.g. real `CLR`/Continental Resources vs an
  unrelated "Continental Resources Group, Inc." shell), accept whichever candidate's SEC
  `tickers` list contains the era's own symbol — reusing
  `utils.ticker_continuity.fetch_current_tickers`'s identical `sec_submissions` cache
  key, so free in the real client. Built `edgar_company_search_match.match_issuer_name`'s
  `symbol` parameter + `_disambiguate_by_ticker`, and
  `build_edgar_company_search_matches.unresolved_issuer_symbol_map` (name -> its one
  associated symbol, skipped for the rare name shared across >1 symbol — 71/73 real
  cases had exactly one). 5 new tests, 499 passed, ruff/bandit clean.
  Live quantification against cached data (zero new network calls) before shipping
  caught the flaw: **0 of 74 names actually resolved.** Root cause — SEC's `tickers`
  field on the submissions payload reflects only *current* listing state, never
  historical ticker history the way `formerNames` carries historical name history. This
  entire worklist bucket is, by construction, companies no longer trading — checking the
  70 real 2-way ties with a known symbol, 65/70 (93%) had *both* candidates showing empty
  `tickers` (confirmed not a fetch bug: Apple's CIK correctly returns `["AAPL"]` as a
  positive control). `CLR`/Continental Resources itself was taken private in 2023 and
  shows `tickers: []` today, identical to the unrelated shell it's tied with — the same
  current-listing bias this project has hit repeatedly (Tier C, OpenFIGI's ticker-keyed
  lookup, Phase 6's rename gap), one layer further down than any prior phase reached.
  Presented the negative result plus three options (keep it anyway since it's harmless/
  free, revert, or try a filing-activity signal instead); user: revert. `git checkout --`
  on all four touched files, confirmed working tree clean and 499/499 still passing on
  `main` post-revert. **Any future disambiguator for this bucket needs a signal that
  survives delisting** — filing-activity-during-the-era (SEC submissions' `filings.recent`
  date-stamped) is the untried candidate; ticker/current-listing-derived signals are a
  dead end for this population. `GROUP`-suffix over-normalization (Phase 12) remains the
  other open lead, untouched by this attempt. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-13: Test suite parallelized. User: "can you parallelize tests if possible,"
  asked mid-turn while Phase 12 was wrapping up. Checked parallel-safety before adding
  anything: every test's file I/O runs through the `tmp_path` fixture, nothing `chdir`s
  the process, and no test writes a shared real path (`reports/`, `data/`) or mutates an
  unguarded env var — confirmed by grep across all 84 test files, not assumed. Added
  `pytest-xdist` and set `-n auto` in `pyproject.toml`'s `addopts`, so the canonical
  `uv run -m pytest -q` command is parallel by default with no flag change needed anywhere
  else (CI, docs, muscle memory).
  The first parallel run immediately failed one real test —
  `test_workflow_emits_structured_progress_logs` — not a parallelization artifact but a
  genuine latent bug the new run order finally exposed: the test's `caplog.set_level(...,
  logger="utils.sec_high_impact_workflow")` targeted a logger name from before the
  2026-08-05 `utils/` -> `utils/legacy/` reorg; the real logger (per
  `utils/legacy/sec_high_impact_logging.py`) is
  `"utils.legacy.sec_high_impact_workflow"`. It only ever passed under strict serial
  execution because an earlier test's global root-logger level (mutable, process-wide
  state) happened to leak through in that fixed order — a real test-isolation smell
  masked by always running in the same sequence. Fixed the logger name, not the symptom.
  Re-ran 3x after the fix with no flakiness. Wall-clock: 494 tests, ~10.8s serial ->
  ~6.5s parallel on this machine. `[PA][REH][CDiP]`

- 2026-08-13: SIC/sector classification, Phase 12 (share-class spacing gaps). Session opened with
  a housekeeping step first: Phases 9-11 (`docs/TASK_LIST.md` already narrated them, 488 tests
  passing) had been built and verified in a prior session but never committed — sitting directly
  on `main`, unbranched. Verified the diff matched the narrative exactly (488/488 tests, ruff
  clean) before branching, committing, and opening PR #13, per the user's explicit choice
  ("commit it now, then find what's next") over leaving it uncommitted.
  Then, "what else is next": checked the freshly-regenerated worklist's top rows and traced why
  several real, still-findable companies (`SEE`/Sealed Air, `CPE`/Callon Petroleum,
  `HZNP`/Horizon Therapeutics, `IAC`, `CLR`/Continental Resources) were landing in
  `ambiguous_candidates` or `no_validated_match` — by replaying the *actual* `match_issuer_name`
  function against the live worklist names, not just eyeballing candidate lists (an early manual
  trace nearly mis-diagnosed `HZNP` as a `PLC`/"Public Ltd Co" suffix gap; only re-running the real
  function surfaced that its 2-word query never even finds a name-matching candidate, so it falls
  through to `HORIZON` alone and hits the candidate-count cap — a reminder to trust the actual code
  path over a manual approximation of it). That trace surfaced a bigger, separately-risky
  discovery — `normalize_name`'s legal-suffix strip loop treats "GROUP" (and other generic
  business words) as a droppable suffix, so `"Continental Resources Group, Inc."` (an unrelated
  junior mining shell, SIC 1000) and `"CONTINENTAL RESOURCES, INC"` (the real Harold Hamm oil
  company, ticker `CLR`, SIC 1311) both collapse to the same normalized name and jointly cause a
  false `ambiguous_candidates` — but a fix needs a disambiguation signal (SEC's own `tickers` field
  from the same submissions payload, already fetched, unused) and careful two-directional live
  verification against everything currently resolved correctly through that suffix list, so it's
  deliberately left as a follow-up, not built today.
  What *was* narrow and safe enough to ship today: two spacing gaps in already-accepted regex
  patterns, found the same way as Phase 9's `-CL A` spacing fix. `DESCRIPTOR_PATTERNS`'
  `-CLASS [A-Z]$` pattern required the hyphen to sit directly against "CLASS" with no space, so
  `"SWEETGREEN INC - CLASS A"` and `"FIRST DATA CORP- CLASS A"` never got stripped even though the
  equivalent abbreviated `-CL A` pattern already tolerated that spacing since Phase 9 — now fixed
  to mirror it (and both `-CL A`/`-CLASS A` patterns now also consume a space *before* the hyphen,
  which the original Phase 9 fix left as a trailing-space artifact, caught by a direct
  `strip_security_descriptors` unit test rather than by any matching behavior actually breaking).
  Separately, `JURISDICTION_SUFFIX` only matched a tight `/XX` with no space, but SEC's own
  submissions payload returns `"Alight Inc. / DE"` — a spaced variant the tight `Core Scientific,
  Inc./tx` precedent from Phase 7 never covered. Quantified entirely by replaying the full
  still-unresolved Tier E population (1,904 names) against the cached search/validation responses
  already on disk with both fixes applied — zero new network calls: 15 names flip to a validated
  match. 6 new tests; 494 pass (was 488); ruff/bandit clean.
  Real run (`build_edgar_company_search_matches.py` + `build_era_sector_enriched.py` +
  `build_sector_manual_research_worklist.py`, ~40s wall-clock, essentially all cache hits): Tier E
  matched 2,438/4,342 -> **2,453/4,342**; distinct CIKs resolved 8,455 -> **8,470**;
  manual-research worklist 11,817 -> **11,791 eras**, 187.7M -> **180.8M trade rows**. Spot-checked
  all 6 largest newly-resolved matches against known real companies (`SG`->Sweetgreen SIC 5812
  Retail-Eating Places, `ALIT`->Alight Inc SIC 7374, `AYX`->Alteryx SIC 7372, `FDC`->First Data
  Corp SIC 7389, `MCFE`->McAfee Corp SIC 7372, `VEI`->Vine Energy SIC 1311) — all correct; `SG`
  resolved via Tier D (`sec_name_matched`) rather than Tier E, confirming the shared
  `strip_security_descriptors` fix benefits both tiers as designed. Next candidate, not started:
  a ticker-based disambiguator for the `ambiguous_candidates` bucket (1,340 names, the largest
  remaining) using SEC's already-fetched `tickers` field to break ties like `CLR`/Continental
  Resources and `CPE`/Callon Petroleum — plus, separately, tightening the `GROUP`-as-suffix
  over-normalization this phase found but didn't touch. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-11: SIC/sector classification, Phase 11 (Tier E 1-word query floor). User: "what else
  is next," then "do a" (run the full live quantification and build it) after being shown the
  trade-off against the project's own prior `CFLT` 2-word-floor decision. Root cause: a name
  that's already exactly 2 words after descriptor-stripping (`"HOLOGIC INC"`, `"ZENDESK INC"`,
  `"ANAPLAN INC"`) could never truncate further under the old floor, so if the full query didn't
  literally prefix-match the registrant's real punctuation, the name got zero EDGAR candidates at
  every query tried — 527 names / 1,800 eras / 58.7M trade rows, the single largest remaining
  bucket. Live-verified the risk the old floor was guarding against no longer applies before
  changing anything: querying `HOLOGIC`/`ZENDESK`/`ANAPLAN` (1 word) each found real single
  candidates — `ZENDESK`/`ANAPLAN` validated correctly, and `HOLOGIC` surfaced an unrelated
  blank-SIC limited partnership that the existing SIC-must-exist guard *correctly rejected* rather
  than accepted, proving the guard (not the word count) was always what made a broad query
  trustworthy. Re-checked the actual `CONFLUENT` precedent live too: it now returns 9 candidates,
  over `MAX_CANDIDATES_TO_VALIDATE`, so `CFLT` still correctly stays unresolved via the existing
  count guard — the 1-word floor doesn't reopen that case.
  `MIN_QUERY_WORDS` dropped from 2 to 1 in `edgar_company_search_match.py`; no other logic
  changed. 3 new tests (two-word-to-one-word truncation, the blank-SIC guard still holding at the
  new floor, the over-candidate-cap guard still holding); 1 existing test's call-count assertion
  updated to match the now-correct extra query attempt. 488 tests pass (was 485); ruff/bandit
  clean.
  Real run (`build_edgar_company_search_matches.py`, full recomputation, 4,342 names): genuinely
  rate-limited this time since most of the new 1-word queries were never cached — 54m20s
  wall-clock. `no_candidates` 527 -> **54**; `no_validated_match` 1,265 -> **501**;
  `ambiguous_candidates` 263 -> **1,348** (the anticipated trade-off — a 1-word query is more
  permissive, so many previously-zero-candidate names landed in genuine multi-candidate ambiguity
  rather than a match, correctly not guessed between); `matched` 2,287 -> **2,438**. Reconciled:
  distinct CIKs resolved 8,351 -> **8,455**; manual-research worklist 12,007 -> **11,817 eras**,
  210.3M -> **187.7M trade rows**. Spot-checked newly-resolved matches (`ZEN`->Zendesk CIK
  1463172 SIC 7374, `PLAN`->Anaplan CIK 1540755 SIC 7372) — correct; confirmed `HOLX` (Hologic)
  stayed unresolved exactly as predicted by the blank-SIC guard, not a regression. Combined across
  all three phases in this session: worklist 13,131 -> **11,817 eras** (-10.0%), 264.1M ->
  **187.7M trade rows** (-28.9%). `[CA][IV][REH][CDiP][KBT]`

- 2026-08-11: SIC/sector classification, Phase 10 (Tier E `formerNames` validation). User: "what
  else is next." Checked the freshly-shrunk worklist's new top rows and found EDGAR search was
  finding the right single-candidate CIK for many big names (`COG`, `ETRN`, `CPE`, `FRC`, `HZNP`,
  `PE`, `ABC`, `CDAY`, `RAD`, ...) but rejecting the match, because `_names_match` only checked a
  candidate's *current* SEC registrant name — and every one of these companies renamed or merged
  since the era's ticker was active (`"CABOT OIL & GAS CORP"` finds CIK 858470 immediately, but
  that CIK is now `"Coterra Energy Inc."` post-2021-merger). SEC's submissions payload — already
  fetched for every validated candidate — carries a `formerNames` array (exact historical name +
  date range) that was sitting completely unused next to `sic`/`name`. Quantified first, entirely
  from the existing SQLite request cache (zero network calls): 515 unique names / 724 eras / 46.0M
  trade rows recoverable, 9 names correctly staying ambiguous (genuine historical-name collisions
  across two real registrants). Spot-checked the riskiest case by hand before building — `RITE AID
  CORP` -> CIK 84129, confirmed via its own `formerNames` to have held that exact name 1994-2024
  before its post-bankruptcy reorg to `NEW RITE AID, LLC`, SIC 5912 (Retail-Drug Stores) — and
  confirmed this isn't a repeat of the "180 Life Sciences Corp" false lead this module's docstring
  already warns about (that CIK's own formerNames history shows it genuinely was 180 Life Sciences
  2019-2025 before two later renames).
  Added `former_names` to `sec_sic_client.fetch_sic`'s result (extracted from the same payload,
  ignored by every existing caller since polars' schema-constrained `DataFrame` construction drops
  unknown keys); `edgar_company_search_match._validate_candidates` now checks a candidate's
  `formerNames` whenever its current name doesn't match, still gated by the existing
  SIC-must-exist guard and the existing 2-validated-matches-is-ambiguous rule — no new trust
  assumption, same authority as the field already used. 5 new tests (formerNames extraction,
  match-via-former-name, the SIC guard still applying to a former-name match, two-CIK ambiguity
  still holding); 485 tests pass (was 480); ruff/bandit clean.
  Real run (`build_edgar_company_search_matches.py` + `build_era_sector_enriched.py`, full
  recomputation over 4,342 names): only **21 new network requests** (8,330 cache hits) — matched
  1,844/4,584 (40.2%) -> **2,287/4,342 (52.7%)**. Reconciled: distinct CIKs resolved 8,276 (after
  Phase 9's Tier D fix) -> **8,351**; manual-research worklist 12,691 -> **12,007 eras**, 245.8M ->
  **210.3M trade rows**. Spot-checked several newly-resolved matches against known real corporate
  history (`COG`->Coterra Energy SIC 1311, `ETRN`->SIC 4922 Natural Gas Transmission, `ABC`->SIC
  5122 Wholesale-Drugs, `RAD`->SIC 5912 Retail-Drug Stores) — all correct. `HZNP`/`CPE`/`FRC`
  remain unresolved (candidates found, but none — current or former name — validate; a different,
  smaller gap than this phase targeted). Combined with Phase 9, today's session: worklist
  13,131 -> **12,007 eras** (-8.6%), 264.1M -> **210.3M trade rows** (-20.3%).
  `[CA][IV][REH][CDiP][KBT]`

- 2026-08-11: SIC/sector classification, Phase 9 (Tier D prefix-match fallback). User: "what
  else is next." Cross-checked the worklist's still-unresolved googleable-issuer names against
  SEC's current-listing file by ticker: 253 names whose ticker is currently SEC-listed under a
  name Tier D's exact match was too strict to see. Two generalizable patterns, not one-offs: (1)
  OpenFIGI truncates its `name` field to a hard 28-character ceiling, sometimes mid-word
  (`"ALPHA METALLURGICAL RESOURCE"` for `"...Resources, Inc."`); (2) Bloomberg abbreviations left
  un-expanded after normalization (`HLDGS` vs `HOLDINGS`, `INTL` vs `INTERNATIONAL`). Quantified
  first: 93 names/9.2M rows truncated, 127 names/11.4M rows abbreviation-shaped (220/~21M total),
  against 50 names correctly left alone as genuine ticker-reuse cases (`IAC`→People Inc,
  `USEG`→Big Sky Industrial) — confirming the pattern is real, not a wrong-company shortcut.
  Added a third fallback pass to `utils.sec_name_cik_lookup.match_by_name`: an unambiguous
  word-boundary prefix match (≥2-token floor, exactly one distinct CIK across all candidates or
  no match). Building the mid-word-truncation case surfaced a real false-positive risk before
  shipping — SPAC sequel numbering (`"...Corp II"` vs `"...Corp III"`) is a genuinely different
  company, yet `"II"` string-prefixes `"III"` — caught via the real `TMTSW`/`TVACU` worklist rows
  and fixed with a Roman-numeral-remainder guard. 5 new tests; 480 pass (was 475); ruff/bandit
  clean. Real run (`--skip-fetch`, zero network calls): distinct CIKs resolved 8,201 -> **8,276**;
  manual-research worklist 13,131 -> **12,691 eras**, 264M -> **245.8M trade rows**. Spot-checked
  several newly-resolved matches (`HTZ`, `AMR`, `CCC`, `JHX`) against known real companies — all
  correct. `SG`/`ALIT`'s `" - CLASS A"` space-before-hyphen variant stays unresolved, out of this
  phase's scope. SIC/sector coverage for the newly-resolved CIKs awaits a follow-up live SIC-fetch
  pass. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-06: SIC/sector classification, Phase 8 correction. User checked
  `reports/sector-research-worklist/sector_research_worklist.csv` directly and found `GPS` and
  `CFLT` both still at the top, contradicting the Phase 8 entry's claim that only `GPS` and `HOLX`
  remained unresolved. Verified against the real data: the claim was wrong — `CFLT` was never
  resolved, and got dropped from the "remains unresolved" list by a counting error while writing
  that summary (6 of 9 originally-named top-10 tickers actually resolved, not 7). `CFLT`'s
  unresolved status is itself correct behavior, not a gap: the Phase 7 SIC guard refuses it,
  because the only single-candidate match reachable within Tier E's 2-word truncation floor for
  `"CONFLUENT INC-CLASS A"` is an unrelated blank-SIC shell (CIK 1171179), not the real Confluent,
  Inc. (CIK 1699838) — whose exact registered name only becomes reachable by truncating to the
  single word `"CONFLUENT"`, below the floor this design deliberately holds to limit
  over-broad-query collision risk. No code changed; `docs/ARCHITECTURE.md`'s Phase 8 entry
  corrected to list all 3 genuinely-unresolved names (`GPS`, `HOLX`, `CFLT`) instead of 2.
  `[KBT][CDiP]`

- 2026-08-06: SIC/sector classification, Phase 8 (descriptor-pattern gap). User: "what else is
  next." Rather than manufacture busywork, checked the refreshed worklist's own top rows and found
  a visible, concrete pattern: `EVERPURE INC-A` (`PSTG`), `C3.AI INC-A`, `ROYALTY PHARMA PLC- CL
  A`, `MOBILEYE GLOBAL INC-A` all carry a share-class suffix format
  `sec_name_cik_lookup.DESCRIPTOR_PATTERNS` didn't cover — a bare trailing `-A`/`-B` letter with no
  `CL`/`CLASS` word, and a `"- CL A"` spacing variant. Quantified before proposing: 599 rows (29.9M
  trade rows) bare-letter, 23 rows (4.1M trade rows) spacing variant — comparable to the Phase 4
  descriptor fix's 299-row recovery. Presented two honest options (fix this small pattern, or call
  it here and hand off the worklist); user: "do 1." Added both patterns, with the bare-letter one
  requiring exactly one trailing character so it can't eat a genuine two-letter word ending like
  `-CO` (tested explicitly). Improves both Tier D and Tier E, since both share
  `strip_security_descriptors` — `PSTG` resolved immediately through Tier D alone (no live search
  needed): its real current name, "Everpure, Inc.", was already in SEC's current-listings file,
  the `-A` suffix was the only blocker. 475 tests pass (was 470); ruff and bandit clean. Real run:
  Tier D directly resolved 24 more CIKs / 106 more eras with zero network calls; a fresh Tier E
  pass (4,584 names, mostly cache hits) then rose 1,680/4,627 (36.3%) -> **1,844/4,584 matched
  (40.2%)**. Reconciled: distinct CIKs resolved 8,085 -> **8,201**; eras with real SIC+sector
  11,150 (30.2%) -> **11,466 (31.1%)**; manual-research worklist 13,448 -> **13,131 eras**, 292M
  -> **264M trade rows**; top-500 volume concentration 83.0% -> **83.4%**. `GPS` and `HOLX` remain
  the only unresolved names among the original spot-checked top-10 rows, both confirmed to be
  genuinely different failure modes (no name anywhere in the pipeline; a real EDGAR search miss)
  rather than another descriptor-pattern gap. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-06: SIC/sector classification, Phase 7 (Tier E recall fix). User asked directly: "does
  that mean the CSV top 10 are resolved and kicked from the stack?" Checked the real data: no —
  only `BK` had dropped out; `ATVI`/`X`/`HOLX`/`PSTG`/`FTCH`/`CFLT`/`CORZ`/`PXD` were all still
  hitting the Tier E search-recall gap diagnosed (not fixed) in Phase 6. User: "build that." Built
  the fix: `utils/edgar_company_search_match.py` now tries the full (descriptor-stripped) name
  first, then progressively drops trailing words to a 2-word floor — EDGAR's `browse-edgar` search
  does literal prefix matching, so a raw name with a trailing-word/punctuation mismatch against the
  exact registrant string often returns nothing where a shorter query would — and validates *every*
  candidate a query returns (reusing `fetch_sic`, often a free cache hit) instead of discarding any
  multi-candidate result as ambiguous outright. Also fixed a compounding bug in
  `utils.sec_name_cik_lookup.normalize_name`: SEC's trailing `/XX` jurisdiction tag (`"Core
  Scientific, Inc./tx"`) was blocking the legal-suffix-stripping loop from ever reaching `"INC"` —
  this also fixed Tier D directly for the same names (`CORZ`), since Tier D's current-listing match
  shares the same normalization function. A live smoke test before committing to the full run
  caught a real false-positive the fix itself introduced: searching for the real Confluent, Inc.
  (CIK 1699838, SIC 7372) also surfaced an unrelated same-named shell (`"CONFLUENT INC"`, CIK
  1171179, blank SIC) — a genuine SEC name collision plain normalized-name matching can't tell
  apart. Every confirmed-correct match checked while building this had a real SIC on record; the
  collision didn't — added a free guard (the SIC is already fetched during validation) requiring
  one before accepting any match. A second, unrelated bug surfaced mid-run: rerunning the EDGAR
  search over only the still-unresolved residual pool and overwriting the output file silently
  *dropped* the first run's 969 matches, since `reconcile_cik` rebuilds `cik_source` fresh from
  that file every run — `distinct_ciks_resolved` measurably went *down* (7,546 -> 7,267) after the
  first rerun, caught by comparing against a saved before-snapshot rather than trusting the new
  numbers blindly. Fixed by re-including any name Tier E itself resolved on a prior run in the
  search population, so every run recomputes a complete, self-consistent table rather than an
  eroding partial one. 470 tests pass (was 448 going into Phase 6+7 combined); ruff and bandit
  clean. Real run (full recomputation, 4,627 unique names, mostly cache hits): EDGAR matches rose
  969/4,453 (21.8%) -> **1,680/4,627 (36.3%)**; distinct CIKs resolved 7,546 -> **8,085**; eras
  with real SIC+sector 10,070 (27.3%) -> **11,150 (30.2%)**; manual-research worklist 14,491 ->
  **13,448 eras**, 378M -> **292M trade rows**; top-500 volume concentration 78.7% -> **83.0%**.
  Of the original spot-checked top-10 rows, 7/9 with any name in the pipeline now resolve
  correctly (`ATVI`, `X`, `BK`, `FTCH`, `CORZ`×2, `PXD`); `PSTG` (bare `-A` suffix the descriptor
  patterns don't cover) and `HOLX` (genuine EDGAR search miss) remain diagnosed residuals, and
  `GPS` stays a genuine residual with no name anywhere in the pipeline. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-06: SIC/sector classification, Phase 6 (ticker-rename/continuity detection). User
  spot-checked the worklist's own top rows against real SEC data and found `GPS`, `BK`, and
  `PSTG` are not delisted — they're ticker renames (`GPS`→`GAP`, `BK`→`BNY`, `PSTG`→`P`, all
  confirmed live against SEC's submissions endpoint), and `CORZ`'s tracked end date is a stale
  vendor-window artifact, not a real corporate event (Core Scientific never stopped trading).
  Traced to two general, structural causes rather than four one-off tickers: (1) OpenFIGI's
  ticker-keyed `/v3/mapping` lookup is current-listing-biased — the same blind spot Tier C
  already has, just one layer upstream — so querying a renamed-away ticker string returns zero
  FIGI matches and `identity_issuer` never gets populated in the first place; (2) nothing in the
  pipeline checked whether a resolved CIK's *current* SEC ticker matched the era's own symbol, so
  "genuinely delisted," "renamed," and "still trading, stale end-date" were all indistinguishable.
  Fixed both, zero new network cost either way: `utils/sector_enrichment_inputs.py` gained
  `load_iex_fallback_names`/`apply_iex_fallback_issuer`, backfilling `identity_issuer` from
  `iex_latest_issuer` (`utils/build_iex_entity_enrichment.py`'s already-ingested local snapshot
  data) whenever the identity pillar left it null, never overwriting a real assertion, with a new
  `identity_issuer_from_iex_fallback` flag for provenance. New `utils/ticker_continuity.py` reads
  `tickers`/`exchanges` from the same SEC submissions payload `sec_sic_client.fetch_sic` already
  fetches for SIC (two more fields sitting unused in that response, same pattern as `sic`/
  `sicDescription` before this program started) and derives `continuity_status` per era —
  `terminal`, `still_active_same_symbol`, or `renamed_or_successor`. Extracted
  `utils/sector_enrichment_report.py` from the orchestrator to stay under the file-size gate (was
  362 lines after the new wiring, now 270). 15 new tests (10 for `ticker_continuity`, 4 for the
  IEX fallback, 1 full end-to-end using the real GPS/GAP case) — 463 pass (was 448); ruff clean.
  Real re-run (17 network requests, mostly cache hits since the prior pass already covered most
  CIKs): `sec_name_matched` 1,948 -> **2,015** (BK now resolves and correctly flags
  `renamed_or_successor`); manual-research worklist 14,559 -> **14,491 eras**, 389M -> **378M
  trade rows**; `has_googleable_name` rows 5,155 -> **6,513** (real names now visible for
  CFLT/CORZ/FTCH/PSTG/PXD even though they still lack an automatic CIK). Across the whole
  universe — not just the four tickers that prompted the investigation — `continuity_status`
  classified **740 eras `renamed_or_successor`** and **7,915 `still_active_same_symbol`**, a
  signal that was structurally invisible before. `GPS` itself stays a genuine residual (no name
  anywhere in the pipeline, not OpenFIGI, not IEX). Separately diagnosed but explicitly left
  unfixed (out of this phase's scope): `CFLT`/`CORZ`/`PXD` already have a real `identity_issuer`
  and still fail Tier E, traced to EDGAR's `browse-edgar` search doing literal prefix matching
  against the exact registered name string — the raw name sent as the query often doesn't
  literally prefix-match (confirmed live: a shorter truncated query finds the right CIK
  immediately), and Tier E separately discards any multi-candidate result as `ambiguous` rather
  than validating each candidate. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-06: SIC/sector classification, Phase 5 (EDGAR full-text/company-search CIK tier).
  User's explicit goal: resolve more of the remaining manual-research worklist via EDGAR, subject
  to two hard constraints — respect SEC's rate-limit guidance, and design every network request to
  yield the most information possible. Tier D (Phase 4) only searches SEC's *current* company
  listings, so it structurally cannot resolve a genuinely deregistered/merged/dissolved issuer.
  Built a new Tier E on the same "search then validate, never trust a bare single hit" shape:
  `utils/sec_company_search_client.py` calls EDGAR's classic `cgi-bin/browse-edgar` company-name
  browse search — confirmed live, before writing the parser, that it *does* return
  historical/inactive registrants (a real bankrupt Circuit City Stores CIK) unlike the
  current-listings JSON file. Routed through a new backward-compatible `parse_response`/
  `is_negative` hook pair on `CachedPrimaryClient.get_json()` so this non-JSON atom/XML endpoint
  gets the same cache/retry/rate-limit machinery as every other SEC call, at no cost to existing
  callers. `utils/edgar_company_search_match.py` accepts a match only when exactly one candidate's
  actual registrant name (fetched via the same `fetch_sic` call already used for SIC — usually a
  free cache hit) validates against the query name; live testing surfaced a real single-candidate
  false lead (`180 Life Sciences Corp` → a wrong-company hit) that this correctly rejected instead
  of guessing. `utils/build_edgar_company_search_matches.py` batches the search over every unique
  unresolved issuer name (deduped once, not per era row — the yield-per-request design goal), at
  the same ~3.3 req/sec (well under SEC's guidance). The first live run hit a real SEC `503`
  ~10 minutes in and aborted the *entire* batch, discarding everything already collected — a real
  robustness gap, since `match_issuer_name` wasn't catching `PrimarySourceError` the way the
  sibling `sec_sic_client.fetch_sic` already does. Fixed to match that established pattern (a
  `fetch_error` status per name, batch continues, nothing cached so it retries), added 2 tests
  proving both the search- and validation-request failure paths degrade gracefully, then reran —
  the SQLite cache replayed everything already searched as free hits. Real run over 4,453 unique
  names: `969` matched (21.8%), `245` ambiguous, `463` name-mismatch (rejected), `2,767`
  no-candidates, `9` transient fetch errors (retryable next run). Wired as Tier E in
  `sector_cik_reconcile.py` and `build_era_sector_enriched.py` via `sector_enrichment_inputs.py`'s
  new `load_edgar_matches`. Live re-run (7,529 distinct CIKs, mostly cache hits, zero errors):
  distinct CIKs resolved 6,605 -> **7,529**; eras with real SIC+sector 8,716 -> **10,002** (27.1%
  of the universe); manual-research worklist **15,882 -> 14,559 eras**, 492M -> **389M trade
  rows**; top-500 volume concentration 72.5% -> 78.7%. 448 tests pass (was 436); ruff clean.
  `[CA][IV][REH][CDiP][KBT]`

- 2026-08-05: SIC/sector classification, Phase 4 (name-matching precision pass). Followed up on
  "what else is next" by checking whether the remaining ~6,777-row googleable-name pool (post
  Phase 3) was genuinely unfindable or just a name-matching gap. Found `identity_issuer` often
  carries a trailing Bloomberg/OpenFIGI security-descriptor suffix (`-CW23`, `-ADR`, `W/I`,
  `-CLASS A`, …) that blocks an otherwise-exact match against SEC's current company-name list.
  Added `utils.sec_name_cik_lookup.strip_security_descriptors` as a second, still-exact fallback
  pass in `match_by_name` — recovers 180 additional unique-name matches (299 era rows / 35.8M
  trade rows), zero new ambiguity risk. Also evaluated and explicitly **rejected** a
  token-subset/fuzzy matcher: on real data it matched "1895 Bancorp of Wisconsin" to an unrelated
  company simply named "Bancorp" (a single generic token satisfying a naive subset check) — a real
  wrong-company risk, so it was not built. Live re-run (43 new network requests, zero errors):
  worklist 16,181 -> **15,882 eras**, 528M -> **492M trade rows**, `sic_and_sector` coverage rose
  to 8,716 eras (23.6% of the universe). 410 tests pass (was 399); ruff and bandit clean.
  `[CA][IV][REH][CDiP][KBT]`

- 2026-08-05: SIC/sector classification, Phase 3 (shrinking the manual-research pool). The user
  pushed back on the 29,597-era manual-research worklist ("how am I expected to manually hit 30k
  manual tickers") — rightly: the top of that list was dominated by huge ETFs (IWM, XLF, XLE, GDX,
  HYG, …) that had never gone through OpenFIGI classification at all, and by dead-ticker eras
  (META, ATVI, SMCI, SNOW, …) that already had a googleable OpenFIGI-asserted issuer name but no
  automatic path to a CIK. Both are automatable, not manual work. Landed: (1)
  `utils/build_openfigi_stable_universe.py` derives an OpenFIGI input for the
  `stable_candidate`/`ipo_or_new_listing_candidate` universe (~11,244 eras, never covered because
  the original OpenFIGI pass was scoped to the dead-ticker review cohort only) — run for real,
  92.5% matched, 43.3% are `fund_etf`. (2) `utils/sec_name_cik_lookup.py` matches an era's
  OpenFIGI-asserted issuer name against SEC's already-fetched current company-name list (zero new
  network calls), rejecting ambiguous normalized-name collisions rather than guessing — wired into
  `sector_cik_reconcile.py` as a new Tier D that, unlike Tier C, safely applies to dead-ticker
  classes too (a name persists even after a ticker gets reused, unlike a ticker match). Both feed
  `build_era_sector_enriched.py` via a new `utils/sector_enrichment_inputs.py` module. Re-ran the
  full live pass (6,562 distinct CIKs, ~4 minutes thanks to heavy cache reuse from the Phase 2 run,
  zero errors): eras with a real SIC+sector rose from 6,836 to **8,417** (22.8% of the universe),
  and — the actual goal — the manual-research worklist dropped from **29,597 eras / 1.12B trade
  rows to 16,181 eras / 528M trade rows (a 45% reduction)**, with 11,767 eras now correctly
  excluded as funds/ETFs rather than sitting in the research queue.
  `stable_candidate`'s unresolved count alone fell from 542 to 13. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-05: SIC/sector classification, Phase 2 (live run). `utils/build_era_sector_enriched.py`
  and `utils/build_sector_manual_research_worklist.py` landed and ran for real against SEC's
  submissions endpoint: 6,087 distinct CIKs, rate-limited to ~3.3 req/sec (well under SEC's
  10 req/sec guidance), ~39 minutes wall-clock, **zero errors** (no `fetch_error`, no unexpected
  404s — every resolved CIK was a real SEC filer). Results landed almost exactly on the Phase 1
  estimates: 6,836 eras (18.5% of the full 36,866-era universe) got a real SIC + sector, a 93.9%
  SIC fill rate on resolved CIKs (369 blank, virtually all funds/ETF trusts — e.g. SPDR Dow Jones
  Industrial Average, a shell fund CIK — exactly the "no SIC on record" case the plan predicted).
  Coverage is exactly as structurally uneven as the CIK-provenance analysis anticipated:
  `stable_candidate` 2,134/2,872 (74%), `ipo_or_new_listing_candidate` 3,934/8,372 (47%), the four
  dead-ticker review classes combined only 768/25,622 (3%) —
  `intermittent_full_window_candidate` got zero automatic coverage at all. The 29,597-era, 1.12B
  trade-row remainder is the manual-research worklist (`reports/sector-research-worklist/`);
  15,762 of those rows already carry a googleable OpenFIGI-asserted issuer name (e.g. META, FB,
  ATVI, SNOW, SMCI) even without a resolved CIK — confirming those are FIGI-tier identity facts,
  not gaps in the reconciliation logic. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-05: SIC/sector classification, Phase 1 (offline, no live SEC calls yet). A repo-review
  concluded this codebase is a reasonable DIY security-master foundation but has no sector/industry
  classification anywhere — OpenFIGI's `marketSector` is a coarse asset-class bucket (`"Equity"` or
  `null`), not an industry. Two Explore passes confirmed SIC/`sicDescription` live only in SEC's
  `data.sec.gov/submissions/CIK*.json` endpoint (already called in 3 places, never read), and found
  CIK coverage fragmented across three unreconciled sources with very different confidence: 454
  `verified`/`sec_date_scoped_display_names` facts (a real date-scoped CIK), 364
  `legacy_historical_override` facts (CIK recoverable from the source URL for 361), and a
  current-listing-biased ticker match (`sec_cik`, 81% coverage on `stable_candidate`, 49% on
  `ipo_or_new_listing_candidate`, as low as 1.8% on `delisted_or_acquired_candidate` — confirming
  dead-ticker sector coverage really is structurally harder, per the user's own instinct). Landed
  this pass: `utils/sic_division_table.py` (standard 10-division SIC rollup),
  `utils/sector_cik_reconcile.py` (tiered CIK reconciliation, strictly scoped so a current-ticker
  match never applies to a dead-ticker review class — verified against real data: reconciled counts
  match the independently-confirmed ground truth almost exactly, ~6,087 distinct CIKs identified),
  and `utils/sec_sic_client.py` (SIC fetcher reusing `resolution_v2_network.CachedPrimaryClient`'s
  cache/retry machinery with the exact cache-key shape `resolution_v2_sec.py` already uses, so
  fetches piggyback on the live resolver's existing cache for free). Next: the live fetch+join
  orchestration tool, the no-CIK manual-research worklist, and a real (not estimated) coverage
  report — deferred to a follow-up PR since it means ~6,000+ live requests against a government
  API. `[CA][IV][REH][CDiP][KBT]`

- 2026-08-05: `utils/` reorganized after a full codebase review flagged two stale report artifacts and 127 flat files in `utils/`. (1) `reports/dead-ticker-review/dead_ticker_review_queue.parquet` and `unresolved_priority_queue.parquet` were regenerated with a new shared `utils/canonical_identity_join.py` join onto the confidence-tiered `data/resolution/identity_facts.jsonl`/`event_facts.jsonl` store — they previously only saw the pre-OpenFIGI legacy CSV (364 verified, 1,686-era regex fund count) even though the canonical store had moved to 818/1,580/14,179 tiered facts and a 7,395-era authoritative fund census 10 days earlier; the priority queue's true unresolved count drops from a legacy-only 12,431 to 4,929 once eras a usable canonical fact already covers are excluded. (2) The narrative-first SEC resolution lane (46 files: `run_sec_high_impact_identity_resolution_iterations.py`, `run_sec_lifecycle_resolution_iterations.py`, `run_sec_terminal_resolution_iterations.py`, and every EDGAR/SEC evidence, workplan, and text-scoring module behind them) moved to `utils/legacy/` — decided by parsing real import/subprocess references rather than by name, since it turned out to still be fully wired to a documented, runnable entrypoint and is the source of all 818 SEC-grade `verified` facts; it moved because `docs/EVENT_CATALOG_RESOLUTION_PLAN.md` measured it at a ~1% yield plateau, not because it stopped working. One real cross-boundary dependency surfaced (`derivative_identity_resolution.py`, a live V3 evidence gate, needed two generic date/text helpers from the archived `sec_terminal_text_evidence.py`) and was fixed by moving those two functions into the shared `sec_identity_evidence.py` module. (3) A new `utils/build_truly_missing_eras_by_year.py` answers "how many eras have zero usable canonical identity, by year": 10,368 eras / 244.1M trade rows, with the earliest-year bucket flagged as left-censored at the 2016-12-12 TOPS capture floor. All 301 tests and ruff pass; see `utils/legacy/README.md` for the full inventory. `[CA][CSD][REH][CDiP][KBT]`

- 2026-08-04: Era×identity enriched product built (`utils/build_era_identity_enriched.py` → `reports/era-identity/eras_identity_enriched.parquet`): all 36,866 eras joined to best-tier identity (verified > corroborated > openfigi_asserted, contested excluded from the default-usable view) plus best event fact and derived era spans. Coverage: 15,254 default-usable eras / 790.2M trade rows; 20,289 eras identity-less (mostly stable candidates outside the resolution cohort). First payoff stat: fund_etf median era span is **34 days** — the launch→spin-down cohort is now directly queryable. `[CA][KBT][CDiP]`

- 2026-08-04: Tiered apply of the OpenFIGI stage into the canonical store — zero manual review, per user decision. Ground-truth measurement vs the 818 SEC-verified identities: 68% exact / 74% high-similarity entity agreement, ~8-12% hard errors concentrated in reused tickers. Canonical `identity_facts.jsonl` is now confidence-tiered: `verified` 818 (SEC-grade, untouched), `corroborated` 1,580 (OpenFIGI + Form 25/SEC-name agreement), `openfigi_asserted` 14,179 (1,323 conflicted facts carry `contested` for exclusion from default joins); 363 Form 25 `event_candidate` facts applied alongside. Apply tool `utils/apply_openfigi_identity_candidates.py` is dry-run-first, idempotent, and skips eras with verified identity; nothing was overwritten. Downstream queries must select their assurance tier explicitly. `[CA][IV][REH][SFT][KBT][CDiP]`

- 2026-08-04: OpenFIGI pillar delivered end-to-end. Phase 1: keyed `/v3/mapping` with `includeUnlistedEquities=true` (the recall unlock: 0%→80.9% on verified-dead ground truth) matched `8,948/13,256` symbols (67.5%), and the authoritative census found `fund_etf` = `7,521` eras / 29.35% — the regex undercounted funds ~4.5×, so the ETF gap is real, not bias. Phase 2: Form 25 event catalog (9,691 filings 2016–2026; NasdaqTrader delisted discontinued and the Wikipedia defunct list nonexistent — both dropped) yielded 2,285 ticker-bound events but only `356/19,205` eras (1.9%) — most unresolved eras are intermittent/thin symbols, not delistings. Phase 3: staged `15,759` OpenFIGI identity candidates (89.15% of the `17,677`-era baseline; 7,395 fund_etf) + 363 Form 25 event candidates at `data/resolution/staged/7a35d98a404e95c5/`, all `verification_state=candidate` with corroboration tiers (1,580 high-confidence form25/sec-name agrees; 12,856 uncorroborated — reuse-misassignment risk lives there, e.g. AAAP). Nothing applied; review starts with the high-confidence tier. Plan/status: `docs/EVENT_CATALOG_RESOLUTION_PLAN.md`. `[CA][IV][REH][SFT][KBT][CDiP]`

- 2026-08-03: Methodology assessment concluded the narrative-first SEC lanes have plateaued at ~1% yield (terminal `7,500`→`37`, lifecycle `861`→`9`, identity `1,992`→`552`); the evidence-first direction (enumerate authoritative event catalogs, then join to eras) was approved, then revised when the user obtained an OpenFIGI API key: dead FIGIs remain queryable, so a keyed full-universe enrichment now front-runs the catalog work as the identity/classification pillar. Approved scope is Step 0 (docs, done) + Phase 1 (`utils/build_openfigi_symbol_identities.py`, keyed batch-100 enrichment over all ~13k cohort symbols retaining ALL FIGI matches, plus an authoritative instrument reclassification and the first real fund/ETF census vs the regex `6.8%` estimate). The catalog probe/build phases wait on the Phase 1 review gate. Full plan: `docs/EVENT_CATALOG_RESOLUTION_PLAN.md`. Note: an earlier catalog-only probe agent died mid-run on a provider quota error; its claimed "probe running" status was stale and no probe output exists. `[CA][KBT][AS][CDiP]`

- V2 CIK event dry run attempted `360` eras with `2,000` SEC requests and proposed `26` verified events / `15,265,414` trade rows, but review found generic-effective, prospective-delisting, and ticker-change fallthrough false positives. Resolver `evidence_delta_v3` now separates terminal and symbol-change vocabularies, requires same-clause old/new symbols plus date, blocks unconfirmed symbol changes from falling through as delistings, and invalidates the unsafe dry-run stage. A conservative stored-snippet audit retained 20 terminal candidates and held/reclassified 6; all require a corrected V3 dry run before apply. `[CA][IV][REH][RM][PA][KBT][CDiP]`

- Phase 2 event-only resolution queue is ready: fixed completed-stage reuse by keying stages to the migrated fact-ID snapshot, then rebuilt a fresh local stage with `5,507` attempts. Added a canonical identity-verified/event-unproven queue with identity/date join gates: `676` action-required eras / `267,638,847` trade rows (`237` event candidates, `439` unresolved); the generated top-200 terminal window covers `229,208,099` trade rows. The first CIK network dry run completed but was rejected for apply; V3 rerun and canonical supersession review remain open. `[CA][IV][REH][RM][PA][SFT][KBT][CDiP]`

- Pareto top-2200 local continuation refreshed after the quarantined-era V2 remap: the current identity-unresolved population is `17,677` eras / `560,616,350` trade rows, with volume cutoffs now at rank `260` (50%), `778` (80%), `1,731` (95%), and `3,309` (99%). Rebuilt the unresolved priority queue with `--top-n 2500`; the top 2,500 covers `97.98%` of unresolved volume and is entirely probable operating-company SEC/event route. Regenerated resolution lanes/workplan: `12,431` unresolved priority eras, `1,991` high-impact operating rows / `536,972,325` trade rows, `5,625` operating lifecycle rows / `10,651,120`, `3,879` derivative/parent rows / `6,695,683`, `135` low-materiality dry-run ledger candidates / `811`, and `801` manual holds / `5,013,608`. `[CA][REH][PA][KBT][CDiP]`

- External code review triaged and mostly confirmed. Fixed: the dead `CANCEL`/
  `CORRECTION` sale-condition regex (unmatchable; cancels are Trade Break messages the
  CSV path does not carry) replaced with documented default odd-lot exclusion
  (`FILTER_VERSION` `v2`); `--limit-days` now counts trading days and corrupt days no
  longer abort runs; dedupe tightened to `trade_id`+`symbol` with a collision metric;
  sessions gained an `unknown` bucket. Verified non-issues: `data/manual_overrides` is
  2.6MB tracked (the ~33MB is `data/resolution` canonical facts, intentional); `numpy`
  was already removed from the README. TradeBreak measurement: 160 breaks across 6.6B
  trades (0.000002%, 55 days) — negligible historical impact; daily bars now apply breaks
  via `trade_id` anti-join with a summary metric, so a bars rebuild is optional rather
  than required. Outstanding: packaging rename (`src` → `iexscoper`) and moving
  `requests` out of the dev group remain open. [REH][KBT]

- Session-validity quarantine implemented and applied (`utils/session_validity.py`,
  `utils/build_session_validity.py`, `--quarantine-path` wiring in the stability audit and
  daily bars). The manifest quarantines exactly the 16 Saturday test sessions with zero
  weekday false positives; the trade-share floor had to be 5%, not 50% — a 50% floor
  would have quarantined 44 real high-volume weekdays from the 2021-10..2022-02 feed-mix
  period. [REH][IV][PA]
- Quarantined era rebuild (`reports/symbol-stability-quarantined/`) measured: 37,428 to
  36,866 eras (-562 weekend micro-eras; 785 single-day weekend micro-eras existed),
  `intermittent_or_reused` 15,241 to 14,664 (-3.8%), the `20170826` end-date spike
  1,113 to 0, 9 ghost-only symbols removed, 22 splices healed. Correction to the earlier
  estimate: most 2017 fragmentation is genuine thin-symbol sparsity (the `20170925`
  1,075-end spike persists), so weekend artifacts explain ~4% of the intermittent class,
  not more. Downstream products (daily bars, V2 cohort) still point at the pre-quarantine
  era table; swapping the canonical path is the next decision. [REH][PA][KBT]

- Weekend test-session artifact discovered: 16 Saturday-dated TOPS captures (IEX weekend
  sessions — e.g. `20170826` holds 8,445 OperationalHalts and only 165 TradeReports vs
  657K trades on a normal Friday) shatter symbol continuity. 1,504 cohort eras (5.7%)
  touch a weekend day (1,397 classified intermittent/reused), 786 eras exist entirely
  inside weekend sessions, and 790 symbols carry both a weekend-end and weekend-start
  era (clean splice candidates). Era-end clustering is therefore dominated by data seams
  (2017 months: 1,000-1,800 ends/month) over real terminal events (late-2022/2023 M&A and
  SPAC waves: 200-350 ends/month). Fix path: session-validity quarantine in ingest
  validation, rebuild symbol eras without the 16 files, then reclassify. [REH][PA][KBT]

- Cohort volume-concentration analysis (confirmed with user): the `26,184`-era cohort holds
  `1,034,586,884` trade rows, but the top 100 eras cover 27%, top 500 cover 62%, and top
  `2,000` cover 92.7%. Restricting to the `25,364` eras without verified identity
  (`700,862,222` trade rows), 50% of remaining volume sits in the top 252 eras, 80% at 867,
  95% at `2,211`, and 99% at `4,484`. The tail is extreme: 54% of unknown eras have ≤100
  lifetime trades (0.027% of volume) and 73% have ≤1,000 (0.295%). [PA][KBT]
- Instrument mix of unknown eras: probable operating companies are 64% of eras but 95.4% of
  volume; preferreds/warrants/units/rights together are ~27% of eras yet under 1% of volume;
  funds/trusts are 6.8% of eras and 3.2% of volume. Volume-weighted resolution is therefore
  almost entirely an operating-company problem; derivative/fund evidence paths serve the
  long tail only. [KBT][CA]
- Top-of-cohort manual classification review (agreed with user): at least 8 of the top 40
  unknown eras are symbol renames whose era boundaries align to the day — FB→META
  (2022-06-08/09), SQ→XYZ (2025-01-17), GOLD→B (2025-05-08), SWN→EXE (2024-10-01),
  GPS→GAP (2024-08-21), FISV→FI (2023-06-06), NYCB→FLG (2024-10-25), COG→CTRA
  (2021-10-04). VXX is misclassified as `probable_operating_company` (it is an ETN).
  Agreed next build: a symbol-change candidate lane pairing an era `last_day` with another
  era `first_day` within a few trading days, emitting review-only candidates; and a manual
  review sprint over the top 200 unknown eras by volume. [KBT][CA][CDiP]

- Historical pre-quarantine V2 milestone: the original `26,184`-era cohort applied 32
  verified facts before the SEC EFTS circuit breaker. These counts were superseded by the
  quarantined-era remigration and are retained here only as run history. [CA][REH][PA][KBT]
- Independent queues generated for identity, event, instrument, observation, and research
  action. All `5,694` legacy closures remain outside research action without being promoted to
  identity or event proof; local reconciliation formed 128 derivative parent/action groups and
  found no continuity propagation that passed every entity/instrument conflict gate. [KBT][REH]
- Dry-run/apply idempotency verified after the live stage: unchanged rerun issued zero new
  requests, request metrics stayed at `7,033` source requests and `10` cache hits, and
  decision facts remained one row per review era. Full repository tests pass. [RM][REH][CDiP]
- The former V2 checkpoint is historical. Current work resumes only through a V3 dry run;
  V2 attempts remain versioned in the registry and cannot suppress V3 evidence evaluation.
  [SFT][KBT][RM]

- Standalone IEX parser parity benchmark harness landed under `utils/`. `[CA][PA]`
- Canonical TOPS normalization adapters landed for the benchmark path. `[CA][REH]`
- Focused benchmark, HIST index, and bounded backfill tests landed under `tests/`. `[REH]`
- Backfill utility landed for `hq-4/IEXTools` with bounded local staging and NAS publish. `[RM][PA]`
- Backfill is currently blocked by parser robustness on some 2025-2026 days. `[REH]`
- Runner hardening landed: isolated unknown message types are now quarantined and logged to a JSONL artifact instead of failing the day immediately. `[REH]`
- Threshold-based failure now exists for total and consecutive unknown messages so extreme framing loss still fails, but defaults now tolerate forward-compatible unknown-message bursts observed on `20250815`. `[REH][PA]`
- Added `utils/legacy/debug_iextools_day.py` to isolate IEXTools parse-only and normalize-only behavior without Parquet writes. `[CA][REH][PA]`
- `20250815` RCA: the early exit `139` did not reproduce in parse-only or normalize-only probes over the first 10M messages; the deterministic blocker is an unknown `0x28` message burst near 107M messages. The current IEX TOPS spec allows unknown future message types, so the runner now quarantines larger bursts before aborting. `[REH][KBT]`
- Backfill summary/report generation landed with retry-only failed-day lists, remaining missing-day lists, and resume checkpoint derivation from NAS state plus results logs. `[CDiP][REH]`
- Backfill recovery hardening landed: gzip corruption and negative-length parser failures now trigger fresh HIST URL refresh, scratch cleanup, redownload, and bounded per-day retry instead of immediate terminal failure. `[REH][RM][PA]`
- Backfill scratch hardening landed: default unknown-message thresholds now match the runner, native crashes are retryable, terminal failures delete failed gz/parquet scratch by default, and `--keep-failed-gz` preserves a raw input only when needed for RCA. `[REH][RM][PA]`
- Runner transport hardening landed: pcap-ng captures are converted to UDP payload streams before `IEXTools` parsing, and zero-message parses now fail explicitly instead of surfacing later as missing parquet publish errors. `[CA][REH][PA]`
- 2017 TOPS1.5 RCA and fix landed: classic pcap inputs are now extracted to UDP payload streams, TOPS 1.5 protocol IDs are recognized, and the runner passes `tops_version=1.5` to `IEXTools`. Validation on `20170103` succeeded with `19,736,841` parsed messages. `[REH][CA][PA][KBT]`
- Backfill HIST refresh race fix landed: worker threads now serialize shared HIST index refreshes and `download_hist_index()` writes through an atomic temp-file replace, preventing partial JSON reads during parallel runs. `[REH][RM][PA]`
- TOPS1.5 price-column regression found and fixed in the hq-4 adapter: IEXTools stores derived float prices in slot attributes that `dataclasses.asdict()` does not include, so published hq-4 backfill outputs before this fix may have null `price`, `bid_price`, and `ask_price` despite populated integer price fields. Streaming repair utility landed to audit first and then fill lossless float prices from integer columns with atomic Parquet replacement. `[REH][KBT][PA][RM]`
- 2017 repair sequence: audit published days with `utils/repair_iextools_price_columns.py`, apply repairs only to affected days, then resume/retry missing or `parser_short_buffer` days with the patched adapter. `[REH][RM][CDiP]`
- 2017 post-run RCA landed: November failures came from selecting tiny TOPS 1.5 HIST placeholders over full TOPS 1.6 files, and early-year short-buffer failures came from hq-4's incorrect TOPS 1.5 TradeBreak body format. Record selection now prefers the largest TOPS file, the runner patches hq-4 TradeBreak decoding, and real validation on `20170109` succeeded with `16,704,442` messages. `[REH][KBT][PA]`
- 2017 final retry RCA landed: the remaining 7 TOPS1.6 days failed on hq-4 `TradeBreak.sale_flags` arriving as a one-byte `bytes` value while the canonical decoder expected an integer. `decode_sale_flags()` now accepts both bytes and ints. `[REH][KBT]`
- Symbol stability audit utility landed for TOPS ticker-era continuity checks before long-window analysis. It flags stable, IPO/new-listing, delisted/acquired, intermittent/reused, and partial-window candidates from observed Parquet symbols. `[CA][KBT][PA]`
- Symbol-era derivation landed: the audit now emits `symbol_eras.parquet`, `symbol_eras.csv`, and `symbol_eras.jsonl`, splitting tickers at major observation gaps and marking whether an era is a `long_window_candidate` or point-in-time only. `[CA][KBT][PA]`
- Daily confirmed-trade OHLCV materializer landed for backtest prep. It excludes QuoteUpdate data and writes resumable day-partitioned bars keyed by `symbol_era_id`. `[CA][PA][RM]`
- Daily trade-bar RCA found seven latent main-Parquet column-page corruptions that require raw-day regeneration before full coverage: `20190916`, `20211228`, `20220209`, `20230123`, `20230127`, `20230620`, and `20230719`. The Parquet footers and symbol/timestamp/type columns are readable, but one row group per file fails on `price_int` or `size` with `Invalid thrift: protocol error`, explaining why symbol-stability scans passed while OHLCV failed. `[REH][KBT]`
- Daily trade-bar coverage is complete across `2409` files after regenerating the seven latent corrupt main Parquet days. `20190330` is intentionally empty because it was a Saturday source file with no confirmed `TradeReport` rows. `[REH][PA][KBT]`
- Stable long-window ticker-era universe landed under `reports/stable-long-window-universe`, with liquidity tiers derived from confirmed-trade median daily notional and trade-day coverage. `[CA][PA][KBT]`
- Stable long-window quality report landed under `reports/stable-long-window-quality`, flagging invalid OHLC rows, nonpositive/near-zero prices, extreme raw returns, and volume/notional outliers before backtest panel construction. `[REH][PA][KBT]`
- IEX entity snapshot diff and enrichment landed for local `iex_entities/` snapshots. The enrichment writes current listing evidence under `reports/iex-entity-enrichment` and joins it to both `symbol_eras.parquet` and the stable long-window universe without committing the raw snapshot directory. `[CA][KBT][PA][CDiP]`
- Stable daily research panel landed at `/media/tn/pq/derived/stable-daily-panel/stable_daily_panel.parquet`, joining confirmed-trade OHLCV, stable universe metadata, IEX entity evidence, and quality flags for `6,656,475` rows across `2,874` stable ticker eras. `[CA][PA][KBT][CDiP]`
- Stable daily panel validation landed under `reports/stable-daily-panel-validation` and passed with `0` hard failures across duplicate keys, critical nulls, OHLC invariants, nonpositive metrics, timestamp order, and quality-flag source parity. `[REH][PA][CDiP]`
- Stable returns table landed at `/media/tn/pq/derived/stable-returns/stable_returns.parquet`, deriving raw/log close-to-close returns and clean-return flags from the validated panel. It contains `6,653,601` non-null return observations and `6,561,194` clean return observations. `[CA][PA][KBT][CDiP]`
- `potential_corporate_action` now flags large raw close-to-close jumps in the stable returns table for later split/dividend triage instead of filtering them out. `[REH][PA][CDiP]`
- Symbol stability audit now skips and reports unreadable Parquet days instead of aborting the full report. `[REH][KBT]`
- OpenFIGI enrichment utility landed for cached, rate-limited current FIGI metadata triage over symbol-stability rows. It flags multiple matches, ticker mismatches, unresolved symbols, and stable candidates with matches, but does not replace a licensed historical security master. `[CA][IV][REH][SFT][PA]`
- SEC no-key ticker/CIK enrichment landed under `reports/sec-ticker-cik`, matching `10,262` symbol eras to a single current CIK, flagging `3` multiple current matches, and leaving `27,163` unmatched/current-unproven. `[CA][REH][PA][KBT][CDiP]`
- Dead ticker review queue landed under `reports/dead-ticker-review`, separating `26,184` non-stable ticker eras by current SEC/IEX evidence, manual historical overrides, and heuristic instrument hints. `18,430` eras remain `historical_identity_unresolved`; `X#001`, `TWTR#001`, `ATVI#001`, and `PXD#001` are seeded as `manual_verified_historical_identity`. `[CA][REH][PA][KBT][CDiP]`
- Dead ticker instrument heuristic audit added under `reports/dead-ticker-review`, preserving legacy `instrument_hint` while adding `instrument_type` and `instrument_reason` for preferreds, warrants, units, rights, share classes, funds/trusts, operating companies, and ambiguous patterns. `[CA][CSD][PA][KBT][CDiP]`
- Dead ticker research routing added to review, priority, and manual template outputs via `research_route`, `recommended_evidence`, and `routing_reason`, separating operating-company SEC/event work from fund/trust, preferred, security-action, share-class, and manual syntax routes. `[CA][CSD][KBT][CDiP]`
- Unresolved dead ticker priority queue landed under `reports/dead-ticker-review`, ranking `18,430` unresolved eras by `probable_operating_company` instrument type when present, delisted/acquired classification, and descending trade rows. The refreshed queue has `12,567` probable operating-company unresolved rows, and the default top-100 batch remains all probable-operating delisted/acquired rows. `[CA][PA][KBT][CDiP]`
- Dead ticker resolution workflow landed with a fillable top-priority research template and an EDGAR lead lookup helper that requires a custom SEC User-Agent before network access. `[CA][IV][REH][SFT][CDiP]`
- Manual override importer landed to dry-run and append only verified template rows while rejecting missing source evidence and duplicate `symbol_era_id` values. `[CA][IV][REH][SFT][CDiP]`
- EDGAR full-text fallback search landed for dead ticker batches where the current SEC ticker directory returns no CIK matches. Results are treated as noisy manual-review leads, not identity proof. `[CA][IV][REH][KBT][CDiP]`
- EDGAR full-text request hardening landed: the helper now mirrors SEC search-page query params, avoids unsupported `size`, defaults to narrow event-term passes without `forms`, keeps `--use-form-filter` as an opt-in probe mode, retries transient 5xx/transport failures, falls back without form filters when SEC EFTS rejects opt-in form-filtered searches, and records exhausted request failures as warning-level `search_error` rows instead of tracebacks. `[REH][PA][KBT][CDiP]`
- EDGAR full-text logging now starts with a fresh `edgar_full_text_search.jsonl` per run unless `--append-log` is explicitly requested, preventing old failed attempts from being misread as current errors. `[REH][KBT][CDiP]`
- EDGAR full-text SEC request fallback now advances across less brittle variants when EFTS 500s on ticker/date searches: primary, without forms, without dates, then ticker text in `q` without `entityName`. `[REH][PA][KBT][CDiP]`
- EDGAR full-text probe validation succeeded on the full top-100 dead-ticker template after adding quiet retry logging and a `SYMBOL AND query` fallback; isolated run exited `0` with `hit=5124`, `no_hits=40`, and no `search_error` rows. `[REH][PA][KBT]`
- EDGAR full-text alias fallback landed with an auditable alias CSV under `data/manual_overrides/`. Re-running the top-100 template resolved prior false no-hits like `XLNX` via `Xilinx AND merger`; latest runs produced merger `hit=8384` and acquisition `hit=8706`, with all 100 target symbols covered in both passes. `[REH][KBT][PA][CDiP]`
- EDGAR full-text triage reducer landed to rank noisy SEC hit rows into compact per-symbol review leads. The merger pass reduced `8,384` raw rows to `479` top-N triage rows across 100 symbols (`234` high, `175` medium, `70` manual-review rows); top-ranked symbols split `55` high, `29` medium, and `16` manual-review leads. The acquisition pass reduced `8,706` raw rows to `481` top-N triage rows (`166` high, `251` medium, `64` manual-review rows). `[CA][CSD][PA][KBT][CDiP]`
- SEC override candidate builder landed to convert high-confidence top-ranked triage rows into review-ready candidate templates without importing them as verified evidence. The merger candidate file has `55` rows and the acquisition candidate file has `47`; dry-run imports report `0` verified rows because candidates stay at `research_status=candidate_needs_review` until manual filing review completes. `[CA][IV][REH][KBT][CDiP]`
- SEC override candidate verifier landed to fetch candidate SEC filing pages with a required SEC User-Agent and score issuer, symbol, event, completion, delisting, and going-private-form evidence into strong/moderate/weak review buckets. It writes review triage only and does not mark candidates verified. `[CA][IV][REH][SFT][PA][KBT][CDiP]`
- SEC verified review batch builder landed. The latest verifier pass over `55` merger candidates produced `31` strong, `21` moderate, and `3` weak review candidates with no fetch errors or header/exhibit document selections; the 31 user-confirmed strong rows were imported into `data/manual_overrides/historical_ticker_identities.csv`, raising manual verified eras from `4` to `35` and reducing unresolved eras to `18,399`. `[CA][IV][REH][KBT][CDiP]`
- Dead ticker resolution ledger workflow landed to split the remaining `18,399` eras into route-specific lanes and track terminal workflow dispositions separately from verified historical identity overrides. New utilities build lane profiles, parent/root security disposition candidates, and dry-run/apply imports into `data/manual_overrides/ticker_era_resolution_ledger.csv`. `[CA][IV][REH][PA][KBT][CDiP]`
- Operating terminal-event workflow hardened with strict SEC date-bound fallback, terminal-window batch generation around `last_day`, and strict terminal review post-processing. This prevents reused multi-year ticker eras from matching old non-terminal S-4/proxy filings and only auto-verifies terminal-close-quality evidence. `[REH][CA][PA][KBT][CDiP]`
- Close-date evidence second pass landed for strong operating-terminal rows that missed the strict terminal-form gate. It only auto-verifies strong `8-K`/`425` evidence within five calendar days of `original_last_day` with issuer, event, and completion/delisting language; six additional rows were imported, raising manual verified eras to `51` and reducing unresolved priority eras to `16,605`. `[REH][CA][PA][KBT][CDiP]`
- SEC terminal text evidence extraction landed for high-impact operating terminal candidates. It fetches exact verifier document URLs first, falls back to archive resolution only when needed, persists bounded snippets/dates instead of full filing text, and emits review plus auto-ready CSVs without importing anything automatically. `[REH][CA][PA][KBT][CDiP]`
- CIK-based SEC terminal follow-up workflow landed for questionable terminal-text rows. It queries SEC submissions, fetches candidate terminal filings around `original_last_day`, rescans bounded snippets for explicit terminal dates, and writes review plus auto-ready outputs without import side effects. `[REH][CA][PA][KBT][CDiP]`
- End-to-end SEC terminal batch runner landed to replace one-off command loops. It processes top-N unresolved terminal rows through SEC search, triage, verifier, strict/close/text/follow-up evidence passes, and dry-run import candidate generation in one command. `[REH][CA][PA][KBT][CDiP]`
- Iterative SEC terminal resolution runner landed. It slices the unresolved priority queue, skips already-attempted rows within a run, repeatedly invokes the full SEC terminal batch, optionally applies conservative imports, and regenerates queues after successful imports. `[REH][CA][PA][KBT][CDiP]`
- Iterative SEC operating-lifecycle resolution runner landed for the remaining lifecycle lane after terminal yield decays. It emits first-day and last-day SEC search windows, builds lifecycle candidates, fetches exact filing text through the verifier path, scores bounded snippets with anchor-specific date/ticker gates, and dry-runs or applies only conservative auto-ready manual override rows. `[REH][CA][PA][KBT][CDiP]`
- CIK-based SEC lifecycle follow-up landed. The lifecycle runner now rescans candidate filings from SEC submissions around each row's first-day or last-day anchor, writes direct and follow-up review/auto-ready outputs, and imports only the combined conservative auto-ready candidate file. `[REH][CA][PA][KBT][CDiP]`
- HIST TOPS day coverage is now complete through `20260622`. `[REH][CDiP][KBT]`
- Unreadable published main Parquet repair is complete for the nine archive-quality failures surfaced by the symbol-stability scan: `20180717`, `20201214`, `20210720`, `20210908`, `20220531`, `20230105`, `20240826`, `20250310`, and `20260519`. Each day was regenerated through the explicit one-day-at-a-time repair mode and atomically replaced after local verification. `[REH][RM][CDiP]`
- Direct NAS validation after repair succeeded for all nine main files, including full row-count scans. The largest repaired days were `20250310` with `8,563,250` main rows and `327,397,099` quote rows, and `20260519` with `9,343,523` main rows and `405,703,062` quote rows. `[REH][PA][KBT]`
- Impact-weighted dead ticker resolution workplan landed under `reports/dead-ticker-review/resolution-workplan`, routing `16,605` unresolved eras into high-impact operating, operating lifecycle, derivative/parent, low-materiality bulk disposition, and manual hold buckets. Low-materiality candidates are generated for dry-run ledger review only; the latest run emitted `3,916` candidates and did not mutate the ledger. `[CA][REH][PA][KBT][CDiP]`
- Identity-first high-impact SEC resolution implementation landed with cache-first filer parsing, unique date-scoped CIK gates, historical submissions shards, same-CIK terminal and symbol-change scorers, bounded retries/circuit breaking, resumable row state, idempotent import candidates, and one-command dry-run/apply behavior. No live SEC run or override mutation was performed during implementation. `[CA][IV][REH][RM][SFT][PA][KBT][CDiP]`
- Identity-first SEC runner progress logging now emits structured startup, batch, row, retry, circuit-breaker, output, and import-step events to the existing Rich console plus JSONL sink; the state file remains the source of truth for already-running pre-patch processes. `[REH][KBT][CDiP]`
- Workplan automation reporting now distinguishes `unattempted`, `retryable`, `completed`, and `automation_exhausted` from historical resolution status. Derivative evidence gates now reject parent syntax alone and route verified instrument actions to the resolution ledger. `[CA][REH][KBT][CDiP]`
- Next operational step: run the identity-first command against the complete 1,992-row high-impact input with an approved SEC User-Agent, review dry-run candidates, then rerun with `--apply-import` if the audit outputs reconcile. `[RM][SFT][KBT]`
- Remaining follow-up: replace brittle byte-stream header scanning with a transport-aware parser that distinguishes unknown-but-well-framed messages from framing loss. `[CA][REH][AS]`
- Symbol-change (rename) candidate lane landed as the first Pareto top-2200 attack. Raw boundary pairing yields 4.6M pairs; a mutual-heaviest-volume rule collapses them to 334 review-only candidates ranked by recaptured volume, recovering all 8 seed renames at ranks 1-47. Key negative finding: IEX/SEC enrichment hints are per-symbol-latest, so dead eras carry null or smeared-modern issuer data and cannot gate rename candidates; both enrichment tables were regenerated against the quarantined era build. `[CA][IV][REH][KBT][CDiP]`
- Full daily-bars rebuild complete on the quarantined corpus: 2,389/2,393 days, 6.59B trades, 154 TradeBreak rows applied, zero unmatched. Four source days are corrupt on the NAS (`20201027`, `20220628`, `20240405`, `20240515` — thrift/snappy errors) and need PCAP repair before their bars can build. `[REH][PA][KBT]`
- V2 resolution store remigrated onto the quarantined era build. A derived era-id remap artifact (22,724 unchanged / 1,738 id-shift / 615 last-day-shift / 321 first-day-shift / 786 vanished) now translates legacy overrides, ledger, identity holds, and workplan attempts at read time; uncovered ids are dropped (not passed through) after 432 same-symbol collision misattachments were found and fixed. Cohort is now 25,622 eras; all 364 verified identities, 127 verified events, 237 leads, and 454 holds carried with zero loss; 448 weekend micro-era closures retired. Priority queue rebuilt (top 2,500). `[CA][IV][REH][KBT][CDiP]`
- Corrupt TOPS day repair complete: `20201027`, `20220628`, `20240405`, and `20240515` were regenerated from IEX HIST PCAPs through the explicit `--days ... --replace-existing` repair mode after clearing a broken cached IEXTools checkout in `/tmp` (missing `.git` and package files caused `cannot import name 'Parser'`). All four main files verified with full row-count scans; the four stale June-25 bar outputs (old era ids, no TradeBreak anti-join) were force-rebuilt one day at a time. Bars coverage is now 2,393/2,393 days with zero failed days; all 33k repaired-day bars join the current era build with zero null era ids, confirming the corrupt days never affected era boundaries. Root cause for the stale-summary trap: source-corrupt days fail before writing, leaving previous bar files in place, so incremental runs skip them. `[REH][RM][PA][KBT]`
