# Changelog

## Unreleased

- fix: `sec_name_cik_lookup.normalize_name` now drops the spelled-out word `"AND"` the
  same way a literal `"&"` already vanishes under punctuation-stripping
  (`JOINER_WORDS`), filtered anywhere in the token stream. `"PETCO HEALTH AND WELLNESS
  CO"` (OpenFIGI) vs SEC's own `"Petco Health & Wellness Company, Inc."` was the
  motivating case — an exact registrant match that failed purely on this asymmetry.
  Shared by Tier D and Tier E; replayed the entire SEC current-listings index under old
  vs. new normalization: zero new ambiguous-name collisions. Cache-only Tier E
  quantification: 4 of 16 `"AND"`-containing unresolved names newly resolve (33
  `"&"`-containing names: zero, all blocked by unrelated ambiguity); all 4 spot-checked
  against SEC's live submissions payload. 4 new tests; 553 pass (was 549), ruff/bandit
  clean. Real run (shared infrastructure, so Tier D also gained matches beyond the
  narrow Tier E quantification): resolved-CIK era rows 14,107 -> 14,130 (+23); distinct
  CIKs resolved 8,607 -> 8,612; manual-research worklist 11,213 -> 11,196 eras, 150.7M
  -> 149.2M trade rows. `[CA][IV][REH][CDiP][KBT]`

- fix: widen `sec_name_cik_lookup.JURISDICTION_SUFFIX` from an exact 2-letter trailing
  code (`/DE`, `/TX`) to any trailing `/WORD`. `BITFARMS LTD/CANADA` traced the gap:
  every one of the 22 unresolved names carrying a trailing `/WORD` is either SEC's
  `"/THE"` sorting artifact (`"EASTERN CO/THE"` = "The Eastern Company"), a `"/NEW"`
  successor tag, or a full state/country name (`/CANADA`, `/AUSTRALIA`, `/MISS`,
  `/OKLA`, `/OHIO`) — never part of a registrant's actual name. Since this touches
  `normalize_name` itself (shared by Tier D's bulk index, not just Tier E), replayed the
  entire SEC current-listings index under both patterns: zero new ambiguous-name
  collisions. Quantified Tier E yield cache-only: 28 names newly resolve. 4 new tests;
  549 pass (was 545), ruff/bandit clean. Real run: Tier E matched 2,823/4,339 ->
  2,848/4,334 (+25); 11 newly-matched names spot-checked by hand, all correct. Distinct
  CIKs resolved 8,589 -> 8,607; manual-research worklist 11,278 -> 11,213 eras, 152.5M
  -> 150.7M trade rows. `[CA][IV][REH][CDiP][KBT]`

- fix: add three ADR-suffix descriptor-pattern variants to
  `sec_name_cik_lookup.DESCRIPTOR_PATTERNS` (shared by Tier D and Tier E) — a
  differently-abbreviated `"SPONS ADR"` sibling of the existing `"SPON ADR"`, the fully
  spelled-out `"SPONSORED ADR"`, and a space between the hyphen and `"ADR"` (widening the
  old tight `-ADR$` pattern). Found tracing the worklist's top ADR-shaped rows (`SONY
  CORP-SPONSORED ADR`, `SIBANYE GOLD LTD-SPONS ADR`, `BRASKEM SA-CLASS A- ADR`), same
  abbreviation/spacing-variant category as Phase 8/9/12's earlier descriptor fixes.
  Quantified cache-only first: 11 names resolve immediately, 13 more blocked purely by
  cache misses (including the two motivating examples) — genuinely needed a live run to
  learn the true yield. 3 new test cases; 545 pass (was 542), ruff/bandit clean. Real
  run: Tier E matched 2,804/4,339 -> 2,823/4,339 (+19, confirming real additional yield
  beyond the cache-only floor). `SONY CORP-SPONSORED ADR` -> Sony Group Corp (SIC 3651),
  `SIBANYE GOLD LTD-SPONS ADR` -> Sibanye Gold Ltd (SIC 1040) — both correct;
  `BRASKEM SA-CLASS A- ADR` correctly stayed unresolved (none of its EDGAR candidates
  carry a real SIC). All 76 ADR-shaped names in the matched population spot-checked —
  no false positives. Distinct CIKs resolved 8,572 -> 8,589; manual-research worklist
  11,300 -> 11,278 eras, 154.6M -> 152.5M trade rows. `[CA][IV][REH][CDiP][KBT]`

- docs: extract `edgar_company_search_match.py`'s accumulated phase-by-phase design
  history (Phase 5-22, ~265 lines) out of the module docstring into
  `docs/EDGAR_COMPANY_SEARCH_MATCH_DESIGN.md` verbatim, replacing it with a concise
  technical summary of the current search/validation/tie-break/rejection/lead mechanisms
  and why each is safe. File 767 -> 559 lines (still over the 300-line CSD review
  threshold, but the remaining bulk is real code plus genuinely necessary per-function
  documentation, not narrative). Docstring-only change; 542 tests still pass, ruff clean,
  no behavior change. `[CA][CDiP]`

- feat: tolerate OpenFIGI's 28-character `identity_issuer` truncation in
  `edgar_company_search_match`'s name validation — the same gap Phase 9 already closed for
  Tier D (`sec_name_cik_lookup._is_prefix_relation`), never carried over to Tier E. EDGAR's
  own search already tolerates a truncated query (literal prefix matching finds the real
  registrant regardless), but exact-normalized-equality validation was rejecting the one
  real candidate it found anyway. Built the obvious fix (reuse `_is_prefix_relation`
  directly) and quantified 562 new matches cache-only, then caught two real, confirmed
  false positives before shipping via random-sampling the result set: `"TPG Pace Holdings
  Corp."` normalizes down to just `"TPG PACE"` (both `"Holdings"` and `"Corp."` are legal
  suffixes) and spuriously prefixed the unrelated sibling SPAC `"TPG Pace Beneficial
  Finance Corp."`; `"Prime Number Holding Ltd"` similarly collapsed to `"PRIME NUMBER"` and
  spuriously prefixed `"Prime Number Acquisition..."` — both real instances of the
  2020-2022 SPAC boom's pattern of one sponsor launching many similarly-named vehicles from
  a shared short prefix. That branch is only safe in Tier D because it additionally
  requires uniqueness across the *entire* SEC index; Tier E validates one
  already-EDGAR-searched candidate at a time with no equivalent check. New
  `_is_safe_final_token_truncation` keeps only the narrower "same token count, exact match
  on every token but the last" sub-case — a genuine mid-word cutoff of the final word only,
  never an entirely extra trailing word. Deliberately does not recover this fix's own
  original motivating example (`"INTERCEPT PHARMACEUTICALS IN"`, a whole extra token, not
  a same-position partial truncation) — no structural way to tell truncation noise apart
  from a real distinguishing word, so it stays unresolved rather than risk the unsafe
  branch. Re-quantified: 345 names under the safe rule (871 under the rejected broader
  rule); 80 fresh random samples of the safe-only result set found zero remaining false
  positives. 5 new tests; 542 pass (was 537), ruff/bandit clean. Real run: Tier E matched
  2,459/4,339 -> 2,804/4,339 (+345, exactly matching quantification — the largest
  single-phase yield this session); a fresh spot-check sample of 25 from the full matched
  population all correct. Distinct CIKs resolved 8,463 -> 8,572; manual-research worklist
  11,748 -> 11,300 eras, 161.1M -> 154.6M trade rows. `[CA][IV][REH][CDiP][KBT]`

- feat: add a filing-window-containment tie-break to `edgar_company_search_match` for
  genuine mid-era CIK successions the original disjoint-based tie-break can't resolve — e.g.
  `LAREDO PETROLEUM INC`'s original CIK (last filing 2019) vs. its holdco-reorg successor
  (`formerNames` carries the same name, now `Vital Energy, Inc.`, filing continuously
  2016-2025); both read `ACTIVITY_PLAUSIBLE` so the "exactly one plausible, others disjoint"
  rule can't separate them. New `_fully_contains_era` accepts the one candidate (among
  several plausible) whose own filing window fully spans the era — strictly stricter
  evidence than the "any filing lands inside" bar the single-candidate path and original
  tie-break already trust, so this never loosens acceptance, only resolves a narrower case.
  Labeled with a distinct `match_basis` (`filing_window_containment_tiebreak`). Deliberately
  doesn't resolve the sibling `LIFE STORAGE INC` shape (a REIT parent and its operating
  partnership, both filing continuously through and past the era — two legitimately
  co-existing entities, not a succession) since both candidates' windows fully contain the
  era there too; stays correctly ambiguous. Quantified cache-only first: 18 of 34 small
  (<=20-candidate) ambiguous ties resolve this way. 2 new tests; 537 pass (was 535),
  ruff/bandit clean. Real run: Tier E matched 2,441/4,339 -> 2,459/4,339 (+18, exactly
  matching quantification); all 18 spot-checked against known real companies via their
  resolved SIC — correct. Distinct CIKs resolved 8,448 -> 8,463; manual-research worklist
  11,783 -> 11,748 eras, 166.4M -> 161.1M trade rows. `[CA][IV][REH][CDiP][KBT]`

- feat: surface `blank_sic_lead_*` research-lead fields on the manual-research worklist for
  blank-SIC candidates with plausible filing activity — built first as auto-acceptance
  (`FIRST REPUBLIC BANK`, CIK 1132979, blank SIC, 42 filings spanning 2004-2024 covering its
  era, looked like strong evidence to accept), but auditing the resulting matches before
  shipping found the signal too weak: of a 34-name quantified population, roughly half had
  `entityType="other"` and zero substantive filings ever, their only history being
  ownership-disclosure forms (SC 13G, Form 3/4/5, 13F-NT) any unrelated party can file
  regardless of whether the CIK was ever the real operating entity — and tightening to
  require a substantive filing would have excluded `FIRST REPUBLIC BANK` itself too
  (plausibly a Section 12(i) case: its real 10-Ks are filed with its banking regulator, not
  SEC). No reliable way to distinguish "genuinely correct, files elsewhere" from
  "coincidental secondary filer" exists in this repo's data, so auto-acceptance was
  abandoned; redesigned as informational-only (mirrors the already-shipped `identity_disproven`
  pattern) — `blank_sic_lead_cik`/`_name`/`_high_confidence` surfaced as metadata, never
  changing `match_status`/`matched_cik`. New `sec_sic_client.SUBSTANTIVE_FORMS` distinguishes
  genuine operating/registration disclosure from ownership-disclosure forms for the
  confidence flag. 12 new tests; 535 pass (was 531), ruff/bandit clean. Real run caught a
  second real bug before it shipped: a lead found at a narrower query level was silently
  dropped when a later, broader query hit the candidate-count cap and returned early without
  carrying it forward — losing the `FIRST REPUBLIC BANK` lead itself. Fixed, regression test
  added, rerun: 34 leads found (12 high-confidence), match outcomes and CIK-resolution counts
  fully unchanged (informational-only, as designed). `[CA][IV][REH][CDiP][KBT]`

- feat: surface `identity_disproven` on the manual-research worklist — a name whose Tier E
  single-candidate match was proven filing-activity-disjoint (Phase 16) now carries that proof
  through to the worklist instead of staying a silent internal rejection. Root case: `UTX` (real
  ticker for United Technologies Corp, 919K trade rows) carries an OpenFIGI-asserted
  `identity_issuer` of `"ULTRATREX INC-A"`, a real but unrelated shell — OpenFIGI's ticker-keyed
  `/v3/mapping` isn't date-aware and now resolves `UTX` to whoever currently holds that ticker
  string, the same current-listing bias this project has hit at every other layer, one step
  further upstream (at the identity assertion itself, not CIK resolution). No available data
  source can recover the correct name for this case, but the worklist can now warn a researcher
  off it rather than presenting a plausible-looking wrong name. New `identity_disproven` field on
  `match_issuer_name`'s result (purely additive, no matching-logic change), threaded through
  `build_edgar_company_search_matches.py`, a new `apply_identity_disproven` side-channel in
  `sector_enrichment_inputs.py` that never touches `resolved_cik`/`cik_source`, and surfaced as a
  worklist column, summary count, and struck-through marker in the top-rows report. 8 new tests;
  522 tests pass (was 514), ruff/bandit clean. Real run confirmed byte-identical match outcomes
  except the new field: 82 names / 100 worklist eras flagged. `[CA][IV][REH][CDiP][KBT]`

- fix: gate `edgar_company_search_match`'s single-candidate accept on filing activity, closing
  the one path the filing-activity guard never covered — a lone validated candidate was accepted
  on name+SIC alone, filing history unchecked, even when provably disjoint from the era. Audited
  the 2,515 then-`matched` names cache-only (zero network calls): 82 provably disjoint (e.g.
  `AETNA INC` -> CIK 1013761, filings ending 2015 for an era starting 2016-12-12 — the real
  operating Aetna for those eras is a different CIK, 1122304), 42 more `ACTIVITY_UNKNOWN`
  (correctly left alone, same "quiet filer" reasoning the tie-break already applies). New
  `_provably_disjoint` rejects and falls through to a shorter query instead of returning a
  confidently-wrong match — lets the existing tie-break resolve some of the 82 to the *correct*
  CIK once the wrong one stops shadowing it, rather than just losing the match. 5 new tests,
  including the Phase-14 regression test rewritten to assert the new (correct) behavior; 514
  tests pass (was 510), ruff/bandit clean. Real run (two passes, second clearing 8 transient
  fetch errors): all 82 cleared out of `single_validated_candidate` — 8 resolved to a different,
  correct CIK via the tie-break (`EXTENDED STAY AMERICA INC` 1002579 -> 1581164 post-bankruptcy-
  reorg entity; `OSIRIS THERAPEUTICS INC` 912815 -> 1360886), 45 honestly `no_validated_match`,
  29 honestly `ambiguous_candidates`, zero left wrongly matched. Tier E matched 2,515/4,339 ->
  2,441/4,339 (-74, expected — correctness over coverage); distinct CIKs resolved 8,517 -> 8,448;
  manual-research worklist 11,683 -> 11,783 eras, 162.8M -> 166.4M trade rows.
  `[CA][IV][REH][CDiP][KBT]`

- feat: raise `edgar_company_search_match.MAX_CANDIDATES_TO_VALIDATE` from 8 to 20, now that the
  filing-activity guard (previous entry) proves candidates can be validated safely at scale — the
  original cap guarded against wasted requests on implausibly generic queries, not against
  validating more candidates being unsafe (validation is exact-match; a wrong candidate simply
  fails to validate). Genuinely unquantifiable for free first (unlike every other phase this
  session): names with 9-20 candidates were never validated at all under the old cap, so nothing
  sat in the cache to replay. Shipped as a real ~19-minute live SEC run. 2 tests updated to derive
  their over-cap candidate count from the real constant instead of a hardcoded `9`; 510 tests still
  pass, ruff/bandit clean. Real run: Tier E matched 2,495/4,339 -> 2,515/4,339 (+20); distinct CIKs
  resolved 8,500 -> 8,517; manual-research worklist 11,707 -> 11,683 eras, 169.7M -> 162.8M trade
  rows. Resolved the two running examples this module's own docstring has cited since Phase 5/7/11
  as "correctly staying unresolved" — `CFLT` (-> the real Confluent, Inc., CIK 1699838) and `MYLAN
  NV` (-> CIK 69499) — purely because their candidate counts had never fit under the old cap.
  `[CA][IV][REH][CDiP][KBT]`

- feat: add a filing-activity tie-break to `edgar_company_search_match` (Tier E) for genuine 2-way
  name collisions between real registrants — e.g. the real Continental Resources, Inc. (CIK 732834,
  ticker `CLR`) vs an unrelated same-named junior-mining shell whose last SEC filing was a 2013
  voluntary deregistration. `sec_sic_client.fetch_filing_activity` reads `filings.recent`/
  `filings.files` from the same already-fetched submissions payload `fetch_sic` uses (zero new
  network cost); a candidate is only accepted when every tied candidate gets a definite
  plausible/disjoint verdict against the era's date span and exactly one is plausible — a candidate
  whose history merely brackets the era without a filing inside it stays `unknown`, never rejected.
  Supersedes a reverted prior attempt using SEC's `tickers` field, which yielded 0 real matches
  since it only reflects current listing state. 16 new tests; 510 pass (was 494); ruff/bandit
  clean. Real run: Tier E matched 2,453/4,342 -> 2,495/4,339 (43 via the new tie-break); distinct
  CIKs resolved 8,470 -> 8,500; manual-research worklist 11,791 -> 11,707 eras, 180.8M -> 169.7M
  trade rows. All 43 newly-resolved matches spot-checked against known real companies (Raytheon,
  CIT Group, Mead Johnson Nutrition, Nuance Communications, Sealed Air, Callon Petroleum, IAC,
  Rexnord, Vivint Smart Home, GCI Liberty, MB Financial, Renewable Energy Group, Welbilt, among
  others) — all correct. `[CA][IV][REH][CDiP][KBT]`

- perf: parallelize the test suite with `pytest-xdist` (`-n auto` in the default
  `addopts`, so the canonical `uv run -m pytest -q` command runs parallel with no flag
  change needed). Verified safe first: every test's file I/O is `tmp_path`-scoped, no
  test `chdir`s the process, and no test mutates a shared real path or unguarded env var.
  A real, if latent, test bug surfaced immediately on the first parallel run —
  `test_workflow_emits_structured_progress_logs` set `caplog`'s level on the stale
  pre-`utils/legacy/` reorg logger name (`"utils.sec_high_impact_workflow"` instead of
  the real `"utils.legacy.sec_high_impact_workflow"`); it only ever passed under strict
  serial execution because an earlier test's global root-logger level (mutable,
  process-wide) happened to leak through in that fixed order. Fixed the logger name, not
  the symptom. Wall-clock: 494 tests, ~10.8s serial -> ~6.5s parallel on this machine. All
  494 tests pass 3x in a row post-fix; ruff/bandit clean. `[PA][REH][CDiP]`

- fix: tolerate two spacing variants in `sec_name_cik_lookup` normalization that were blocking
  otherwise-exact matches — `DESCRIPTOR_PATTERNS`' `-CLASS [A-Z]$` pattern now mirrors the
  already-tolerant `-CL A` pattern's spacing (`"SWEETGREEN INC - CLASS A"`, `"FIRST DATA CORP-
  CLASS A"`), and both patterns now also consume a space before the hyphen, closing a
  trailing-space artifact the original `-CL A` fix left behind (`"UCP INC - CL A"`).
  `JURISDICTION_SUFFIX` now tolerates a space between the slash and the state code (`"Alight Inc.
  / DE"`, as returned by SEC's own submissions payload), not just the tight `"/DE"` form. Found by
  replaying the actual `match_issuer_name` function against real worklist names rather than
  eyeballing candidate lists by hand — an earlier manual trace nearly mis-diagnosed one case
  (`HZNP`) as an unrelated `PLC`-suffix gap. Quantified from the existing request cache (zero new
  network calls): 15 names flip to a validated match. 6 new tests; 494 pass (was 488); ruff/bandit
  clean. Real run: Tier E matched 2,438/4,342 -> 2,453/4,342; distinct CIKs resolved 8,455 ->
  8,470; manual-research worklist 11,817 -> 11,791 eras, 187.7M -> 180.8M trade rows. Also traced,
  but deliberately left unfixed pending a ticker-based disambiguator: `normalize_name` treats
  "GROUP" as a droppable legal suffix, causing a real false-ambiguity collision between an
  unrelated shell (`"Continental Resources Group, Inc."`) and the real Continental Resources
  (`CLR`). `[CA][IV][REH][CDiP][KBT]`

- feat: drop `edgar_company_search_match.MIN_QUERY_WORDS` from 2 to 1 — a name already exactly 2
  words after descriptor-stripping (`"HOLOGIC INC"`, `"ZENDESK INC"`, `"ANAPLAN INC"`) could never
  truncate further under the old floor, so a literal-prefix mismatch against EDGAR's real
  registrant punctuation left it with zero candidates at every query tried (527 names/1,800
  eras/58.7M trade rows, the largest remaining worklist bucket). Live-verified before changing
  anything that the risk the floor was guarding against (the documented `CFLT`/`CONFLUENT`
  same-name-shell collision) is already handled by the existing SIC-must-exist guard, not the word
  count: 1-word queries for `HOLOGIC`/`ZENDESK`/`ANAPLAN` each found a real single candidate, with
  `HOLOGIC`'s blank-SIC unrelated-entity hit correctly rejected rather than accepted; re-checked
  `CONFLUENT` itself live and confirmed it now returns 9 candidates (over the validate cap), so
  `CFLT` still correctly stays unresolved via that separate guard. 3 new tests + 1 existing test
  updated; 488 pass (was 485); ruff/bandit clean. Real run (54m20s wall-clock, genuinely
  rate-limited): `no_candidates` 527 -> 54; `ambiguous_candidates` 263 -> 1,348 (the anticipated
  trade-off — a broader query surfaces real ambiguity more often, correctly left unresolved rather
  than guessed); `matched` 2,287 -> 2,438; distinct CIKs resolved 8,351 -> 8,455; manual-research
  worklist 12,007 -> 11,817 eras, 210.3M -> 187.7M trade rows. `[CA][IV][REH][CDiP][KBT]`

- feat: extend `edgar_company_search_match` (Tier E) to validate a candidate against SEC's
  `formerNames` history, not just its current registrant name — companies that renamed or merged
  since an era's ticker was active (`"CABOT OIL & GAS CORP"` -> now `"Coterra Energy Inc."`) were
  finding the right single-candidate CIK and then failing validation purely because the current
  name doesn't match. `sec_sic_client.fetch_sic` now surfaces `former_names` from the same
  already-fetched SEC submissions payload; the existing SIC-must-exist guard and
  2-validated-matches-is-ambiguous rule both still apply unchanged. Quantified first from the
  existing request cache (zero network calls): 515 names/724 eras/46.0M trade rows recoverable, 9
  names correctly staying ambiguous. Real run: only 21 new network requests (8,330 cache hits);
  Tier E matched 1,844/4,584 (40.2%) -> 2,287/4,342 (52.7%); distinct CIKs resolved 8,276 -> 8,351;
  manual-research worklist 12,691 -> 12,007 eras, 245.8M -> 210.3M trade rows. 485 tests pass (was
  480); ruff/bandit clean. `[CA][IV][REH][CDiP][KBT]`

- feat: add a word-boundary prefix-match fallback to `sec_name_cik_lookup.match_by_name` (Tier D),
  covering OpenFIGI's 28-character `name`-field truncation (sometimes mid-word) and un-expanded
  Bloomberg abbreviations (`HLDGS`, `INTL`) left over after normalization. Unambiguous only: ≥2
  tokens on the shorter side, exactly one distinct CIK across every candidate satisfying the
  relation. Guards a real false-positive risk found while building it — SPAC sequel numbering
  (`"...Corp II"` vs `"...Corp III"`) is a genuinely different company even though `"II"`
  string-prefixes `"III"` — via a Roman-numeral-remainder check. Quantified first: 220 names/~21M
  trade rows recoverable among names whose ticker was independently confirmed current-SEC-listed;
  50 genuine ticker-reuse cases (`IAC`, `USEG`) correctly stayed unmatched. Real run (`--skip-fetch`,
  zero network calls): distinct CIKs resolved 8,201 -> 8,276; manual-research worklist 13,131 ->
  12,691 eras, 264M -> 245.8M trade rows. 480 tests pass (was 475); ruff/bandit clean.
  `[CA][IV][REH][CDiP][KBT]`

- feat: extend `sec_name_cik_lookup.DESCRIPTOR_PATTERNS` to cover a bare trailing `-A`/`-B`
  share-class letter (`"EVERPURE INC-A"`) and the `"- CL A"` spacing variant
  (`"ROYALTY PHARMA PLC- CL A"`), both visible at the top of the refreshed worklist after the Tier
  E recall fix. Quantified first: 599 rows (29.9M trade rows) with the bare-letter form, 23 rows
  (4.1M trade rows) with the spacing variant. Improves both Tier D and Tier E, since both share
  `strip_security_descriptors` — `PSTG` resolved immediately through Tier D alone, no live search
  needed, since its real current name was already in SEC's current-listings file and the `-A`
  suffix was the only blocker. Real run: Tier D resolved 24 more CIKs / 106 more eras with zero
  network calls; a fresh Tier E pass then rose 1,680/4,627 (36.3%) -> 1,844/4,584 (40.2%). Distinct
  CIKs resolved 8,085 -> 8,201; eras with real SIC+sector 11,150 -> 11,466 (31.1%);
  manual-research worklist 13,448 -> 13,131 eras, 292M -> 264M trade rows; top-500 volume
  concentration 83.0% -> 83.4% `[CA][IV][REH][CDiP][KBT]`

- feat: fix Tier E's EDGAR search-recall gap (progressive truncation + validate-among-candidates)
  and a jurisdiction-tag bug in `sec_name_cik_lookup.normalize_name` that also silently fixed Tier
  D for the same names (`CORZ`). A live smoke test before the full run caught a real false-positive
  risk the fix itself introduced — a genuine SEC name collision between the real Confluent, Inc.
  (CIK 1699838, SIC 7372) and an unrelated same-named shell (CIK 1171179, blank SIC) that plain
  normalized-name matching can't tell apart — fixed by requiring a real SIC on record before
  accepting any name match, free since it's already fetched during validation. A second, unrelated
  bug was caught mid-run: rerunning the EDGAR search over only the residual unresolved pool and
  overwriting its output silently dropped the previous run's 969 matches (`reconcile_cik` rebuilds
  fresh from that file every run); fixed by re-including any name Tier E previously resolved in the
  search population so every run recomputes a complete table, not an eroding one. Real run (4,627
  names, mostly cache hits): EDGAR matches 969/4,453 (21.8%) -> 1,680/4,627 (36.3%); distinct CIKs
  resolved 7,546 -> 8,085; eras with real SIC+sector 10,070 -> 11,150 (30.2%); manual-research
  worklist 14,491 -> 13,448 eras, 378M -> 292M trade rows; top-500 volume concentration 78.7% ->
  83.0%. Of the original spot-checked top-10 worklist rows, 7/9 with any name in the pipeline now
  resolve correctly (`ATVI`, `X`, `BK`, `FTCH`, `CORZ`, `PXD`); only `PSTG`/`HOLX` (genuine EDGAR
  search misses) and `GPS` (no name anywhere in the pipeline) remain `[CA][IV][REH][CDiP][KBT]`

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
