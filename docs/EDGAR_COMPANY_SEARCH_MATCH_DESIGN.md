# `utils/edgar_company_search_match.py` — Design History

This is the full phase-by-phase narrative for Tier E's issuer-name-to-CIK matching
(`match_issuer_name` and its helpers), preserved verbatim from the module's own docstring
as of Phase 22. The module docstring itself was trimmed to a concise technical summary in
Phase 23 to bring the file back under the CSD 300-line review threshold — this file is
where the full historical reasoning, quantification numbers, and real-run results live
now. See `docs/ARCHITECTURE.md` for the broader SIC/sector-classification project context
and `docs/TASK_LIST.md` for the complete session-by-session record (all phases, not just
this module's).

---

EDGAR's classic `browse-edgar` company search does **literal prefix matching** against
the exact registered name string — confirmed live: `"CORE SCIENTIFIC INC"` (an
OpenFIGI/IEX-style name) returns zero hits against the registrant's actual `"Core
Scientific, Inc./tx"`, but the shorter `"Core Scientific"` matches immediately, because
prefix matching only needs the query to literally start the target string, and the
comma/suffix/jurisdiction-tag divergence breaks that for the full name but not the
truncated one. So this tries the full (descriptor-stripped) name first, then
progressively drops trailing words down to a single-word floor, stopping at the first
query that yields a validated match.

A 2-word floor was the original design (1-word queries considered "too generic to be
worth the ambiguity/request cost"), which meant a name that's already exactly 2 words
after descriptor-stripping (`"HOLOGIC INC"`, `"ZENDESK INC"`, `"ANAPLAN INC"`) could
never truncate further — and if the full 2-word query didn't literally prefix-match the
registrant's real punctuation, the name got zero candidates at *every* query tried, with
no shorter fallback possible. Live-checked whether dropping to 1 word reintroduces the
false-positive risk that motivated the floor (see the `CONFLUENT` case below): querying
`HOLOGIC`, `ZENDESK`, and `ANAPLAN` each found the real registrant as a validated match
(`Hologic`'s own single-word search actually surfaces an unrelated blank-SIC limited
partnership first, correctly rejected by the guard below rather than accepted); querying
`MYLAN` returned 12 candidates, over the `MAX_CANDIDATES_TO_VALIDATE` cap at the time
(8), so it was reported ambiguous exactly as the count guard already handled then, not a
new risk — the cap has since been raised (Phase 15 below), and `MYLAN` now validates
cleanly to a single real candidate, CIK 69499. The 1-word floor is safe specifically
because the guards below don't change — they were always what made a broad query
trustworthy, not the word count.

A truncated query is more permissive, so it usually returns *more* candidates, not
fewer — every candidate a query returns is validated against the real registrant name
(the same `sec_sic_client.fetch_sic` call already used for SIC, often a free cache hit),
never a bare "only one search hit" trusted on its own. If more than one candidate
validates, that's genuine ambiguity (two different real companies both matching the
query name), not a bug to work around. A query returning an implausibly large candidate
count (self-evidently too generic to be useful) is reported ambiguous without validating
each one individually, and truncation stops there — a shorter query would only be worse.

A validated name match still isn't automatically trusted: a live run surfaced a genuine
SEC name *collision* — searching for the real, Kafka-company "Confluent, Inc." (CIK
1699838, SIC 7372) also turned up an unrelated same-named shell, "CONFLUENT INC" (CIK
1171179, blank SIC), that normalized-name matching alone can't tell apart from the real
one. Every confirmed-correct match found while building this had a real SIC on record;
the collision didn't — so a candidate with no SIC is never accepted, even if its name
matches exactly. This is free (the SIC is already fetched during validation), and safe
by the same logic as everything else here: better genuinely unresolved than
confidently wrong. (`CONFLUENT` alone, re-checked live after lowering the query floor,
returned 9 candidates at the time — over the `MAX_CANDIDATES_TO_VALIDATE` cap then in
effect (8) — so `CFLT` stayed correctly unresolved via the candidate-count guard, not a
regression; once that cap was raised (Phase 15 below), all 9 got validated and exactly
one — the real Confluent, Inc., CIK 1699838 — passed, with the blank-SIC shell correctly
rejected by the guard right above rather than by candidate count.)

A candidate also validates against any of its SEC `formerNames` entries, not just its
*current* registrant name — a large share of real single-candidate hits were being
silently discarded as `no_validated_match` purely because the company renamed or merged
since the era's ticker was active (`"CABOT OIL & GAS CORP"` finds CIK 858470's one and
only candidate immediately, but that CIK's current name is `"Coterra Energy Inc."` post-
2021-merger — its `formerNames` carries the exact queried name with a real date range).
Same authority as `entity_name` (same already-fetched SEC submissions payload, just a
field nobody read before), same SIC-must-exist guard, so no new trust assumption:
checked live on the real worklist before shipping — 515 names / 724 eras / 46.0M trade
rows recoverable this way, with 9 names correctly staying ambiguous because more than one
candidate's history matches.

A genuine 2+-way name collision (real registrants, both with a real SIC, both matching
the query name — the blank-SIC guard above can't help when both candidates actually have
one) can still sometimes be broken by whether a candidate's real SEC filing history
(`sec_sic_client.fetch_filing_activity`) is even consistent with it having been the
operating entity during the era: "Continental Resources, Inc." (CIK 732834, ticker `CLR`,
SIC 1311) collides by name with an unrelated same-named junior-mining shell,
"Continental Resources Group, Inc." (CIK 1430975, SIC 1000 — a real, non-blank SIC, so
the blank-SIC guard doesn't apply). The shell's last SEC filing is a `15-12G` voluntary
deregistration filed 2013-03-05; it has been permanently dark since, so it cannot
possibly be the real trading entity for any era in this repo's IEX TOPS data (2016-12-12
onward), while the real Continental Resources filed continuously through the whole
window. `_filing_activity_verdict` only ever accepts when it can *prove* a candidate is
disjoint from the era (its newest filing predates the era's start, or every known filing
postdates the era's end with no older shard history left uncertain) — a candidate whose
history merely brackets the era without a filing actually landing inside it is left
`ACTIVITY_UNKNOWN`, not rejected, since a real filer can legitimately be quiet across a
short window (some worklist eras span under two weeks). `_disambiguate_by_filing_activity`
then only accepts when every tied candidate got a definite verdict and exactly one came
back plausible — a single `UNKNOWN` anywhere blocks acceptance, same "better genuinely
unresolved than confidently wrong" posture as everything else here.

A prior attempt at this same problem tried SEC's `tickers` field as the tie-break signal
instead and found **zero real yield**: `tickers` only reflects a registrant's *current*
listing state, and this entire population is, by construction, companies no longer
trading — the real Continental Resources itself was taken private in 2023 and shows
`tickers: []` today, identical to the unrelated shell it's tied with. Filing history
survives delisting where current-listing state cannot: a company still files its
deregistration paperwork on its way out, and that's exactly the signal that distinguishes
it from a shell that's simply been dormant for over a decade. `filings.files` shard-
walking for older history beyond `filings.recent`'s ~1000-row window was evaluated and
not built: measured against the real worklist, it would unblock exactly one additional
name — not worth the added network cost and complexity for that yield; `has_older_shards`
being `True` is treated as "can't tell" (`ACTIVITY_UNKNOWN`) rather than fetched.
`_validate_candidates` no longer stops at 2 validated candidates for the same reason —
some real ties involve a 3rd+ candidate that needs to be seen to disambiguate correctly.
Quantified from the existing request cache before shipping (zero network calls): of the
74 names that reach a genuine 2-way validated tie, 38 resolve via this guard.

`MAX_CANDIDATES_TO_VALIDATE` was raised from 8 to 20 once the filing-activity guard gave
a real way to disambiguate a wider candidate set safely — the risk the original 8-cap
guarded against was never really "validating more candidates is dangerous" (validation
is exact-match, so a wrong candidate simply fails to validate, same as ever), it was
"validating a implausibly generic query's candidates is wasted request cost." Unlike the
filing-activity guard, this one genuinely couldn't be quantified for free first: names in
the 9-20 candidate range had *never* been validated at all under the old cap (the count
guard returns before fetching any of them), so there was nothing cached to replay.
Shipped as a real, rate-limited live run instead, same request budget per candidate as
always. Net result: 20 more names matched (18 by ordinary single-candidate validation, 2
via the filing-activity tie-break), plus a valuable reclassification — 210 previously
"ambiguous, never checked" names turned out to validate zero real candidates at all once
actually looked at, correctly reclassified as `no_validated_match` rather than sitting in
an falsely-alarming ambiguous bucket. This also finally resolved the two running examples
this docstring has cited since Phase 5/7/11 as "correctly staying unresolved" — `CFLT`
and `MYLAN` — both purely because their candidate counts (9 and 12) had never before fit
under the cap, not because of any new logic.

Phase 16 closed a gap the filing-activity guard never covered: `_disambiguate_by_filing_activity`
only ever ran when 2+ candidates validated by name, so a *single* validated candidate was
accepted on name+SIC alone, with its filing history never checked against `era_span` at
all — even when that history was flatly disjoint from the era. Quantified first, cache-only
(zero network calls, `EvidenceRegistry.get(...)` read directly against the request cache
already populated by every prior real run): applying the identical `ACTIVITY_DISJOINT`
check to all 2,515 then-`matched` names found 82 with filing history that provably
couldn't overlap their era (e.g. `AETNA INC` -> CIK 1013761, filings 1996-2015 for an era
starting 2016-12-12 — the real operating Aetna for those eras is a different CIK,
1122304 — and several modern-SPAC name collisions like `ALBATROSS ACQUISITION CORP`,
matched to a CIK whose entire filing history postdates the era it was matched to by
years), plus 42 more `ACTIVITY_UNKNOWN` (bracketing history, no filing landing inside a
narrow era window — correctly left alone, same "quiet filer" reasoning as the tie-break
guard). `_provably_disjoint` now gates the single-candidate accept the same way the tie-
break already gated a 2+-way accept: `continue` to a shorter query instead of returning a
confidently-wrong match, so a name whose only reachable candidate is disjoint gets a
chance to resolve against a different candidate at a broader query (some of the 82 land on
the *correct* CIK this way, via the existing tie-break, once the wrong one stops
shadowing it) rather than simply losing its match outright. Fails open exactly like the
tie-break guard: no `era_span` or a failed filing-activity fetch never rejects. This
supersedes the Phase 14 regression test asserting the opposite — that test existed to
keep the single-candidate path unchanged *until this exact audit ran*, not as a permanent
guarantee; the module docstring's Phase 14 entry above still narrates why the guard
originally stopped short of this path.

Phase 18 surfaces the Phase 16 guard's own evidence as a result field instead of just an
internal rejection: `identity_disproven` is `True` whenever `_provably_disjoint` rejected
a single candidate at any truncation level, even if the name ultimately stays unmatched
(no shorter query finds a real replacement). Found by tracing a top-worklist row by hand —
`UTX` (era 2016-12-12..2020-04-03, 919K trade rows, the real United Technologies Corp,
delisted the day this era ends by its merger into Raytheon Technologies) carries an
OpenFIGI-asserted `identity_issuer` of `"ULTRATREX INC-A"`, a real but unrelated shell.
OpenFIGI's ticker-keyed `/v3/mapping` endpoint isn't date-aware and returns whatever
entity currently (or most recently) holds a ticker string, the same current-listing bias
this project has hit at every other layer (Tier C, `ticker_continuity`, the reverted
Phase 13 attempt) — one layer further upstream than any of those, at the identity
assertion itself rather than at CIK resolution. This module already independently proves
`"ULTRATREX INC-A"` can't be the operating entity for `UTX`'s era (its real filing history
is 2025-2026, entirely outside it), so the evidence to warn a researcher off the name
already exists; it just wasn't surfaced. 74 names across the still-unresolved population
carry this proof (100 worklist eras, 3.66M trade rows) — real research-quality value, not
a one-off. No CIK-matching behavior changes: `identity_disproven` is purely additive
metadata on the existing result dict.

Phase 19 tried loosening the blank-SIC guard itself and, after auditing the results
before shipping, backed off to a safer, informational-only design. Tracing a top-worklist
row (`FIRST REPUBLIC BANK/CA`, priority rank 3, 1.86M trade rows) found EDGAR's
`"FIRST REPUBLIC"` search surfacing CIK 1132979, whose current name is the *exact* string
`"FIRST REPUBLIC BANK"` — but a blank SIC, so `_validate_candidates` rejected it outright
without ever checking its filing history. That CIK has 42 real filings spanning
2004-2024, squarely covering (and extending well past) FRC's 2016-2023 era — the same
`ACTIVITY_PLAUSIBLE` signal already trusted for the tie-break and Phase 16's
disjoint-rejection, which looked like strong independent evidence to *auto-accept* the
candidate the same way `_provably_disjoint` auto-*rejects* one.
Spot-checking the resulting matches before shipping (this project's standing practice,
not skipped here) found the signal too weak to trust blindly: of a 34-name quantified
population, roughly half had `entityType="other"` and *zero* substantive filings ever —
their entire filing history was ownership-disclosure forms (SC 13G, Form 3/4/5, 13F-NT)
that any unrelated third party can file *about* a CIK regardless of whether it was ever
the real operating entity (`BLACK KNIGHT INC`, `FARMER BROS CO`, `SHELL MIDSTREAM
PARTNERS LP`, `STEEL PARTNERS HOLDINGS LP`, `WELLESLEY BANK` among them — all real
companies, but these specific candidate CIKs are very likely the wrong entity for them).
Worse, tightening to require a genuine *substantive* filing (`sec_sic_client.SUBSTANTIVE_FORMS`
— 10-K/10-Q/8-K/S-1/etc.) landing in the era would have excluded `FIRST REPUBLIC BANK`
itself too: its entire 42-filing history is also ownership-disclosure forms only,
plausibly because Section 12(i) lets certain banks file their real 10-Ks with their
banking regulator instead of SEC, so SEC's own system never sees them. No reliable way to
distinguish "genuinely the right company, files elsewhere" from "coincidental secondary
filer" exists anywhere in this repo's data — so this never became a new acceptance path.
`_find_blank_sic_lead` surfaces the same candidate purely as `blank_sic_lead_*`
informational fields on the result (`blank_sic_lead_cik`, `blank_sic_lead_name`,
`blank_sic_lead_high_confidence` — the latter from `_is_high_confidence_lead`, requiring
both `entityType="operating"` *and* a substantive filing inside the era) — a strong
research lead for a human, never an automatic accept, `match_status`/`matched_cik`
completely unaffected.

Phase 20 extends `_disambiguate_by_filing_activity` itself for a shape the original
"exactly one plausible, all others disjoint" rule can't resolve: a genuine mid-era
successor. `LAREDO PETROLEUM INC`'s EDGAR search surfaces two real, non-blank-SIC
candidates that both validate by name — CIK 1519352, currently still named `"Laredo
Petroleum, Inc."` but whose last filing is 2019-01-31, and CIK 1528129, whose
`formerNames` carries `"Laredo Petroleum, Inc."` (and `"Laredo Petroleum Holdings,
Inc."`) and which is now `"Vital Energy, Inc."`, filing continuously from 2016 onward
(the 2023 rename explaining why the LPI-ticker era ends almost exactly when it does).
Both read `ACTIVITY_PLAUSIBLE` (each has *some* filing landing in the era), so the
original rule — built for a disjoint-shell collision, not a live succession — can't tell
them apart and reports ambiguous. `_fully_contains_era` breaks the tie by requiring more
than plain plausibility: among the plausible candidates, accept the one whose *entire*
filing window (earliest to latest) spans the *whole* era, when exactly one does. This is
strictly stricter evidence than what the single-candidate path and the original
one-plausible tie-break already trust (an "any filing lands inside" bar), so it never
loosens anything — it only resolves a narrower case those couldn't reach at all. Labeled
with a distinct `match_basis` (`BASIS_FILING_WINDOW_CONTAINMENT`), since containment
evidence doesn't *prove* the non-picked candidate was never active, only that its own
history doesn't span the whole era — real but meaningfully different confidence from a
provable-disjoint tie-break. Deliberately doesn't resolve every multi-plausible tie: the
real `LIFE STORAGE INC` case (a REIT parent and its operating partnership, both filing
continuously through and past the whole era — two *legitimately* co-existing real
entities, not a succession) has both candidates' windows fully containing the era, so
containment can't pick between them either, and it correctly stays ambiguous rather than
guessed; a different signal (SIC specificity, e.g. `6798`-REIT vs. a sibling's generic
`6500`) would be needed there, not built here. Quantified first, cache-only (zero network
calls, replaying the existing search+validate sequence against 34 currently-`ambiguous`
names with a small, already-fully-fetched candidate set): 18 resolve via window
containment (`BLACK KNIGHT INC`, `DCP MIDSTREAM LP`, `TRAVELCENTERS OF AMERICA INC`,
`COLONY CAPITAL INC` -> now `DigitalBridge Group, Inc.`, `TCF FINANCIAL CORP`, among
others), 10 stay ambiguous with the `LIFE STORAGE`-shaped no-containment-difference,
4 already correctly blocked by an `ACTIVITY_UNKNOWN` candidate.

Phase 21 investigated (but didn't build) a SIC-specificity signal for the `LIFE STORAGE`
shape and found it unsafe — a real REIT parent can carry the *generic* SIC while its
operating-partnership sibling carries the specific one, the opposite of what the idea
needed. Negative result; see `docs/TASK_LIST.md`'s Phase 21 entry, no code shipped.

Phase 22 closes part of a gap between Tier D and Tier E that Phase 9 only ever fixed on
one side: OpenFIGI truncates `identity_issuer` to a hard 28-character ceiling, sometimes
mid-word (`"10X CAPITAL VENTURE ACQUISIT"` for the real `"...Acquisition Corp"`). EDGAR's
own search already tolerates this (literal prefix matching finds the real registrant
regardless), but `_names_match` required *exact* normalized equality, so the single
correct candidate it found was rejected anyway — `no_validated_match`, not `matched`.

The original motivating example, `INTERCEPT PHARMACEUTICALS IN` (OpenFIGI cut `"INC"`
down to `"IN"`), turned out to need `sec_name_cik_lookup._is_prefix_relation`'s *broader*
"extra trailing tokens, exact match at that position" case (`"INTERCEPT
PHARMACEUTICALS"` cleanly prefixes the query with `"IN"` left over) — the same branch,
built and quantified here first, that produced two confirmed false positives before any
code shipped: `"TPG Pace Holdings Corp."` normalizes down to just `"TPG PACE"` (both
`"Holdings"` and `"Corp."` are legal suffixes) and spuriously prefixes the unrelated
sibling SPAC `"TPG Pace Beneficial Finance Corp."`; `"Prime Number Holding Ltd"`
similarly collapses to `"PRIME NUMBER"` and spuriously prefixes `"Prime Number
Acquisition..."` — both real instances of the 2020-2022 SPAC boom's common pattern, one
sponsor launching many similarly-named vehicles from the same short prefix. That branch
is only safe in Tier D because it additionally requires uniqueness across the *entire*
SEC index; Tier E validates one already-EDGAR-searched candidate at a time with no
equivalent global-uniqueness check available, and there's no way to structurally tell
"IN is truncation noise of INC" apart from "BENEFICIAL FIN is a real distinguishing
name" from token shape alone — so that branch was dropped entirely for this module,
including the case that originally motivated it.

`_is_safe_final_token_truncation` keeps only the narrower "same token count, exact match
on every token but the last" sub-case (`sec_name_cik_lookup`'s
`MIN_PARTIAL_TOKEN_CHARS`/`ROMAN_NUMERAL_CHARS` constants, reused directly) — a genuine
mid-word cutoff of the *final* word only, never an entirely extra word. Quantified before
shipping (zero network calls, replaying the existing search+validate sequence against
the SQLite request cache): of 1,826 still-unresolved names, 345 gain a validated match
under the safe rule (871 under the broader, rejected rule — confirming the unsafe branch
alone would have accounted for more than half the naive yield). 80 fresh random samples
of the safe-only result set spot-checked by hand found zero remaining false positives, a
sharp contrast to 2 found in the first ~100 samples of the broader rule.

Real run: Tier E matched 2,459/4,339 -> 2,804/4,339 (+345, exactly matching
quantification — the largest single-phase yield of the whole SIC/sector session). 542
tests pass; ruff/bandit clean. Distinct CIKs resolved 8,463 -> 8,572; manual-research
worklist 11,748 -> 11,300 eras, 161.1M -> 154.6M trade rows.
