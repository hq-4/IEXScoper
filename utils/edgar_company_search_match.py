"""Match an issuer name to a CIK via EDGAR's company-name browse search
(`utils.sec_company_search_client`).

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
4 already correctly blocked by an `ACTIVITY_UNKNOWN` candidate. [CA][IV][REH][KBT]
"""

from __future__ import annotations

from typing import Any

from utils.resolution_v2_network import CachedPrimaryClient, PrimarySourceError
from utils.sec_company_search_client import search_company_ciks
from utils.sec_name_cik_lookup import normalize_name, strip_security_descriptors
from utils.sec_sic_client import STATUS_FETCH_ERROR as SIC_STATUS_FETCH_ERROR
from utils.sec_sic_client import fetch_filing_activity, fetch_sic

STATUS_MATCHED = "matched"
STATUS_NO_CANDIDATES = "no_candidates"
STATUS_AMBIGUOUS = "ambiguous_candidates"
STATUS_NO_VALIDATED_MATCH = "no_validated_match"
STATUS_FETCH_ERROR = "fetch_error"

BASIS_SINGLE_CANDIDATE = "single_validated_candidate"
BASIS_FILING_ACTIVITY = "filing_activity_tiebreak"
BASIS_FILING_WINDOW_CONTAINMENT = "filing_window_containment_tiebreak"

ACTIVITY_PLAUSIBLE = "plausible"
ACTIVITY_DISJOINT = "disjoint"
ACTIVITY_UNKNOWN = "unknown"

MAX_CANDIDATES_TO_VALIDATE = 20
MIN_QUERY_WORDS = 1


def match_issuer_name(
    client: CachedPrimaryClient,
    issuer_name: str,
    *,
    max_age_days: int = 90,
    era_span: tuple[str, str] | None = None,
) -> dict[str, Any]:
    """One issuer name -> one result row (always the same set of keys, regardless of
    outcome — see `_result`). Never raises; every outcome is a structured status so a
    batch run over thousands of names continues past a transient SEC 5xx/timeout on one
    name instead of aborting and losing every result already collected — nothing is
    cached on a `PrimarySourceError`, so a `fetch_error` name is retried, not
    permanently skipped, on the next run.

    `era_span` is optional and only ever used as a tie-break, never a trust shortcut:
    when 2+ candidates all validate (a genuine name collision between real registrants),
    and the caller can supply the `(first_day, last_day)` window every era sharing this
    issuer name actually traded in, `_disambiguate_by_filing_activity` may still resolve
    it — see that function and the module docstring for the full Continental Resources
    case this targets. No `era_span` supplied stays byte-identical to today's behavior.

    `identity_disproven` (Phase 18) is `True` in the returned result whenever a
    single-candidate match was rejected by `_provably_disjoint` at *any* truncation
    level, even if a later, shorter query eventually finds a real (different) match —
    see the module docstring's Phase 18 entry. It surfaces a stronger signal than a bare
    unmatched status: not just "we couldn't confirm a CIK," but "the name string itself
    is provably wrong for this era," which callers like the manual-research worklist can
    use to warn a researcher off a misleading OpenFIGI-asserted name (e.g. `UTX`
    resolving to `"ULTRATREX INC-A"`, a real but unrelated shell — see the docstring).

    `blank_sic_lead_*` (Phase 19) surfaces a candidate `_validate_candidates` rejected
    for having a blank SIC, when that candidate's name matches and its filing history is
    independently plausible for `era_span` — informational only, never accepted as a
    match; see `_find_blank_sic_lead`'s docstring for why this stays a research lead
    rather than an automatic accept."""
    saw_any_candidates = False
    disproven = False
    blank_sic_lead: tuple[str, str, bool] | None = None
    for query in _search_query_variants(issuer_name):
        try:
            candidates = search_company_ciks(client, query, max_age_days=max_age_days)
        except PrimarySourceError:
            return _result(issuer_name, STATUS_FETCH_ERROR, identity_disproven=disproven)
        if not candidates:
            continue
        saw_any_candidates = True
        if len(candidates) > MAX_CANDIDATES_TO_VALIDATE:
            # Further truncation only broadens the match set further — stop here. Still
            # carries forward any blank_sic_lead already found at a shorter, narrower
            # query level earlier in this same loop (e.g. the real FIRST REPUBLIC BANK
            # case: a 12-candidate "FIRST REPUBLIC" query finds the lead, but the next,
            # broader "FIRST" query alone hits this cap) — losing it here would silently
            # drop a real research lead just because a later, less useful query was
            # also tried.
            return _result(
                issuer_name,
                STATUS_AMBIGUOUS,
                candidate_count=len(candidates),
                identity_disproven=disproven,
                blank_sic_lead=blank_sic_lead,
            )
        validated = _validate_candidates(client, issuer_name, candidates, max_age_days)
        if validated is None:
            return _result(issuer_name, STATUS_FETCH_ERROR, identity_disproven=disproven)
        if blank_sic_lead is None:
            blank_sic_lead = _find_blank_sic_lead(client, issuer_name, candidates, era_span, max_age_days)
        if len(validated) == 1:
            cik, sic_result, matched_name = validated[0]
            if _provably_disjoint(client, cik, era_span, max_age_days):
                disproven = True
                continue  # provably wrong for this era; a shorter query may find another
            return _result(
                issuer_name,
                STATUS_MATCHED,
                matched_cik=cik,
                candidate_name=matched_name,
                sic=sic_result.get("sic"),
                sic_description=sic_result.get("sic_description"),
                match_basis=BASIS_SINGLE_CANDIDATE,
                identity_disproven=disproven,
            )
        if len(validated) > 1:
            resolved = _disambiguate_by_filing_activity(client, validated, era_span, max_age_days)
            if resolved is not None:
                cik, sic_result, matched_name, basis = resolved
                return _result(
                    issuer_name,
                    STATUS_MATCHED,
                    matched_cik=cik,
                    candidate_name=matched_name,
                    sic=sic_result.get("sic"),
                    sic_description=sic_result.get("sic_description"),
                    match_basis=basis,
                    identity_disproven=disproven,
                )
            return _result(
                issuer_name,
                STATUS_AMBIGUOUS,
                candidate_count=len(validated),
                identity_disproven=disproven,
                blank_sic_lead=blank_sic_lead,
            )
        # Zero candidates validated at this query — try a shorter one.
    status = STATUS_NO_VALIDATED_MATCH if saw_any_candidates else STATUS_NO_CANDIDATES
    return _result(issuer_name, status, identity_disproven=disproven, blank_sic_lead=blank_sic_lead)


def _search_query_variants(name: str) -> list[str]:
    """Most-specific query first, then progressively fewer trailing words down to a
    `MIN_QUERY_WORDS` (single-word) floor — safe because `_validate_candidates` below
    never trusts a query result on its own, regardless of how broad the query was.
    Deduped and whitespace-normalized so a name with no descriptor suffix to strip
    doesn't waste a redundant identical search."""
    variants: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        normalized = " ".join(candidate.split())
        key = normalized.casefold()
        if normalized and key not in seen:
            seen.add(key)
            variants.append(normalized)

    add(name)
    stripped = strip_security_descriptors(name)
    add(stripped)
    words = stripped.split()
    while len(words) > MIN_QUERY_WORDS:
        words = words[:-1]
        add(" ".join(words))
    return variants


def _validate_candidates(
    client: CachedPrimaryClient, issuer_name: str, candidates: list[str], max_age_days: int
) -> list[tuple[str, dict[str, Any], str]] | None:
    """Every candidate's real registrant name (reusing `fetch_sic` — often a free cache
    hit) is checked against the query name — current name first, then any SEC
    `formerNames` entry. Checks the *entire* candidate list (still bounded by the caller's
    `MAX_CANDIDATES_TO_VALIDATE` cap, so no new network ceiling) rather than stopping at
    the first 2 validated matches: `_disambiguate_by_filing_activity` needs to see every
    tied candidate to know whether exactly one is plausible, and stopping early risked
    missing the real match entirely if it happened to be candidate #3+ (measured live: 3
    of the real worklist's 2-way ties were only "unambiguous" because a genuine 3rd
    validating candidate was never looked at). Returns `None` (not an empty list) on a
    fetch error, so the caller reports `fetch_error` instead of a false negative."""
    validated: list[tuple[str, dict[str, Any], str]] = []
    for cik in candidates:
        sic_result = fetch_sic(client, cik, max_age_days=max_age_days)
        if sic_result.get("fetch_status") == SIC_STATUS_FETCH_ERROR:
            return None
        if not sic_result.get("sic"):
            # A blank SIC means SEC has no record of this registrant ever actually
            # filing/operating — real evidence, not a hunch: a live search for a
            # legitimate operating company ("CONFLUENT INC-CLASS A") turned up an
            # unrelated same-name shell (CIK 1171179, blank SIC) alongside the real one
            # (CIK 1699838, SIC 7372) that a plain normalized-name match alone couldn't
            # tell apart. Every confirmed-correct match checked while building this had
            # a real SIC on record; this genuine collision didn't. Phase 19 tried
            # admitting a blank-SIC candidate when its filing history is independently
            # plausible for the era and found the signal too weak to auto-accept on (see
            # `_find_blank_sic_lead` below) — surfaced as a research lead instead.
            continue
        matched_name = _matching_candidate_name(issuer_name, sic_result)
        if matched_name is not None:
            validated.append((cik, sic_result, matched_name))
    return validated


def _find_blank_sic_lead(
    client: CachedPrimaryClient,
    issuer_name: str,
    candidates: list[str],
    era_span: tuple[str, str] | None,
    max_age_days: int,
) -> tuple[str, str, bool] | None:
    """Phase 19: the first blank-SIC candidate that name-matches and has *some* filing
    landing inside `era_span` — informational only (`_result`'s `blank_sic_lead_*`
    fields), never changes `match_status`/`matched_cik`. Originally built to
    *auto-accept* such a candidate, the same way `_provably_disjoint` auto-rejects one:
    the real motivating case, `FIRST REPUBLIC BANK` (CIK 1132979, blank SIC, 42 filings
    spanning 2004-2024 that squarely cover its era), looked like overwhelming evidence.
    But auditing the resulting matches before shipping found the signal is too weak to
    trust blindly: roughly half of a 34-name quantified population turned out to have
    `entityType="other"` and *zero* substantive filings ever — their only filing activity
    was ownership-disclosure forms (SC 13G, Form 3/4/5, 13F-NT) that any unrelated third
    party can file *about* a CIK regardless of whether it was ever the real operating
    entity (`BLACK KNIGHT INC`, `FARMER BROS CO`, `SHELL MIDSTREAM PARTNERS LP`, `STEEL
    PARTNERS HOLDINGS LP`, `WELLESLEY BANK` among them) — and, worse, tightening to
    require a *substantive* filing (`SUBSTANTIVE_FORMS`) landing in the era would also
    have excluded `FIRST REPUBLIC BANK` itself, whose entire 42-filing history is also
    ownership-disclosure forms only (plausibly explained by Section 12(i): some banks
    file their real 10-Ks with their banking regulator instead of SEC, so SEC's own
    system never sees them). No reliable way to tell "genuinely the right company, files
    elsewhere" from "coincidental secondary filer" exists in this repo's data — so this
    stays a lead for a human, not an automatic accept, same "better genuinely unresolved
    than confidently wrong" posture as everything else here. `blank_sic_lead_high_confidence`
    (see `_is_high_confidence_lead`) still distinguishes the two cases where it can, so a
    researcher knows which leads are worth checking first."""
    if era_span is None:
        return None
    for cik in candidates:
        sic_result = fetch_sic(client, cik, max_age_days=max_age_days)
        if sic_result.get("fetch_status") == SIC_STATUS_FETCH_ERROR or sic_result.get("sic"):
            continue
        matched_name = _matching_candidate_name(issuer_name, sic_result)
        if matched_name is None:
            continue
        activity = fetch_filing_activity(client, cik, max_age_days=max_age_days)
        if activity.get("fetch_status") != "ok":
            continue
        if _filing_activity_verdict(activity, era_span) != ACTIVITY_PLAUSIBLE:
            continue
        return cik, matched_name, _is_high_confidence_lead(activity, era_span)
    return None


def _is_high_confidence_lead(activity: dict[str, Any], era_span: tuple[str, str]) -> bool:
    """A blank-SIC lead is "high confidence" only when SEC itself classifies the
    registrant as `entityType="operating"` *and* it has a genuine substantive filing
    (`sec_sic_client.SUBSTANTIVE_FORMS` — 10-K/10-Q/8-K/S-1/etc., not just an
    ownership-disclosure form any outside party can file) landing inside `era_span`.
    Both conditions are needed: `entityType` alone doesn't catch a shell that happens to
    have one real registration statement decades before the era; a substantive filing
    alone doesn't catch a `entityType="other"` registrant SEC itself doesn't consider a
    normal reporting company."""
    if activity.get("entity_type") != "operating":
        return False
    first_day, last_day = era_span
    substantive_dates = activity.get("substantive_filing_dates") or ()
    return any(first_day <= day <= last_day for day in substantive_dates)


def _matching_candidate_name(issuer_name: str, sic_result: dict[str, Any]) -> str | None:
    """The specific name (current `entity_name`, or one of `former_names`) that actually
    validated the query — current name checked first since it's the common case and
    needs no extra work. Returns `None` when nothing matches."""
    entity_name = sic_result.get("entity_name")
    if _names_match(issuer_name, entity_name):
        return entity_name
    for former_name in sic_result.get("former_names") or ():
        if _names_match(issuer_name, former_name):
            return former_name
    return None


def _names_match(issuer_name: str, candidate_name: str | None) -> bool:
    if not candidate_name:
        return False
    target = normalize_name(candidate_name)
    if not target:
        return False
    if normalize_name(issuer_name) == target:
        return True
    return normalize_name(strip_security_descriptors(issuer_name)) == target


def _filing_activity_verdict(activity: dict[str, Any], era_span: tuple[str, str]) -> str:
    """Whether a candidate's real SEC filing history (`sec_sic_client.fetch_filing_activity`)
    is consistent with it having actually been the operating entity during `era_span`
    (`(first_day, last_day)`, ISO `YYYY-MM-DD`). Three outcomes, never a guess:

    - `ACTIVITY_DISJOINT`: provably could not have been active during the era — either
      its newest known filing predates the era (`filings.recent` always holds the
      *newest* filings, so nothing newer exists regardless of unfetched older shards —
      the CLR-shell case: last filing a 2013 `15-12G` deregistration, era starting
      2016+), or every known filing postdates the era *and* there's no older shard left
      to check (`has_older_shards` is `False`, so that's the candidate's entire known
      SEC life).
    - `ACTIVITY_PLAUSIBLE`: at least one filing date falls inside the era window.
    - `ACTIVITY_UNKNOWN`: can't tell, and "can't tell" is never treated as a rejection.
      Covers a missing/malformed `era_span`; a candidate with older shard history that
      might contain the era but wasn't fetched (never guess about unfetched data); and —
      the one that matters most — a candidate whose filing history *brackets* the era
      but has no filing that actually lands inside it. That last case is deliberately
      NOT a rejection: a real filer can legitimately be quiet across a short window (some
      worklist eras span under two weeks)."""
    first_day, last_day = era_span
    if not first_day or not last_day or first_day > last_day:
        return ACTIVITY_UNKNOWN
    dates = activity.get("filing_dates") or ()
    has_older_shards = bool(activity.get("has_older_shards"))
    if not dates:
        return ACTIVITY_UNKNOWN if has_older_shards else ACTIVITY_DISJOINT
    if activity["latest_filing_date"] < first_day:
        return ACTIVITY_DISJOINT
    if activity["earliest_filing_date"] > last_day:
        return ACTIVITY_UNKNOWN if has_older_shards else ACTIVITY_DISJOINT
    if any(first_day <= day <= last_day for day in dates):
        return ACTIVITY_PLAUSIBLE
    return ACTIVITY_UNKNOWN


def _provably_disjoint(
    client: CachedPrimaryClient, cik: str, era_span: tuple[str, str] | None, max_age_days: int
) -> bool:
    """True only when the sole validated candidate's own SEC filing history *proves* it
    could not have been the operating entity during `era_span` — the same
    `ACTIVITY_DISJOINT` logic `_disambiguate_by_filing_activity` already applies to a
    genuine name-collision tie, now also guarding the single-candidate path (see the
    module docstring's Phase 16 entry for the audit that found this gap). Fails open —
    never rejects — when `era_span` is missing or the filing-activity fetch itself
    errors: this call is effectively always a free cache hit, since `_validate_candidates`
    just fetched the identical payload via `fetch_sic` moments earlier, so a fetch
    failure here signals a new, unrelated problem, not evidence against the candidate."""
    if era_span is None:
        return False
    activity = fetch_filing_activity(client, cik, max_age_days=max_age_days)
    if activity.get("fetch_status") != "ok":
        return False
    return _filing_activity_verdict(activity, era_span) == ACTIVITY_DISJOINT


def _disambiguate_by_filing_activity(
    client: CachedPrimaryClient,
    validated: list[tuple[str, dict[str, Any], str]],
    era_span: tuple[str, str] | None,
    max_age_days: int,
) -> tuple[str, dict[str, Any], str, str] | None:
    """Among 2+ already name-validated candidates (a genuine collision — see the module
    docstring's Continental Resources / Continental Resources Group case), accept the one
    whose real filing history is plausible for `era_span` when every other tied candidate
    is definitively `ACTIVITY_DISJOINT` — never picking between two live possibilities.
    A single `ACTIVITY_UNKNOWN` anywhere in the tie blocks acceptance entirely: partial
    confidence isn't enough to break a tie, only full confidence is. A non-`ok` fetch
    status is treated as `ACTIVITY_UNKNOWN` (fails closed) rather than a distinct
    fetch-error signal — simpler, and this call is effectively always a cache hit since
    `_validate_candidates` already fetched the identical payload via `fetch_sic` moments
    earlier.

    Phase 20: when *more than one* candidate is plausible (both have some filing landing
    in the era — the `LAREDO PETROLEUM INC` case: an original CIK whose filings stop in
    2019, and its holdco-reorg successor, later renamed `Vital Energy, Inc.`, whose
    filings span the entire era), `_fully_contains_era` breaks the tie when exactly one
    candidate's *own* filing window (`earliest_filing_date`..`latest_filing_date`) fully
    spans `era_span` while the others' don't — stricter evidence than plain
    `ACTIVITY_PLAUSIBLE` (which only needs one filing anywhere inside the window), so this
    never loosens what the single-candidate/simple-tie paths already trust, only adds a
    narrower extra case. Returns a 4-tuple (the original 3 plus a `basis` string) so the
    caller can label a containment-based accept distinctly from a plain-disjoint one — real
    but meaningfully weaker evidence (it doesn't prove the non-picked candidate was never
    active, only that its own history doesn't span the whole era)."""
    if era_span is None:
        return None
    plausible: list[tuple[str, dict[str, Any], str, dict[str, Any]]] = []
    for cik, sic_result, matched_name in validated:
        activity = fetch_filing_activity(client, cik, max_age_days=max_age_days)
        if activity.get("fetch_status") != "ok":
            return None
        verdict = _filing_activity_verdict(activity, era_span)
        if verdict == ACTIVITY_UNKNOWN:
            return None
        if verdict == ACTIVITY_PLAUSIBLE:
            plausible.append((cik, sic_result, matched_name, activity))
    if len(plausible) == 1:
        cik, sic_result, matched_name, _ = plausible[0]
        return cik, sic_result, matched_name, BASIS_FILING_ACTIVITY
    if len(plausible) > 1:
        containing = [p for p in plausible if _fully_contains_era(p[3], era_span)]
        if len(containing) == 1:
            cik, sic_result, matched_name, _ = containing[0]
            return cik, sic_result, matched_name, BASIS_FILING_WINDOW_CONTAINMENT
    return None


def _fully_contains_era(activity: dict[str, Any], era_span: tuple[str, str]) -> bool:
    """True only when a candidate's own known filing window (`earliest_filing_date`..
    `latest_filing_date`) fully spans `era_span` on both ends — a candidate that stopped
    filing partway through the era, or only started partway through it, doesn't qualify
    even though it may still be `ACTIVITY_PLAUSIBLE` (some filing landed inside the
    window). Deliberately doesn't special-case `has_older_shards`: an unfetched older
    shard could in principle push `earliest_filing_date` further back, but guessing that
    would loosen this check rather than tighten it — failing to recognize containment
    here just means the tie stays unresolved, the same safe default as everywhere else in
    this module."""
    first_day, last_day = era_span
    earliest = activity.get("earliest_filing_date")
    latest = activity.get("latest_filing_date")
    return bool(earliest and latest and earliest <= first_day and latest >= last_day)


def _result(
    issuer_name: str,
    status: str,
    *,
    matched_cik: str | None = None,
    candidate_count: int | None = None,
    candidate_name: str | None = None,
    sic: str | None = None,
    sic_description: str | None = None,
    match_basis: str | None = None,
    identity_disproven: bool = False,
    blank_sic_lead: tuple[str, str, bool] | None = None,
) -> dict[str, Any]:
    lead_cik, lead_name, lead_high_confidence = blank_sic_lead or (None, None, False)
    return {
        "identity_issuer": issuer_name,
        "match_status": status,
        "matched_cik": matched_cik,
        "candidate_count": candidate_count,
        "candidate_name": candidate_name,
        "sic": sic,
        "sic_description": sic_description,
        "match_basis": match_basis,
        "identity_disproven": identity_disproven,
        "blank_sic_lead_cik": lead_cik,
        "blank_sic_lead_name": lead_name,
        "blank_sic_lead_high_confidence": lead_high_confidence,
    }
