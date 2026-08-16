"""Match an issuer name to a CIK via EDGAR's company-name browse search
(`utils.sec_company_search_client`).

Full phase-by-phase design history (Phase 5 through 22 — every guard's motivating real
case, quantification numbers, and real-run results) lives in
`docs/EDGAR_COMPANY_SEARCH_MATCH_DESIGN.md`, extracted there in Phase 23 to keep this
file under the CSD 300-line review threshold. This docstring is the current, concise
technical summary; `docs/ARCHITECTURE.md`/`docs/TASK_LIST.md` carry the broader
SIC/sector-classification project narrative.

**Search.** EDGAR's `browse-edgar` company search does literal prefix matching against
the exact registered name string, so `match_issuer_name` tries the full
(descriptor-stripped) name first, then progressively drops trailing words down to a
1-word floor (`MIN_QUERY_WORDS`), stopping at the first query whose candidates validate.
A query returning more than `MAX_CANDIDATES_TO_VALIDATE` raw candidates is reported
`ambiguous_candidates` without validating any of them — self-evidently too generic.

**Validation.** Every candidate a query returns is checked against the real registrant
name via `sec_sic_client.fetch_sic` (`_validate_candidates`) — never a bare "only one
search hit" trusted on its own — against both the current `entity_name` and any
`formerNames` entry (a company renaming/merging since the era doesn't lose its match). A
candidate with a blank SIC is never accepted this way: real evidence a registrant never
actually filed/operated, not a hunch (the `CONFLUENT INC` shell collision).
`_names_match` also accepts `_is_safe_final_token_truncation` — a genuine mid-word
truncation of OpenFIGI's `identity_issuer` field (28-char ceiling), same token count on
both sides, exact match on every token but the last. Deliberately narrower than Tier D's
`sec_name_cik_lookup._is_prefix_relation` (which also allows "extra trailing tokens,
exact match at that position") — that broader case is only safe in Tier D because it
requires uniqueness across the *entire* SEC index, a check Tier E can't replicate.

**Multi-candidate ties.** `_disambiguate_by_filing_activity` uses
`sec_sic_client.fetch_filing_activity` to break a genuine name collision between two
real, non-blank-SIC registrants: accepts the one candidate whose filing history is
`ACTIVITY_PLAUSIBLE` for the era's `(first_day, last_day)` span when every other tied
candidate is definitively `ACTIVITY_DISJOINT` (`BASIS_FILING_ACTIVITY`), or — when
*multiple* candidates are plausible — the one whose own filing window fully contains the
whole era when exactly one does (`_fully_contains_era`, `BASIS_FILING_WINDOW_CONTAINMENT`,
a genuine mid-era CIK succession). A single `ACTIVITY_UNKNOWN` anywhere blocks
acceptance; a real filer can legitimately be quiet across a short era.

**Single-candidate rejection.** `_provably_disjoint` applies the identical
`ACTIVITY_DISJOINT` check to a *single* validated candidate — accepting on name+SIC
alone was never enough evidence that this specific candidate operated during this
specific era. Rejection falls through to a shorter query rather than returning a
confidently-wrong match. `identity_disproven` surfaces that rejection as result
metadata even when the name ultimately stays unmatched, so callers like the
manual-research worklist can warn a researcher off a misleading OpenFIGI-asserted name.

**Blank-SIC leads.** `_find_blank_sic_lead` surfaces a blank-SIC, name-matching
candidate with era-plausible filing activity as informational `blank_sic_lead_*` result
fields — never an automatic accept (a blank-SIC + "any filing lands in era" signal alone
was tried and found too weak to trust blindly). `blank_sic_lead_high_confidence`
(`_is_high_confidence_lead`) additionally requires `entityType="operating"` and a
*substantive* filing (`sec_sic_client.SUBSTANTIVE_FORMS` — 10-K/10-Q/8-K/etc., not just
an ownership-disclosure form any outside party can file) inside the era.

Every guard here follows the same posture: better genuinely unresolved than confidently
wrong. Every acceptance rule was quantified against the cached request history before
shipping, and every new signal was spot-checked against real, known companies — several
were caught producing false positives during that process and either narrowed to a safe
subset or abandoned outright (see the design-history doc for specifics: SEC's `tickers`
field, a broader SIC-specificity tie-break, and `_is_prefix_relation`'s "extra trailing
tokens" branch all looked promising and didn't survive contact with real data).
[CA][IV][REH][KBT]
"""

from __future__ import annotations

from typing import Any

from utils.resolution_v2_network import CachedPrimaryClient, PrimarySourceError
from utils.sec_company_search_client import search_company_ciks
from utils.sec_name_cik_lookup import (
    MIN_PARTIAL_TOKEN_CHARS,
    MIN_PREFIX_TOKENS,
    ROMAN_NUMERAL_CHARS,
    normalize_name,
    strip_security_descriptors,
)
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
    stripped = normalize_name(strip_security_descriptors(issuer_name))
    if stripped == target:
        return True
    return _is_safe_final_token_truncation(tuple(stripped.split()), tuple(target.split()))


def _is_safe_final_token_truncation(
    query_tokens: tuple[str, ...], candidate_tokens: tuple[str, ...]
) -> bool:
    """Phase 22: OpenFIGI truncates its `identity_issuer` field to a hard 28-character
    ceiling, sometimes mid-word (`"TPG PACE BENEFICIAL FIN"` for the real registrant's
    `"...FINANCE"`) — the exact gap Phase 9 already found and fixed for Tier D via
    `sec_name_cik_lookup._is_prefix_relation`. This is a narrower, Tier-E-specific subset
    of that same function: only the same-token-count, same-position, final-token partial
    truncation case, never the "extra trailing tokens, exact match at that position"
    case `_is_prefix_relation` also allows. That narrower scope means this deliberately
    does *not* recover this fix's own original motivating example,
    `"INTERCEPT PHARMACEUTICALS IN"` (`"IN"` is a whole extra token relative to the
    candidate's suffix-stripped name, not a same-position partial truncation) — see the
    module docstring's Phase 22 entry for why.

    That second case is exactly what Tier D uses for cases like `"HERTZ GLOBAL"` +
    `"HLDGS"`, and it's safe *there* only because Tier D additionally requires the match
    to be unique across the *entire* SEC current-listings index before accepting it. Tier
    E has no equivalent global check — it validates one already-EDGAR-searched candidate
    at a time — and reusing that branch here produced two real, confirmed false
    positives during this fix's own quantification: a candidate whose name strips down
    to a short, generic sponsor-family prefix (`"TPG Pace Holdings Corp."` -> `"TPG
    PACE"`, `"Prime Number Holding Ltd"` -> `"PRIME NUMBER"`) can spuriously prefix an
    unrelated, differently-suffixed sibling SPAC from the *same* sponsor family (`"TPG
    Pace Beneficial Finance Corp."`, `"Prime Number Acquisition..."`) — common during the
    2020-2022 SPAC boom, where one sponsor launches many similarly-named vehicles.
    Requiring the *same* token count and an exact match on every token but the last
    closes that gap: it only ever accepts when the sole difference is a partial cutoff of
    the final word itself, never an entirely different trailing word."""
    if len(query_tokens) != len(candidate_tokens) or len(query_tokens) < MIN_PREFIX_TOKENS:
        return False
    if query_tokens[:-1] != candidate_tokens[:-1]:
        return False
    query_last, candidate_last = query_tokens[-1], candidate_tokens[-1]
    if query_last == candidate_last:
        return False
    partial, full = (
        (query_last, candidate_last)
        if len(query_last) <= len(candidate_last)
        else (candidate_last, query_last)
    )
    if len(partial) < MIN_PARTIAL_TOKEN_CHARS or not full.startswith(partial):
        return False
    remainder = full[len(partial) :]
    return any(ch not in ROMAN_NUMERAL_CHARS for ch in remainder)


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
