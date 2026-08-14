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
`MYLAN` returned 12 candidates, over `MAX_CANDIDATES_TO_VALIDATE`, so it's reported
ambiguous exactly as the count guard already handles today, not a new risk. The 1-word
floor is safe specifically because the guards below don't change — they were always
what made a broad query trustworthy, not the word count.

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
actually returns 9 candidates — over `MAX_CANDIDATES_TO_VALIDATE` — so `CFLT` still
correctly stays unresolved today, just via the candidate-count guard instead of the word
floor; the blank-SIC guard would also have rejected the shell on its own if it hadn't.)

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
74 names that reach a genuine 2-way validated tie, 38 resolve via this guard. The larger
`ambiguous_candidates` bucket (over `MAX_CANDIDATES_TO_VALIDATE`, never individually
validated at all) is a different, larger, out-of-scope problem this doesn't touch.
[CA][IV][REH][KBT]
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

ACTIVITY_PLAUSIBLE = "plausible"
ACTIVITY_DISJOINT = "disjoint"
ACTIVITY_UNKNOWN = "unknown"

MAX_CANDIDATES_TO_VALIDATE = 8
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
    case this targets. No `era_span` supplied stays byte-identical to today's behavior."""
    saw_any_candidates = False
    for query in _search_query_variants(issuer_name):
        try:
            candidates = search_company_ciks(client, query, max_age_days=max_age_days)
        except PrimarySourceError:
            return _result(issuer_name, STATUS_FETCH_ERROR)
        if not candidates:
            continue
        saw_any_candidates = True
        if len(candidates) > MAX_CANDIDATES_TO_VALIDATE:
            # Further truncation only broadens the match set further — stop here.
            return _result(issuer_name, STATUS_AMBIGUOUS, candidate_count=len(candidates))
        validated = _validate_candidates(client, issuer_name, candidates, max_age_days)
        if validated is None:
            return _result(issuer_name, STATUS_FETCH_ERROR)
        if len(validated) == 1:
            cik, sic_result, matched_name = validated[0]
            return _result(
                issuer_name,
                STATUS_MATCHED,
                matched_cik=cik,
                candidate_name=matched_name,
                sic=sic_result.get("sic"),
                sic_description=sic_result.get("sic_description"),
                match_basis=BASIS_SINGLE_CANDIDATE,
            )
        if len(validated) > 1:
            resolved = _disambiguate_by_filing_activity(client, validated, era_span, max_age_days)
            if resolved is not None:
                cik, sic_result, matched_name = resolved
                return _result(
                    issuer_name,
                    STATUS_MATCHED,
                    matched_cik=cik,
                    candidate_name=matched_name,
                    sic=sic_result.get("sic"),
                    sic_description=sic_result.get("sic_description"),
                    match_basis=BASIS_FILING_ACTIVITY,
                )
            return _result(issuer_name, STATUS_AMBIGUOUS, candidate_count=len(validated))
        # Zero candidates validated at this query — try a shorter one.
    status = STATUS_NO_VALIDATED_MATCH if saw_any_candidates else STATUS_NO_CANDIDATES
    return _result(issuer_name, status)


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
            # a real SIC on record; this genuine collision didn't.
            continue
        matched_name = _matching_candidate_name(issuer_name, sic_result)
        if matched_name is not None:
            validated.append((cik, sic_result, matched_name))
    return validated


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


def _disambiguate_by_filing_activity(
    client: CachedPrimaryClient,
    validated: list[tuple[str, dict[str, Any], str]],
    era_span: tuple[str, str] | None,
    max_age_days: int,
) -> tuple[str, dict[str, Any], str] | None:
    """Among 2+ already name-validated candidates (a genuine collision — see the module
    docstring's Continental Resources / Continental Resources Group case), accept the one
    whose real filing history is plausible for `era_span` when every other tied candidate
    is definitively `ACTIVITY_DISJOINT` — never picking between two live possibilities.
    A single `ACTIVITY_UNKNOWN` anywhere in the tie blocks acceptance entirely: partial
    confidence isn't enough to break a tie, only full confidence is. A non-`ok` fetch
    status is treated as `ACTIVITY_UNKNOWN` (fails closed) rather than a distinct
    fetch-error signal — simpler, and this call is effectively always a cache hit since
    `_validate_candidates` already fetched the identical payload via `fetch_sic` moments
    earlier."""
    if era_span is None:
        return None
    plausible: list[tuple[str, dict[str, Any], str]] = []
    for cik, sic_result, matched_name in validated:
        activity = fetch_filing_activity(client, cik, max_age_days=max_age_days)
        if activity.get("fetch_status") != "ok":
            return None
        verdict = _filing_activity_verdict(activity, era_span)
        if verdict == ACTIVITY_UNKNOWN:
            return None
        if verdict == ACTIVITY_PLAUSIBLE:
            plausible.append((cik, sic_result, matched_name))
    return plausible[0] if len(plausible) == 1 else None


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
) -> dict[str, Any]:
    return {
        "identity_issuer": issuer_name,
        "match_status": status,
        "matched_cik": matched_cik,
        "candidate_count": candidate_count,
        "candidate_name": candidate_name,
        "sic": sic,
        "sic_description": sic_description,
        "match_basis": match_basis,
    }
